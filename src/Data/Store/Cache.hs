{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE DeriveDataTypeable #-}
{-# LANGUAGE DeriveGeneric #-}
{-# LANGUAGE TupleSections #-}
{-# LANGUAGE AllowAmbiguousTypes #-}
{-# LANGUAGE LambdaCase #-}

{-# LANGUAGE TypeOperators #-}
{-# LANGUAGE DataKinds #-}
{-# LANGUAGE GADTs #-}
{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE PolyKinds #-}
{-# LANGUAGE UndecidableInstances #-}
{-# LANGUAGE MultiParamTypeClasses #-}
{-# LANGUAGE TypeFamilies #-}
{-# LANGUAGE UnicodeSyntax #-}
{-# LANGUAGE RecordWildCards #-}
module Data.Store.Cache where

import Control.Exception

import Control.Concurrent.STM
import Control.Concurrent.Async

import Control.Monad

import Data.Binary
import Data.Hashable
import Data.Typeable
import GHC.Generics (Generic)

import Data.Time.Clock

import Debug.Trace

import qualified STMContainers.Set as TS
import qualified STMContainers.Map as TM

import Data.Store.KV

{-
  Use separate "atoms" map for all updates

  1) Value update by PK
    - use normal Promise/Absent/Error logic in atoms
    - when IO successfully returns do the normal conflict resolution
      for the given K

  2) Index update:
    - use Promise/Error logic with index atoms
    - insert into the index when and only when the "indexCacheOrIO" is requested
    - after IO action in indexCacheOrIO get the batch of values
        * update the index with (i, Set v)
        * for every v in Set v do conflict resolution with the main store
    - do not insert index entries in any other way

  3) Conflicts

   Promise | PreparedDiff TxId -> rollback TxId

   Promise | update Memo v     -> wait for Promise,
    then conflict resolution (version/value/source)
-}

newtype Key k = Key k deriving (Eq, Show, Ord, Generic, Typeable)
instance (Typeable k, Binary k) => Binary (Key k)
instance Hashable k => Hashable (Key k)

newtype Version = Version Integer deriving (Eq, Show, Ord, Generic, Typeable)
instance Binary Version

newtype TxId = TxId Integer deriving (Eq, Show, Ord, Generic, Typeable)
instance Binary TxId

data Val a = Val !a !Version deriving (Eq, Show, Ord, Generic, Typeable)
instance Binary a => Binary (Val a)

data Diff a = Add !a |
              Update !a !a deriving (Eq, Show, Ord, Generic, Typeable)
instance Binary a => Binary (Diff a)

data IOResult a = Return a | Fail !SomeException !UTCTime

data ValAtom a where
  RetryingIO   :: ValAtom a
  PerformingIO :: ValAtom a
  PromiseVal   :: !(Async (IOResult (Maybe a))) -> ValAtom a
  PreparedVal  :: !(Diff a)                     -> ValAtom a
  BrokenVal    :: !SomeException -> !UTCTime    -> ValAtom a

data IdxAtom a where
  IOIdx         :: IdxAtom a
  RetryingIOIdx :: IdxAtom a
  PromiseIdx    :: !(Async (IOResult [a]))    -> IdxAtom a
  BrokenIdx     :: !SomeException -> !UTCTime -> IdxAtom a


newtype Atoms k a = Atoms (StoreMap k a)

newtype AtomIdx pk v ix = AtomIdx (Atoms ix (IdxAtom (pk, v)))

data ErrorCaching = NoErrorCaching | TimedCaching NominalDiffTime

type family IOResult_ v (caching :: ErrorCaching)
type instance IOResult_ v NoErrorCaching = Either SomeException v
type instance IOResult_ v (TimedCaching _) = Either (SomeException, UTCTime) v

data CacheStore pk v ixs (caching :: ErrorCaching) = CacheStore {
  store        :: GenStore ixs pk v,
  idxAtoms     :: TList ixs (AtomIdx pk v),
  valAtoms     :: Atoms pk (ValAtom v),
  errorCaching :: ErrorCaching
}

data AtomS p e v = Promise_ !p | Broken_ !e | Value_ !v

data ResultVal a = NoVal | Unknown | Exists !a

type CachedVal a = Either SomeException (ResultVal (Val a))

toResultVal :: Maybe a -> ResultVal a
toResultVal Nothing = NoVal
toResultVal (Just v) = Exists v

newtype CacheConf = CacheConf {
  errorCachingStrategy :: ErrorCaching
}

defaultCacheConf :: CacheConf
defaultCacheConf = CacheConf NoErrorCaching

cacheStore :: (Eq pk, Hashable pk, TListGen ixs (AtomIdx pk v)) =>
              TList ixs (GenIdx pk v) -> STM (CacheStore pk v ixs c)
cacheStore indexes = cacheStoreWithConf indexes defaultCacheConf

cacheStoreWithConf :: (Eq pk, Hashable pk, TListGen ixs (AtomIdx pk v)) =>
              TList ixs (GenIdx pk v) -> CacheConf -> STM (CacheStore pk v ixs c)
cacheStoreWithConf indexes CacheConf {..} = do
  store <- mkStore
  valAtoms <- TM.new
  idxAtoms <- genTListM (AtomIdx . Atoms <$> TM.new)
  return $ CacheStore (IdxSet store indexes)
                      idxAtoms (Atoms valAtoms) errorCachingStrategy


cached :: forall pk v ixs c . (Eq pk, Hashable pk) =>
          CacheStore pk (Val v) ixs c ->
          pk ->
          STM (Maybe (Val v))
cached CacheStore { store = (IdxSet (Store storeKV) _) } pk = TM.lookup pk storeKV


indexCached :: forall pk v i ixs c .
              (Eq pk, Hashable pk, Eq i, Hashable i, Eq v, Hashable v, TListLookup ixs i)  =>
              CacheStore pk (Val v) ixs c ->
              i ->
              STM (Maybe [(pk, Val v)])
indexCached CacheStore { store = store } = getByIndex store

{-
  TODO Add some logic to leep track of IO actions that take too long to execute.
-}
cachedOrIO :: forall pk v ixs c . (Eq pk, Hashable pk, Eq v, Hashable v, Show v) =>
               CacheStore pk (Val v) ixs c ->
               pk ->
               (pk -> IO (Maybe (Val v))) ->
               IO (CachedVal v)
cachedOrIO CacheStore {..} = cachedOrIO_ valAtoms store
  where
    cachedOrIO_
      va@(Atoms valAtoms)
      store@(IdxSet (Store storeKV) idxs)
      pk fromIO =
          join $ atomically $ TM.lookup pk storeKV >>= \case
                Just x  ->
                  return $ return $ Right (Exists x)
                Nothing ->
                  TM.lookup pk valAtoms >>= \case
                    Nothing                           -> startAsync >>= \a -> return (a >>= waitAndUpdateStore)
                    Just PerformingIO                 -> retry
                    Just RetryingIO                   -> retry
                    Just (PromiseVal a)               -> return $ waitAndUpdateStore a
                    Just (BrokenVal e t)              -> returnErrorOrRetry e t
                    Just (PreparedVal (Add _))        -> return $ return $ Right Unknown
                    Just (PreparedVal (Update old _)) -> return $ return $ Right (Exists old)

      where
        setAtom a = TM.insert a pk valAtoms
        clearAtom = TM.delete pk valAtoms

        returnErrorOrRetry :: SomeException -> UTCTime -> STM (IO (Either SomeException (ResultVal (Val v))))
        returnErrorOrRetry e t = do
          setAtom RetryingIO
          return $ returnErrorOrRetry_ errorCaching
          where
            returnErrorOrRetry_ NoErrorCaching        = return $ Left e
            returnErrorOrRetry_ (TimedCaching period) = do
                      ct <- getCurrentTime
                      if longEnough period ct t
                        then join (atomically startAsync) >>= waitAndUpdateStore
                        else do
                          atomically $ TM.lookup pk valAtoms >>= \case
                              Just RetryingIO -> setAtom (BrokenVal e t)
                              _               -> return ()
                          return $ Left e

        startAsync :: STM (IO (Async (IOResult (Maybe (Val v)))))
        startAsync = do
          setAtom PerformingIO
          return $ mask_ $ do
            a <- async $ try (fromIO pk) >>= \case
                  Left e  -> getCurrentTime >>= \t -> return $ Fail e t
                  Right v -> return $ Return v
            atomically $ setAtom (PromiseVal a)
            return a

        waitAndUpdateStore :: Async (IOResult (Maybe (Val v))) -> IO (Either SomeException (ResultVal (Val v)))
        waitAndUpdateStore a =
          wait a >>= \case
            Return v -> case v of
              Just z  -> do
                atomically $ resolve store va pk z >> clearAtom
                return $ Right $ Exists z
              Nothing ->
                return $ Right NoVal
            Fail e t -> do
              atomically $ case errorCaching of
                NoErrorCaching -> clearAtom
                TimedCaching _ -> setAtom (BrokenVal e t)
              return $ Left e

{-

-}
indexCachedOrIO :: forall pk v i ixs c .
                   (Eq pk, Hashable pk, Eq i, Hashable i, Eq v, TListLookup ixs i)  =>
                   CacheStore pk (Val v) ixs c ->
                   i ->
                   (i -> IO [(pk, Val v)]) ->
                   IO (Either SomeException [(pk, Val v)])
indexCachedOrIO cs @ CacheStore {..} i =
  indexCachedOrIO_ idxAtoms valAtoms store i
  where
    AtomIdx idxAtoms = getIdxAtom cs i
    indexCachedOrIO_
        (Atoms idxAtoms)
        va@(Atoms valAtoms)
        store@(IdxSet (Store storeKV) idxs)
        i fromIO =
           join $ atomically $ getByIndex store i >>= \case
                Just values ->
                  return . return $ Right values
                Nothing     ->
                  TM.lookup i idxAtoms >>= \case
                    Nothing              -> startAsync >>= \a -> return (a >>= waitAndUpdateStore)
                    Just IOIdx           -> retry
                    Just RetryingIOIdx   -> retry
                    Just (PromiseIdx a)  -> return $ waitAndUpdateStore a
                    Just (BrokenIdx e t) -> returnErrorOrRetry e t

      where
        setAtom a = TM.insert a i idxAtoms
        clearAtom = TM.delete i idxAtoms

        returnErrorOrRetry :: SomeException -> UTCTime -> STM (IO (Either SomeException [(pk, Val v)]))
        returnErrorOrRetry e t = do
          setAtom RetryingIOIdx
          return $ returnErrorOrRetry_ errorCaching
          where
            returnErrorOrRetry_ NoErrorCaching        = return $ Left e
            returnErrorOrRetry_ (TimedCaching period) = do
                ct <- getCurrentTime
                if longEnough period ct t
                  then join (atomically startAsync) >>= waitAndUpdateStore
                  else do
                    atomically $ TM.lookup i idxAtoms >>= \case
                        Just RetryingIOIdx -> setAtom (BrokenIdx e t)
                        _                  -> return ()
                    return $ Left e

        startAsync :: STM (IO (Async (IOResult [(pk, Val v)])))
        startAsync = do
          setAtom IOIdx
          return $ mask_ $ do
            a <- async $ try (fromIO i) >>= \case
                  Left e  -> getCurrentTime >>= \t -> return $ Fail e t
                  Right v -> return $ Return v
            atomically $ setAtom (PromiseIdx a)
            return a

        waitAndUpdateStore :: Async (IOResult [(pk, Val v)]) -> IO (Either SomeException [(pk, Val v)])
        waitAndUpdateStore a =
          wait a >>= \case
            Return values -> do
              atomically $ do
                insertIntoIdx store i (map fst values)
                forM_ values $ uncurry (resolve store va)
                clearAtom
              return $ Right values
            Fail e t -> do
              atomically $ case errorCaching of
                NoErrorCaching -> clearAtom
                TimedCaching _ -> setAtom (BrokenIdx e t)
              return $ Left e



getIdxAtom :: TListLookup ixs ix => CacheStore pk v ixs c -> ix -> AtomIdx pk v ix
getIdxAtom CacheStore { idxAtoms = idxAtoms } ik = tlistLookup Proxy (Proxy :: Proxy ik) idxAtoms


resolve :: forall pk v ixs .
           (Eq pk, Hashable pk) =>
           GenStore ixs pk (Val v) ->
           Atoms pk (ValAtom (Val v)) ->
           pk ->
           Val v -> STM ()
resolve
  (IdxSet (Store storeKV) _)
  (Atoms atoms)
  pk
  val@(Val v version) =
    TM.lookup pk atoms >>= \case
        Nothing -> TM.lookup pk storeKV >>= \case
            Nothing -> insertToStore
            Just (Val v' version') ->
                when (version > version') insertToStore
        Just a  -> case a of
          PromiseVal _  -> insertToStore
          BrokenVal _ _ -> insertToStore
          PreparedVal _ -> return ()
    where
      insertToStore = TM.insert val pk storeKV

longEnough :: NominalDiffTime -> UTCTime -> UTCTime -> Bool
longEnough period utc1 utc2 = diffUTCTime utc1 utc2 > period
