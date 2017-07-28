{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE DeriveDataTypeable #-}
{-# LANGUAGE DeriveGeneric #-}
{-# LANGUAGE TupleSections #-}
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

data ValAtom a where
  RetryingIO   :: ValAtom a
  PerformingIO :: ValAtom a
  PromiseVal   :: !(Async (Maybe a))         -> ValAtom a
  PreparedVal  :: !(Diff a)                  -> ValAtom a
  BrokenVal    :: !SomeException -> !UTCTime -> ValAtom a

data IdxAtom a where
  IOIdx         :: IdxAtom a
  RetryingIOIdx :: IdxAtom a
  PromiseIdx    :: !(Async [a])   -> IdxAtom a
  BrokenIdx     :: !SomeException -> !UTCTime -> IdxAtom a


newtype Atoms k a = Atoms (StoreMap k a)

newtype AtomIdx pk v ix = AtomIdx (Atoms ix (IdxAtom (pk, v)))

data ErrorCaching = NoErrorCaching | TimedCaching NominalDiffTime

data CacheStore pk v ixs = CacheStore {
  store        :: GenStore ixs pk v,
  idxAtoms     :: TList ixs (AtomIdx pk v),
  valAtoms     :: Atoms pk (ValAtom v),
  errorCaching :: ErrorCaching
}

data AtomS p e v = Promise_ p | Broken_ e | Value_ v

data ResultVal a = NoVal | Unknown | Exists a

type CachedVal a = Either SomeException (ResultVal (Val a))


toResultVal :: Maybe a -> ResultVal a
toResultVal Nothing = NoVal
toResultVal (Just v) = Exists v

data CacheConf = CacheConf {
  errorCachingStrategy :: ErrorCaching
}

defaultCacheConf :: CacheConf
defaultCacheConf = CacheConf NoErrorCaching

cacheStore :: (Eq pk, Hashable pk, TListGen ixs (AtomIdx pk v)) =>
              TList ixs (GenIdx pk v) -> STM (CacheStore pk v ixs)
cacheStore indexes = cacheStoreWithConf indexes defaultCacheConf

cacheStoreWithConf :: (Eq pk, Hashable pk, TListGen ixs (AtomIdx pk v)) =>
              TList ixs (GenIdx pk v) -> CacheConf -> STM (CacheStore pk v ixs)
cacheStoreWithConf indexes CacheConf {..} = do
  store <- mkStore
  valAtoms <- TM.new
  idxAtoms <- genTListM (AtomIdx . Atoms <$> TM.new)
  return $ CacheStore (IdxSet store indexes)
                      idxAtoms (Atoms valAtoms) errorCachingStrategy


cached :: forall pk v ixs . (Eq pk, Hashable pk) =>
          CacheStore pk (Val v) ixs ->
          pk ->
          STM (Maybe (Val v))
cached CacheStore { store = (IdxSet (Store storeKV) _) } pk = TM.lookup pk storeKV


indexCached :: forall pk v i ixs .
              (Eq pk, Hashable pk, Eq i, Hashable i, Eq v, Hashable v, TListLookup ixs i)  =>
              CacheStore pk (Val v) ixs ->
              i ->
              STM (Maybe [(pk, Val v)])
indexCached CacheStore { store = store } = getByIndex store

{-
  TODO Add some logic to leep track of IO actions that take too long to execute.


-}
cachedOrIO :: forall pk v ixs . (Eq pk, Hashable pk, Eq v, Hashable v, Show v) =>
               CacheStore pk (Val v) ixs ->
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
          return $ case errorCaching of
                    NoErrorCaching      -> return $ Left e
                    TimedCaching period -> mask_ $ do
                      ct <- getCurrentTime
                      if longEnough period ct t
                        then join (atomically startAsync) >>= waitAndUpdateStore
                        else do
                          atomically $ TM.lookup pk valAtoms >>= \case
                              Just RetryingIO -> setAtom (BrokenVal e t)
                              _               -> return ()
                          return $ Left e

        startAsync :: STM (IO (Async (Maybe (Val v))))
        startAsync = do
          setAtom PerformingIO
          return $ mask_ $ do
            a <- async (fromIO pk)
            -- TODO Add some serial number to every promise so that
            -- it would be possible to keep track of which promise it is
            atomically $ setAtom (PromiseVal a)
            return a


        waitAndUpdateStore :: Async (Maybe (Val v)) -> IO (Either SomeException (ResultVal (Val v)))
        waitAndUpdateStore a = do
          r <- waitCatch a
          case (r, errorCaching) of
            (Right v, _) -> do
                atomically $ do
                  forM_ v (resolve store va pk)
                  clearAtom
                return $ Right $ toResultVal v

            (Left e, NoErrorCaching) -> do
              atomically clearAtom
              return $ Left e

            (Left e, TimedCaching _) -> do
              {- TODO getCurrentTime is pretty slow and to avoid multiple calls
                 we could move it inside of the async (fromIO pk) computation
              -}
              time <- getCurrentTime
              -- TODO Add some check here to know what exactly are we replacing
              atomically $ setAtom (BrokenVal e time)
              return (Left e)


indexCachedOrIO :: forall pk v i ixs .
                   (Eq pk, Hashable pk, Eq i, Hashable i, Eq v, TListLookup ixs i)  =>
                   CacheStore pk (Val v) ixs ->
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
          return $ case errorCaching of
              NoErrorCaching      -> return $ Left e
              TimedCaching period -> mask_ $ do
                ct <- getCurrentTime
                if longEnough period ct t
                  then join (atomically startAsync) >>= waitAndUpdateStore
                  else do
                    atomically $ TM.lookup i idxAtoms >>= \case
                        Just RetryingIOIdx -> setAtom (BrokenIdx e t)
                        _                  -> return ()
                    return $ Left e

        startAsync :: STM (IO (Async [(pk, Val v)]))
        startAsync = do
          setAtom IOIdx
          return $ mask_ $ do
            a <- async (fromIO i)
            -- TODO Add some serial number to every promise so that
            -- it would be possible to keep track of which promise it is
            atomically $ setAtom (PromiseIdx a)
            return a


        waitAndUpdateStore :: Async [(pk, Val v)] -> IO (Either SomeException [(pk, Val v)])
        waitAndUpdateStore a = do
          r <- waitCatch a
          case (r, errorCaching) of
            (Right values, _) -> do
                atomically $ do
                  insertIntoIdx store i (map fst values)
                  forM_ values $ uncurry (resolve store va)
                  clearAtom
                return $ Right values

            (Left e, NoErrorCaching) -> do
              atomically clearAtom
              return $ Left e

            (Left e, TimedCaching _) -> do
              {- TODO getCurrentTime is pretty slow and to avoid multiple calls
                 we could move it inside of the async (fromIO pk) computation
              -}
              time <- getCurrentTime
              -- TODO Add some check here to know what exactly are we replacing
              atomically $ setAtom (BrokenIdx e time)
              return $ Left e


-- indexCachedOrIO :: forall pk v i ixs .
--                    (Eq pk, Hashable pk, Eq i, Hashable i, Eq v, TListLookup ixs i)  =>
--                    CacheStore pk (Val v) ixs ->
--                    i ->
--                    (i -> IO [(pk, Val v)]) ->
--                    IO (Either SomeException [(pk, Val v)])
-- indexCachedOrIO cs @ CacheStore { store = store, valAtoms = valAtoms } i =
--   indexCachedOrIO_ idxAtoms valAtoms store i
--   where
--     AtomIdx idxAtoms = getIdxAtom cs i
--     indexCachedOrIO_
--         (Atoms idxAtoms)
--         va@(Atoms valAtoms)
--         store@(IdxSet (Store storeKV) idxs)
--         i fromIO = do
--           x <- join $ atomically $ getByIndex store i >>= \case
--                 Just values ->
--                   return . return $ Value_ values
--                 Nothing     ->
--                   TM.lookup i idxAtoms >>= \case
--                     Nothing -> do
--                       TM.insert IOIdx i idxAtoms
--                       return $ mask_ $ do
--                         a <- async (fromIO i)
--                         atomically $ TM.insert (PromiseIdx a) i idxAtoms
--                         return $ Promise_ a
--                     Just IOIdx -> retry
--                     Just (PromiseIdx a) -> return $ return $ Promise_ a
--                     Just (BrokenIdx e _)  -> return $ return $ Broken_ e
--           case x of
--             Value_ v   -> return $ Right v
--             Promise_ a -> waitCatch a >>= atomically . updateStore
--             Broken_ e  -> return $ Left e
--
--       where
--         updateStore :: Either SomeException [(pk, Val v)] -> STM (Either SomeException [(pk, Val v)])
--         updateStore ioResult = do
--           atom' <- TM.lookup i idxAtoms
--           case (atom', ioResult) of
--             (Nothing, _) ->
--               -- That means another client has called 'removeAtom'
--               -- so just don't do anything here
--               return ioResult
--             (Just (BrokenIdx e t), Left exception) -> do
--               TM.insert (BrokenIdx exception t) i idxAtoms
--               return (Left exception)
--
--             (Just _, Left exception) -> do
--               TM.insert (BrokenIdx exception t) i idxAtoms
--               return (Left exception)
--
--             (_, Right values) -> do
--               updateValues values
--               return (Right values)
--
--           where
--             updateValues :: [(pk, Val v)] -> STM ()
--             updateValues values = do
--                 insertIntoIdx store i (map fst values)
--                 forM_ values (uncurry (resolve store va))
--                 TM.delete i idxAtoms


getIdxAtom :: TListLookup ixs ix => CacheStore pk v ixs -> ix -> AtomIdx pk v ix
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
