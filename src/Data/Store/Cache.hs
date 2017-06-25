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
  IOVal       :: ValAtom a
  BrokenVal   :: SomeException -> UTCTime -> ValAtom a
  PromiseVal  :: !(Async (Maybe a))            -> ValAtom a
  PreparedVal :: !(Diff a)                     -> ValAtom a

data IdxAtom a where
  IOIdx      :: IdxAtom a
  PromiseIdx :: !(Async [a])   -> IdxAtom a
  BrokenIdx  :: !SomeException -> IdxAtom a


newtype Atoms k a = Atoms (StoreMap k a)

newtype AtomIdx pk v ix = AtomIdx (Atoms ix (IdxAtom (pk, v)))

data ErrorCaching = NoErrorCaching | TimedCaching Int

data ErrorCachingS e a args = ErrorCachingS {
  broken    :: SomeException -> args -> Maybe e,
  requested :: SomeException -> ValAtom a
}

data CacheStore pk v ixs = CacheStore {
  store        :: GenStore ixs pk v,
  idxAtoms     :: TList ixs (AtomIdx pk v),
  valAtoms     :: Atoms pk (ValAtom v),
  errorCaching :: ErrorCaching
}

-- Auxiliary internal type
data AtomS p e v = Promise_ p | Broken_ e | Value_ v

-- TODO Use this one instead of Maybe
data ResulVal a = Absent | BeingPrepared | Present (Val a)

cacheStore :: (Eq pk, Hashable pk, TListGen ixs (AtomIdx pk v)) =>
              TList ixs (GenIdx pk v) -> STM (CacheStore pk v ixs)
cacheStore indexes = do
  store <- mkStore
  valAtoms <- TM.new
  idxAtoms <- genTListM (AtomIdx . Atoms <$> TM.new)
  return $ CacheStore (IdxSet store indexes)
                      idxAtoms (Atoms valAtoms) NoErrorCaching


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


cachedOrIO :: forall pk v ixs . (Eq pk, Hashable pk, Eq v, Hashable v) =>
               CacheStore pk (Val v) ixs ->
               pk ->
               (pk -> IO (Maybe (Val v))) ->
               IO (Either SomeException (Maybe (Val v)))
cachedOrIO CacheStore {..} = cachedOrIO_ valAtoms store
  where
    cachedOrIO_
      va@(Atoms valAtoms)
      store@(IdxSet (Store storeKV) idxs)
      pk fromIO = do
          x <- join $ atomically $ TM.lookup pk storeKV >>= \case
                Just x  ->
                  return $ return $ Value_ (Just x)
                Nothing ->
                  TM.lookup pk valAtoms >>= \case
                    Nothing -> do
                      TM.insert IOVal pk valAtoms
                      return $ mask_ $ do
                        a <- async (fromIO pk)
                        atomically $ TM.insert (PromiseVal a) pk valAtoms
                        return $ Promise_ a
                    Just IOVal                        -> retry
                    Just (PromiseVal a)               -> return $ return $ Promise_ a
                    Just (BrokenVal e c)              -> return $ return $ Broken_ (e, c)

                    {- TODO Replace Maybe with a type with 3 values:
                      Absent | BeingPrepared | Present (Val a)
                      to distinguish betweet "no value" situation and
                      "wait a second, it's being prepared in 2-PC"
                    -}
                    Just (PreparedVal (Add _))        -> return $ return $ Value_ Nothing

                    Just (PreparedVal (Update old _)) -> return $ return $ Value_ (Just old)
          case x of
            Value_ v        -> return $ Right v
            Promise_ a      -> waitAndUpdateStore a
            Broken_ (e, c)  ->
              case (errorCaching, c) of
                (NoErrorCaching, _)  -> return $ Left e
                (TimedCaching t, et) -> do
                  ct <- getCurrentTime
                  -- when (ct < et + 1000) $ return ()
                  return $ Left e

      where
        waitAndUpdateStore :: Async (Maybe (Val v)) -> IO (Either SomeException (Maybe (Val v)))
        waitAndUpdateStore a = do
          r <- waitCatch a
          case (r, errorCaching) of
            (Right v, _) -> do
                atomically $ do
                  forM_ v (resolve store va pk)
                  TM.delete pk valAtoms
                return (Right v)

            (Left e, NoErrorCaching) -> return (Left e)

            (Left e, TimedCaching _) -> do
              {- TODO getCurrentTime is pretty slow and to avoid multiple calls
                 we could move it inside of the async (fromIO pk) computation
              -}
              time <- getCurrentTime
              atomically $ TM.lookup pk valAtoms >>= \case
                  -- it is the only valid case
                  Just (BrokenVal e _) -> TM.insert (BrokenVal e time) pk valAtoms

                  _ ->
                    -- TODO That must not happen ever, add some assertion here
                    return ()

              return (Left e)



indexCachedOrIO :: forall pk v i ixs .
                   (Eq pk, Hashable pk, Eq i, Hashable i, Eq v, TListLookup ixs i)  =>
                   CacheStore pk (Val v) ixs ->
                   i ->
                   (i -> IO [(pk, Val v)]) ->
                   IO (Either SomeException [(pk, Val v)])
indexCachedOrIO cs @ CacheStore { store = store, valAtoms = valAtoms } i =
  indexCachedOrIO_ idxAtoms valAtoms store i
  where
    AtomIdx idxAtoms = getIdxAtom cs i
    indexCachedOrIO_
        (Atoms idxAtoms)
        va@(Atoms valAtoms)
        store@(IdxSet (Store storeKV) idxs)
        i fromIO = do
          x <- join $ atomically $ getByIndex store i >>= \case
                Just values -> return . return $ Value_ values
                Nothing     ->
                  TM.lookup i idxAtoms >>= \case
                    Nothing -> do
                      TM.insert IOIdx i idxAtoms
                      return $ mask_ $ do
                        a <- async (fromIO i)
                        atomically $ TM.insert (PromiseIdx a) i idxAtoms
                        return $ Promise_ a
                    Just IOIdx -> retry
                    Just (PromiseIdx a) -> return $ return $ Promise_ a
                    Just (BrokenIdx e)  -> return $ return $ Broken_ e
          case x of
            Value_ v   -> return $ Right v
            Promise_ a -> waitCatch a >>= atomically . updateStore
            Broken_ e  -> return $ Left e

      where
        updateStore :: Either SomeException [(pk, Val v)] -> STM (Either SomeException [(pk, Val v)])
        updateStore ioResult = do
          atom' <- TM.lookup i idxAtoms
          case (atom', ioResult) of
            (Nothing, _) ->
              -- That means another client has called 'removeAtom'
              -- so just don't do anything here
              return ioResult
            (Just (BrokenIdx e), Left exception) -> do
              TM.insert (BrokenIdx exception) i idxAtoms
              return (Left exception)

            (Just _, Left exception) -> do
              TM.insert (BrokenIdx exception) i idxAtoms
              return (Left exception)

            (_, Right values) -> do
              updateValues values
              return (Right values)

          where
            updateValues :: [(pk, Val v)] -> STM ()
            updateValues values = do
                insertIntoIdx store i (map fst values)
                forM_ values (uncurry (resolve store va))
                TM.delete i idxAtoms


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
