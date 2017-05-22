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

data Thing a where
  Absent       :: Thing a
  BadThing     :: SomeException -> Thing a
  Promise      :: !(IO (Async t)) -> Thing a
  PreparedDiff :: !(Diff a) -> Thing a
  Memo         :: !(Val a)  -> Thing a


type Stored v = TVar (Thing v)
type Cache k v = Store (Key k) (Stored v)

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
  BrokenVal   :: !SomeException     -> ValAtom a
  PromiseVal  :: !(Async (Maybe a)) -> ValAtom a
  PreparedVal :: !(Diff a)          -> ValAtom a


-- TODO There has to be some periodical job to clean up the broken ones
data IdxAtom a where
  IOIdx      :: IdxAtom a
  PromiseIdx :: !(Async [a])   -> IdxAtom a
  BrokenIdx  :: !SomeException -> IdxAtom a


newtype Atoms k a = Atoms (StoreMap k a)

newtype AtomIdx pk v ix = AtomIdx (Atoms ix (IdxAtom (pk, Val v)))

data CacheStore pk v ixs = CacheStore {
  store    :: GenStore ixs pk v,
  idxAtoms :: TList ixs (AtomIdx pk v),
  valAtoms :: Atoms pk (ValAtom v)
}

cachedOrIO_ :: forall pk v ixs .
               (Eq pk, Hashable pk, Eq v, Hashable v) =>
               CacheStore pk (Val v) ixs ->
               pk ->
               (pk -> IO (Maybe (Val v))) ->
               IO (Either SomeException (Maybe (Val v)))
cachedOrIO_ CacheStore { store = store, valAtoms = va } = cachedOrIO va store

{-
forall pk v i idx .
                   (Eq pk, Hashable pk, Eq i, Hashable i, Eq v, Hashable v, IdxLookup idx i pk (Val v)) =>
                   Atoms i (IdxAtom (pk, Val v)) ->
                   Atoms pk (ValAtom (Val v)) ->
                   GenStore idx pk (Val v) ->
                   i ->
                   (i -> IO [(pk, Val v)]) ->
                   IO (Either SomeException [(pk, Val v)])
-}
--
-- indexCachedOrIO_ :: forall pk v i ixs .
--                    (Eq pk, Hashable pk, Eq i, Hashable i, Eq v, Hashable v, IdxLookup ixs i pk (Val v))  =>
--                    CacheStore pk (Val v) ixs ->
--                    i ->
--                    (i -> IO [(pk, Val v)]) ->
--                    IO (Either SomeException [(pk, Val v)])
-- indexCachedOrIO_ CacheStore {..} i fromIO =
--     indexCachedOrIO


getIdxAtom :: TListLookup ixs ix => CacheStore pk v ixs -> i -> AtomIdx pk v ix
getIdxAtom CacheStore { idxAtoms = idxAtoms } ik = tlistLookup Proxy (Proxy :: Proxy ik) idxAtoms


cachedOrIO :: forall pk v ixs .
              (Eq pk, Hashable pk, Eq v, Hashable v) =>
              Atoms pk (ValAtom (Val v)) ->
              GenStore ixs pk (Val v) ->
              pk ->
              (pk -> IO (Maybe (Val v))) ->
              IO (Either SomeException (Maybe (Val v)))
cachedOrIO
  va@(Atoms valAtoms)
  store@(IdxSet (Store storeKV) idxs)
  pk fromIO = do
      x <- join $ atomically $ TM.lookup pk storeKV >>= \case
            Just x  ->
              return $ return $ Right (Just x)
            Nothing     ->
              TM.lookup pk valAtoms >>= \case
                Nothing -> do
                  TM.insert IOVal pk valAtoms
                  return $ mask_ $ do
                    a <- async (fromIO pk)
                    atomically $ TM.insert (PromiseVal a) pk valAtoms
                    return $ Left a
                Just IOVal                        -> retry
                Just (PromiseVal a)               -> return $ return $ Left a
                Just (PreparedVal (Add _))        -> return $ return $ Right Nothing
                Just (PreparedVal (Update old _)) -> return $ return $ Right (Just old)
      case x of
        Left a  -> waitCatch a >>= atomically . updateStore
        Right v -> return $ Right v
  where
    updateStore :: Either SomeException (Maybe (Val v)) -> STM (Either SomeException (Maybe (Val v)))
    updateStore ioResult = do
      atom' <- TM.lookup pk valAtoms
      case (atom', ioResult) of
        (Nothing, _) ->
          -- That means another client has called 'removeAtom'
          -- so just don't do anything here
          return ioResult
        (Just (BrokenVal e), Left exception) -> do
          TM.insert (BrokenVal exception) pk valAtoms
          return (Left exception)

        -- TODO Decide what to do in this case
        (Just _, Left exception) -> do
          TM.insert (BrokenVal exception) pk valAtoms
          return (Left exception)

        (_, x@(Right (Just val))) -> do
          resolve store va pk val
          TM.delete pk valAtoms
          return x


indexCachedOrIO :: forall pk v i idx .
                   (Eq pk, Hashable pk, Eq i, Hashable i, Eq v, Hashable v, TListLookup idx i) =>
                   Atoms i (IdxAtom (pk, Val v)) ->
                   Atoms pk (ValAtom (Val v)) ->
                   GenStore idx pk (Val v) ->
                   i ->
                   (i -> IO [(pk, Val v)]) ->
                   IO (Either SomeException [(pk, Val v)])
indexCachedOrIO
    (Atoms idxAtoms)
    va@(Atoms valAtoms)
    store@(IdxSet (Store storeKV) idxs)
    i fromIO = do
      x <- join $ atomically $ getByIndex store i >>= \case
            Just values -> return . return $ Right values
            Nothing     ->
              TM.lookup i idxAtoms >>= \case
                Nothing -> do
                  TM.insert IOIdx i idxAtoms
                  return $ mask_ $ do
                    a <- async (fromIO i)
                    atomically $ TM.insert (PromiseIdx a) i idxAtoms
                    return $ Left a
                Just IOIdx -> retry
                Just (PromiseIdx a) -> return $ return $ Left a
      case x of
        Left a  -> waitCatch a >>= atomically . updateStore
        Right v -> return $ Right v

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
        (_, Right values) -> do
          updateStore values
          return (Right values)

      where
        updateStore :: [(pk, Val v)] -> STM ()
        updateStore values = do
            insertIntoIdx store i (map fst values)
            forM_ values (uncurry (resolve store va))
            TM.delete i idxAtoms


resolve :: forall pk v ixs .
           (Eq pk, Hashable pk, Eq v, Hashable v) =>
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
          PromiseVal _ -> insertToStore
          BrokenVal e  -> insertToStore
          PreparedVal _ -> return ()
    where
      insertToStore = TM.insert val pk storeKV
