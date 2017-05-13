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
module Data.Store.Cache where

import Control.Exception

import Control.Concurrent.STM
import Control.Concurrent.Async

import Control.Monad

import Data.Binary
import Data.Hashable
import Data.Typeable
import GHC.Generics (Generic)

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

data Diff a = Add !(Val a) |
              Update !(Val a) !(Val a) deriving (Eq, Show, Ord, Generic, Typeable)
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
  BrokenVal   :: !SomeException  -> ValAtom a
  PromiseVal  :: !(IO (Async (Maybe (Val a)))) -> ValAtom a
  PreparedVal :: !(Diff a)       -> ValAtom a


-- TODO There has to be some periodical job to clean this ones up
data IdxAtom a where
  FutureAtom :: !(IO (Async [a])) -> IdxAtom a
  BrokenAtom :: !SomeException    -> IdxAtom a


newtype Atoms k a = Atoms (StoreMap k a)


indexCachedOrIO :: forall pk v i idx .
                   (Eq pk, Hashable pk, Eq i, Hashable i, Eq v, Hashable v, IdxLookup idx i pk (Val v)) =>
                   Atoms i (IdxAtom (pk, Val v)) ->
                   Atoms pk (ValAtom v) ->
                   GenStore idx pk (Val v) ->
                   i ->
                   (i -> IO [(pk, Val v)]) ->
                   IO (Either SomeException [(pk, Val v)])
indexCachedOrIO
    (Atoms idxAtoms)
    va@(Atoms valAtoms)
    store@(IdxSet (Store storeKV) idxs)
    i fromIO =
  join $ atomically $ getByIndex store i >>= \case
    Nothing -> getOrCreateAtom >>= \case
        FutureAtom promise ->
          return $ promise >>= waitCatch >>= atomically . processResult
        BrokenAtom e ->
          return . return $ Left e

    Just values -> return . return $ Right values

  where
    getOrCreateAtom :: STM (IdxAtom (pk, Val v))
    getOrCreateAtom = TM.lookup i idxAtoms >>= \case
      Nothing -> do
        let atom = FutureAtom (async (fromIO i))
        TM.insert atom i idxAtoms
        return atom
      Just atom -> return atom

    processResult :: Either SomeException [(pk, Val v)] -> STM (Either SomeException [(pk, Val v)])
    processResult ioResult = do
      atom' <- TM.lookup i idxAtoms
      case (atom', ioResult) of
        (Nothing, _) ->
          -- That means another client has called 'removeAtom'
          -- so just don't do anything here
          return ioResult
        (Just (BrokenAtom e), Left exception) -> do
          TM.insert (BrokenAtom exception) i idxAtoms
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


cachedOrIO :: forall pk v ixs .
              (Eq pk, Hashable pk, Eq v, Hashable v) =>
              Atoms pk (ValAtom v) ->
              GenStore ixs pk (Val v) ->
              pk ->
              (pk -> IO (Maybe (Val v))) ->
              IO (Either SomeException (Maybe (Val v)))
cachedOrIO
  va@(Atoms valAtoms)
  store@(IdxSet (Store storeKV) idxs)
  pk fromIO =
  join $ atomically $ TM.lookup pk storeKV >>= \case
      Nothing -> getOrCreateAtom >>= \case
          PromiseVal promise ->
              return $ promise >>= waitCatch >>= processResult
          PreparedVal (Add _) ->
              return . return $ Right Nothing
          PreparedVal (Update old _) ->
              return . return $ Right (Just old)
          BrokenVal e ->
              return . return $ Left e
      Just x  ->
          return . return $ Right (Just x)

  where
    getOrCreateAtom :: STM (ValAtom v)
    getOrCreateAtom = TM.lookup pk valAtoms >>= \case
      Nothing -> do
        let atom = PromiseVal (async (fromIO pk))
        TM.insert atom pk valAtoms
        return atom
      Just atom -> return atom

    processResult :: Either SomeException (Maybe (Val v)) -> IO (Either SomeException (Maybe (Val v)))
    processResult ioResult = atomically $ do
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


resolve :: forall pk v ixs .
           (Eq pk, Hashable pk, Eq v, Hashable v) =>
           GenStore ixs pk (Val v) ->
           Atoms pk (ValAtom v) ->
           pk ->
           Val v -> STM ()
resolve
  (IdxSet (Store storeKV) _)
  (Atoms atoms)
  pk
  val@(Val v version) =
    TM.lookup pk atoms >>= \case
        Nothing -> TM.lookup pk storeKV >>= \case
            Nothing -> TM.insert val pk storeKV
            Just (Val v' version') ->
                when (version > version') $ TM.insert val pk storeKV
        Just a  -> case a of
          BrokenVal e -> TM.insert val pk storeKV
          _           -> return ()


cachedOrIOGeneric :: (Eq pk, Hashable pk) =>
                      StoreMap pk (Stored v) ->
                      pk ->
                      (pk -> IO t) ->
                      (t -> STM (Maybe (Val v))) ->
                      IO (Maybe (Val v))
cachedOrIOGeneric kv k fromIO processIOResult =
  join $ atomically $ TM.lookup k kv >>= \case
      Nothing -> do
        let a = async (fromIO k)
        t <- newTVar (Promise a)
        TM.insert t k kv
        return $ do
          v <- waitCatch =<< a
          atomically $ readTVar t >>= \case
                Promise _ -> case v of
                  -- TODO Do something smarter than that
                  Left e  -> writeTVar t (BadThing e) >> return Nothing
                  Right v -> do
                    v' <- processIOResult v
                    writeTVar t (maybe Absent Memo v')
                    return v'
                smth      -> val smth
      Just x  ->
        return $ atomically $ readTVar x >>= val

  where
    val = \case
      Absent                    -> return Nothing
      BadThing _                -> return Nothing
      Memo v                    -> return $ Just v
      PreparedDiff (Update v _) -> return $ Just v
      {- FIXME This is a potential problem: if there is PreparedDiff (Add _)
         waiting for too long to commit, the whole value reading will become
         blocked until commit or rollback -}
      _                         -> retry




data Negotiation = Prepared | Rejected | Screwed | Commitable | Commited
  deriving (Eq, Show, Generic, Typeable)
instance Binary Negotiation


prepareStore :: (Eq k, Hashable k) =>
                Cache k v -> Key k -> Diff v -> STM (Maybe (Stored v))
prepareStore (Store kv) k diff = do
  val <- TM.lookup k kv
  case (val, diff) of
    (Nothing, Add val) -> do
      t <- newTVar (PreparedDiff diff)
      TM.insert t k kv
      return (Just t)

    (Just thing, Update _ (Val _ v)) -> readTVar thing >>= \case
        Memo (Val _ v') | v == v' -> do
          writeTVar thing (PreparedDiff diff)
          return (Just thing)

        _ -> return Nothing

    _ -> return Nothing


commitThing :: TVar (Thing v) -> IO Negotiation
commitThing thing = atomically $ readTVar thing >>= \case
  (PreparedDiff (Add v))      -> writeTVar thing (Memo v) >> return Commited
  (PreparedDiff (Update _ v)) -> writeTVar thing (Memo v) >> return Commited
  _  -> return Screwed


rollbackStore :: (Eq k, Hashable k) =>
                  Cache k v -> Key k -> IO ()
rollbackStore (Store kv) k = atomically $ TM.lookup k kv >>= \case
  Nothing    -> return ()
  Just thing -> readTVar thing >>= \case
    (PreparedDiff (Add v))        -> TM.delete k kv
    (PreparedDiff (Update old _)) -> writeTVar thing (Memo old)
    _  -> return ()
