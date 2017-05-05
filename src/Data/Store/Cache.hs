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


cachedOnly :: (Eq k, Hashable k) => Cache k v -> Key k -> STM (Maybe (Val v))
cachedOnly (Store kv) k = TM.lookup k kv >>= \case
  Nothing -> return Nothing
  Just t  -> readTVar t >>= \case
    Memo v -> return $ Just v
    _      -> return Nothing

cachedOrIO :: (Eq k, Hashable k) =>
                 Cache k v ->
                 Key k ->
                 (Key k -> IO (Maybe (Val v))) ->
                 IO (Maybe (Val v))
cachedOrIO (Store kv) k fromIO = cachedOrIOGeneric kv k fromIO return

cachedOrIOGeneric :: forall k v t . (Eq k, Hashable k) =>
                       StoreMap (Key k) (Stored v) ->
                       Key k ->
                       (Key k -> IO t) ->
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




getByIdx :: (Eq k, Eq i, Hashable i) =>
            GenStore ixs k v ->
            i ->
            (i -> IO [Maybe (Val v)]) ->
            IO [(k, Maybe (Val v))]
getByIdx (IdxSet m ixs) i batchIO = do
  -- cachedOrIOGeneric m i batchIO processIOResult
  return []
  where
    processIOResult :: [Maybe (Val v)] -> STM (Maybe (Val v))
    processIOResult _ = return Nothing



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
