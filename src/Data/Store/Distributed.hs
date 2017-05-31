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
module Data.Store.Distributed where

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
import Data.Store.Cache

data Negotiation = Prepared | Rejected | Screwed | Commitable | Commited
  deriving (Eq, Show, Generic, Typeable)
instance Binary Negotiation



-- prepareStore :: (Eq k, Hashable k) =>
--                 Cache k v -> Key k -> Diff (Val v) -> STM (Maybe (Stored v))
-- prepareStore (Store kv) k diff = do
--   val <- TM.lookup k kv
--   case (val, diff) of
--     (Nothing, Add val) -> do
--       t <- newTVar (PreparedDiff diff)
--       TM.insert t k kv
--       return (Just t)
--
--     (Just thing, Update _ (Val _ v)) -> readTVar thing >>= \case
--         Memo (Val _ v') | v == v' -> do
--           writeTVar thing (PreparedDiff diff)
--           return (Just thing)
--
--         _ -> return Nothing
--
--     _ -> return Nothing
--
--
-- commitThing :: TVar (Thing v) -> IO Negotiation
-- commitThing thing = atomically $ readTVar thing >>= \case
--   (PreparedDiff (Add v))      -> writeTVar thing (Memo v) >> return Commited
--   (PreparedDiff (Update _ v)) -> writeTVar thing (Memo v) >> return Commited
--   _  -> return Screwed
--
--
-- rollbackStore :: (Eq k, Hashable k) =>
--                   Cache k v -> Key k -> IO ()
-- rollbackStore (Store kv) k = atomically $ TM.lookup k kv >>= \case
--   Nothing    -> return ()
--   Just thing -> readTVar thing >>= \case
--     (PreparedDiff (Add v))        -> TM.delete k kv
--     (PreparedDiff (Update old _)) -> writeTVar thing (Memo old)
--     _  -> return ()
