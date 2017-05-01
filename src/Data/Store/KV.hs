{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE DeriveDataTypeable #-}
{-# LANGUAGE DeriveGeneric #-}
{-# LANGUAGE TupleSections #-}
{-# LANGUAGE LambdaCase #-}

{-# LANGUAGE TypeOperators #-}
{-# LANGUAGE DataKinds #-}
{-# LANGUAGE KindSignatures #-}
{-# LANGUAGE GADTs #-}
{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE PolyKinds #-}
{-# LANGUAGE UndecidableInstances #-}
{-# LANGUAGE MultiParamTypeClasses #-}
{-# LANGUAGE OverlappingInstances #-}
{-# LANGUAGE TypeFamilies #-}
{-# LANGUAGE UnicodeSyntax #-}
module Data.Store.KV where


import Control.Concurrent.STM
import Control.Concurrent hiding (newChan)
import Control.Concurrent.MVar
import Control.Concurrent.Async

import qualified STMContainers.Set as TS
import qualified STMContainers.Map as TM

import Control.Exception

import Control.Monad
import Control.Monad.Trans.Class
import Control.Monad.Trans.Reader

import Data.Data
import Data.Maybe (fromMaybe, catMaybes)
import Data.Hashable
import GHC.Generics (Generic)

import qualified ListT as LT

{-
  Primitive key-value in-memory consensus log.

  - Conceptually, we store the set of (key, value, version).
  - All the nodes have the same set


  1) Reads happen where they happen, we assume all the nodes having the same in-memory state

  2) Updates/Creates happen the following way:
    - Update event (k, u) comes to node N
    - the worker on the node N (initiator) checks if there exists commit log for the key k
      * if it does the update is rejected

    - if it doesn't initiator sends (k, v => v + 1) to all other nodes (executors)
    - every executor
       * checks if (k, v => v + 1) is compatible with the current log entries for this key, i.e.
         the last version in the log is "v"
       * if so, adds a change log entry for
       * send back an "I'm ready" message (as onCommit hook for the STM transaction)
       * wait for the "commit" message
       * commit log (remove log entries)

       ===
       At the moment "log" most probably is just one (new-version, new-value) entry,
       but in principle it would be possible to use some custom logic for building up
       a consistent log. Think about conflict-free types as well.
-}



type StoreMap k v = TM.Map k v
newtype Store k v = Store (StoreMap k v)

type Idx v = TS.Set v

{-
  Generic KV store with a bunch of indexes.
  The general concept is largely copied from Data.IxSet.Typed
-}
data GenStore (ixs :: [*]) (k :: *) (v :: *) where
  IdxSet :: !(Store k v) -> !(Indexes ixs v) -> GenStore ixs k v

data Indexes (ixs :: [*]) (v :: *) where
  Nil   :: Indexes '[] v
  (:+:) :: GenIdx ix v -> Indexes ixs v -> Indexes (ix ': ixs) v

data GenIdx ix v where
  IdxFun :: (v -> ix) -> !(StoreMap ix (Idx v)) -> GenIdx ix v

infixr 5 :+:

class IdxLookup idxList i v where
  idxLookup :: Proxy idxList -> Proxy i -> Indexes idxList v -> GenIdx i v

instance IdxLookup (i ': ixs) i v where
  idxLookup _ _ (iv :+: _) = iv

instance IdxLookup idxList i v => IdxLookup (i1 ': idxList) i v where
  idxLookup pList pI (i :+: ix) = idxLookup (pp pList) pI ix
    where
      pp :: Proxy (z ': zs) -> Proxy zs
      pp _ = Proxy


idxFun :: (v -> ix) -> STM (GenIdx ix v)
idxFun f = IdxFun f <$> mkStoreMap

-- k is a primary key, i is an index key
getIdx :: IdxLookup ixs i v => GenStore ixs k v -> i -> GenIdx i v
getIdx (IdxSet _ indexes) ik = idxLookup Proxy (Proxy :: Proxy ik) indexes



mkStore :: STM (Store k v)
mkStore = Store <$> TM.new

mkStoreMap :: STM (StoreMap k v)
mkStoreMap = TM.new
