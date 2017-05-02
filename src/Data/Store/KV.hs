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


type StoreMap k v = TM.Map k v
newtype Store k v = Store (StoreMap k v)

type Idx v = TS.Set v

{-
  Generic KV store with a bunch of indexes.
  The general concept is largely copied from Data.IxSet.Typed
-}
data GenStore (ixs :: [*]) (k :: *) (v :: *) where
  IdxSet :: (Eq k, Hashable k) => !(Store k v) -> !(Indexes ixs v) -> GenStore ixs k v

data Indexes (ixs :: [*]) (v :: *) where
  Nil   :: Indexes '[] v
  (:+:) :: GenIdx ix v -> Indexes ixs v -> Indexes (ix ': ixs) v

data GenIdx ix v where
  IdxFun :: (Eq ix, Hashable ix) => (v -> ix) -> !(StoreMap ix (Idx v)) -> GenIdx ix v

infixr 5 :+:

class IdxLookup idxList i v where
  idxLookup :: Proxy idxList -> Proxy i -> Indexes idxList v -> GenIdx i v

instance IdxLookup (ix ': ixs) ix v where
  idxLookup _ _ (iv :+: _) = iv

instance IdxLookup idxList i v => IdxLookup (i1 ': idxList) i v where
  idxLookup pList pI (i :+: ix) = idxLookup (proxyTail pList) pI ix

idxFun :: (Eq ix, Hashable ix) => (v -> ix) -> STM (GenIdx ix v)
idxFun f = IdxFun f <$> mkStoreMap

-- k is a primary key, i is an index key
getIdx :: IdxLookup ixs i v => GenStore ixs k v -> i -> GenIdx i v
getIdx (IdxSet _ indexes) ik = idxLookup Proxy (Proxy :: Proxy ik) indexes


{-

-}
class UpdateStore idxList v where
  updateIdxs :: Proxy idxList -> v -> Indexes idxList v -> STM ()

instance UpdateStore '[] v where
  updateIdxs _ _ _ = return ()

instance (Eq v, Hashable v, UpdateStore idxList v) => UpdateStore (i1 ': idxList) v where
  updateIdxs pList v (i :+: ix) = do
    case i of
      IdxFun idxf idxStore -> do
        let idxKey = idxf v
        TM.lookup idxKey idxStore >>= \case
          Nothing -> do
            s <- TS.new
            TS.insert v s
            TM.insert s idxKey idxStore
          Just s -> TS.insert v s
    updateIdxs (proxyTail pList) v ix


updateStore :: (Eq k, Hashable k, UpdateStore ixs v) =>
               GenStore ixs k v -> k -> v -> STM (GenStore ixs k v)
updateStore g@(IdxSet (Store m) indexes) k v =
  TM.insert v k m >> updateIdxs Proxy v indexes >> return g



proxyTail :: Proxy (z ': zs) -> Proxy zs
proxyTail _ = Proxy

mkStore :: STM (Store k v)
mkStore = Store <$> TM.new

mkStoreMap :: STM (StoreMap k v)
mkStoreMap = TM.new
