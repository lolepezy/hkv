{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE ScopedTypeVariables #-}
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

  - pk is a primary key
  - ixs can have indexes on any other field (or function)
-}
data GenStore (ixs :: [*]) (pk :: *) (v :: *) where
  IdxSet :: (Eq pk, Hashable pk) => !(Store pk v) -> !(Indexes ixs pk v) -> GenStore ixs pk v

data Indexes (ixs :: [*]) (pk :: *) (v :: *) where
  Nil   :: Indexes '[] pk v
  (:+:) :: GenIdx ix pk v -> Indexes ixs pk v -> Indexes (ix ': ixs) pk v

data GenIdx ix pk v where
  IdxFun :: (Eq ix, Hashable ix) => (v -> ix) -> !(StoreMap ix (Idx pk)) -> GenIdx ix pk v

infixr 5 :+:

class IdxLookup idxList i pk v where
  idxLookup :: Proxy idxList -> Proxy i -> Indexes idxList pk v -> GenIdx i pk v

instance IdxLookup (ix ': ixs) ix pk v where
  idxLookup _ _ (iv :+: _) = iv

instance {-# OVERLAPS #-} IdxLookup ixs i pk v => IdxLookup (i1 ': ixs) i pk v where
  idxLookup pList pI (i :+: ix) = idxLookup (proxyTail pList) pI ix

idxFun :: (Eq ix, Hashable ix) => (v -> ix) -> STM (GenIdx ix pk v)
idxFun f = IdxFun f <$> mkStoreMap

-- k is a primary key, i is an index key
getIdx :: IdxLookup ixs i pk v => GenStore ixs pk v -> i -> GenIdx i pk v
getIdx (IdxSet _ indexes) ik = idxLookup Proxy (Proxy :: Proxy ik) indexes


getByIndex :: (Eq i, Hashable i, Eq pk, Hashable pk, IdxLookup ixs i pk v) =>
              GenStore ixs pk v -> i -> STM (Maybe [(pk, v)])
getByIndex store@(IdxSet (Store storeKV) idxs) i = do
    let IdxFun _ m = getIdx store i
    TM.lookup i m >>= \case
      Just val -> do
        pks <- LT.toList (TS.stream val)
        rs  <- forM pks $ \pk -> do
          v <- TM.lookup pk storeKV
          return (pk, v)
        return $ Just [ (pk, v) | (pk, Just v) <- rs ]
      Nothing  -> return Nothing


-- TODO this is bit hacky, so here should be some reasonable
-- explanation why do we only update one but not the others
insertIntoIdx :: (Eq i, Hashable i, Eq pk, Hashable pk, IdxLookup ixs i pk v) =>
                 GenStore ixs pk v -> i -> [pk] -> STM ()
insertIntoIdx store i vs = do
  let IdxFun _ m = getIdx store i
  idx <- TS.new
  forM_ vs (`TS.insert` idx)
  TM.insert idx i m


{-

-}
class UpdateStore idxList pk v where
  updateIdxs :: Proxy idxList -> pk -> v -> Indexes idxList pk v -> STM ()

instance UpdateStore '[] pk v where
  updateIdxs _ _ _ _ = return ()

instance {-# OVERLAPS #-} (Eq pk, Hashable pk, UpdateStore idxList pk v) =>
                          UpdateStore (i1 ': idxList) pk v where
  updateIdxs pList pk v (i :+: ix) = do
    case i of
      IdxFun idxf idxStore -> do
        let idxKey = idxf v
        TM.lookup idxKey idxStore >>= \case
          Nothing -> do
            s <- TS.new
            TS.insert pk s
            TM.insert s idxKey idxStore
          Just s -> TS.insert pk s
    updateIdxs (proxyTail pList) pk v ix


update :: (Eq pk, Hashable pk, UpdateStore ixs pk v) =>
               GenStore ixs pk v -> pk -> v -> STM (GenStore ixs pk v)
update g@(IdxSet (Store m) indexes) pk v =
  TM.insert v pk m >> updateIdxs Proxy pk v indexes >> return g


proxyTail :: Proxy (z ': zs) -> Proxy zs
proxyTail _ = Proxy

mkStore :: STM (Store k v)
mkStore = Store <$> TM.new

mkStoreMap :: STM (StoreMap k v)
mkStoreMap = TM.new
