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

data TList (ixs :: [*]) (f :: * -> *) where
  TNil  :: TList '[] f
  (:-:) :: f ix -> TList ixs f -> TList (ix ': ixs) f

{-
  Generic KV store with a bunch of indexes.
  The general concept is largely copied from Data.IxSet.Typed

  - pk is a primary key
  - ixs can have indexes on any other field (or function)
-}

data GenStore (ixs :: [*]) (pk :: *) (v :: *) where
  IdxSet :: (Eq pk, Hashable pk) => !(Store pk v) -> !(TList ixs (GenIdx pk v)) -> GenStore ixs pk v

data GenIdx pk v ix where
  IdxFun :: (Eq ix, Hashable ix) => (v -> ix) -> !(StoreMap ix (Idx pk)) -> GenIdx pk v ix

infixr 5 :-:

class TListLookup ixs ix where
  tlistLookup :: Proxy ixs -> Proxy ix -> TList ixs f -> f ix

instance TListLookup (ix ': ixs) ix where
  tlistLookup _ _ (iv :-: _) = iv

instance {-# OVERLAPS #-} TListLookup ixs ix => TListLookup (ix1 ': ixs) ix where
  tlistLookup pList pI (i :-: ix) = tlistLookup (proxyTail pList) pI ix

idxFun :: (Eq ix, Hashable ix) => (v -> ix) -> STM (GenIdx pk v ix)
idxFun f = IdxFun f <$> mkStoreMap

-- k is a primary key, i is an index key
getIdx :: TListLookup ixs ix => GenStore ixs pk v -> i -> GenIdx pk v ix
getIdx (IdxSet _ indexes) ik = tlistLookup Proxy (Proxy :: Proxy ik) indexes


getByIndex :: (Eq i, Hashable i, Eq pk, Hashable pk, TListLookup ixs i) =>
              GenStore ixs pk v -> i -> STM (Maybe [(pk, v)])
getByIndex store@(IdxSet (Store storeKV) idxs) i = do
    let IdxFun _ m = getIdx store i
    TM.lookup i m >>= \case
      Just val -> do
        pks <- LT.toList (TS.stream val)
        rs  <- forM pks $ \pk ->
          (pk,) <$> TM.lookup pk storeKV
        return $ Just [ (pk, v) | (pk, Just v) <- rs ]
      Nothing  -> return Nothing


-- TODO this is bit hacky, so here should be some reasonable
-- explanation why do we only update one but not the others
insertIntoIdx :: (Eq i, Hashable i, Eq pk, Hashable pk, TListLookup ixs i) =>
                 GenStore ixs pk v -> i -> [pk] -> STM ()
insertIntoIdx store i vs = do
  let IdxFun _ m = getIdx store i
  idx <- TS.new
  forM_ vs (`TS.insert` idx)
  TM.insert idx i m


{-

-}
class UpdateStore ixs pk v where
  updateIdxs :: Proxy ixs -> pk -> v -> TList ixs (GenIdx pk v) -> STM ()

instance UpdateStore '[] pk v where
  updateIdxs _ _ _ _ = return ()

instance {-# OVERLAPS #-} (Eq pk, Hashable pk, UpdateStore idxList pk v) =>
                          UpdateStore (i1 ': idxList) pk v where
  updateIdxs pList pk v (i :-: ix) = do
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


-- TODO handle insertion of duplicate pk's
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
