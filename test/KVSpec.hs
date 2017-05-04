{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE DeriveGeneric #-}
{-# LANGUAGE TupleSections #-}

{-# LANGUAGE TypeOperators #-}
{-# LANGUAGE DataKinds #-}
{-# LANGUAGE GADTs #-}
{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE PolyKinds #-}
{-# LANGUAGE UndecidableInstances #-}
{-# LANGUAGE MultiParamTypeClasses #-}
{-# LANGUAGE OverlappingInstances #-}
{-# LANGUAGE TypeFamilies #-}
{-# LANGUAGE UnicodeSyntax #-}

import qualified STMContainers.Set as TS
import qualified STMContainers.Map as TM

import Control.Concurrent.STM

import Data.Hashable
import Data.List
import GHC.Generics (Generic)

import qualified ListT as LT

import Test.QuickCheck (arbitrary, Property, Arbitrary, quickCheck, (==>))
import Test.QuickCheck.Monadic (assert, monadicIO, pick, pre, run)

import Data.Store.KV
import Data.Store.Cache

newtype IKey = IKey Int deriving (Eq, Ord, Show, Generic)
instance Hashable IKey

instance Arbitrary IKey where
  arbitrary = IKey <$> arbitrary

type Entry = (IKey, String, Bool)
type EntryIdxs = '[String, Bool]

type Store_ = GenStore EntryIdxs IKey Entry

mkTestStore = do
  store :: Store IKey Entry <- atomically mkStore
  atomically $ do
    sIdx <- idxFun (\(_, s, _) -> s)
    bIdx <- idxFun (\(_, _, b) -> b)
    return $ IdxSet store (sIdx :+: bIdx :+: Nil)

getByIndex :: (Eq i, Hashable i, IdxLookup EntryIdxs i Entry) =>
              Store_ -> i -> STM [Entry]
getByIndex gStore aKey = do
    let IdxFun sf sMap = getIdx gStore aKey
    Just val <- TM.lookup aKey sMap
    v <- LT.toList (TS.stream val)
    return $ sortOn (\(x, _, _) -> x) v


prop_retrievable_by_idx_after_insert :: Property
prop_retrievable_by_idx_after_insert = monadicIO $ do
  e@(k, s, b) <- pick arbitrary
  gStore <- run mkTestStore

  run $ atomically $ update gStore k e

  let IdxFun sf sMap = getIdx gStore ("" :: String)
  let IdxFun bf bMap = getIdx gStore True

  v1 <- run $ atomically $ getByIndex gStore s
  v2 <- run $ atomically $ getByIndex gStore b

  assert $ head v1 == e
  assert $ head v2 == e


main :: IO ()
main = do
  quickCheck prop_retrievable_by_idx_after_insert
  quickCheck prop_retrievable_by_idx_after_insert
