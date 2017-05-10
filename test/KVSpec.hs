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
{-# LANGUAGE TypeFamilies #-}
{-# LANGUAGE UnicodeSyntax #-}

import qualified STMContainers.Set as TS
import qualified STMContainers.Map as TM

import Control.Concurrent.STM
import Control.Monad (forM_, forM)

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

primaryKey :: Entry -> IKey
primaryKey (pk, _, _) = pk

prop_retrievable_by_idx_after_insert :: Property
prop_retrievable_by_idx_after_insert = monadicIO $ do
  -- e@(k, s, b) <- pick arbitrary
  entries :: [Entry] <- pick arbitrary
  gStore <- run mkTestStore

  run $ atomically $ forM_ entries $ \e -> update gStore (primaryKey e) e

  vs <- run $ atomically $ forM entries $ \e -> do
    let (pk, s, b) = e
    Just v1 <- getByIndex gStore s
    Just v2 <- getByIndex gStore b
    return (e, v1, v2)

  assert $ all (\(e, _, v) -> e `elem` map snd v) vs
  assert $ all (\(e, v, _) -> e `elem` map snd v) vs

  assert $ all (\((_, s, _), v, _) -> all (\(_, s', _) -> s' == s) (map snd v)) vs
  assert $ all (\((_, _, b), _, v) -> all (\(_, _, b') -> b' == b) (map snd v)) vs


main :: IO ()
main = do
  quickCheck prop_retrievable_by_idx_after_insert
