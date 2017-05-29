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

import Control.Concurrent.Async
import Control.Concurrent.STM
import Control.Monad (forM_, forM)

import Data.Hashable
import Data.List
import Data.Maybe (listToMaybe)
import GHC.Generics (Generic)

import qualified ListT as LT

import Test.QuickCheck (arbitrary, Property, Arbitrary, quickCheck, choose, sized, vectorOf, sublistOf)
import Test.QuickCheck.Monadic (PropertyM, assert, monadicIO, pick, pre, run)

import Data.Store.KV
import qualified Data.Store.Cache as Cache

newtype IKey = IKey Int deriving (Eq, Ord, Show, Generic)
instance Hashable IKey

instance Arbitrary IKey where
  arbitrary = IKey <$> arbitrary

type Entry = (IKey, String, Bool)
type EntryIdxs = '[String, Bool]

type Store_ = GenStore EntryIdxs IKey Entry

newtype Counter = Counter Int deriving (Eq, Ord, Show, Generic)

mkTestStore = atomically $ do
    sIdx <- idxFun (\(Cache.Val (_, s, _) _) -> s)
    bIdx <- idxFun (\(Cache.Val (_, _, b) _) -> b)
    Cache.cacheStore (sIdx :-: bIdx :-: TNil)

primaryKey :: Entry -> IKey
primaryKey (pk, _, _) = pk

mkValAtoms :: STM (Cache.Atoms pk (Cache.ValAtom v))
mkValAtoms = Cache.Atoms <$> TM.new

mkIdxAtoms :: STM (Cache.Atoms i (Cache.IdxAtom (pk, Cache.Val v)))
mkIdxAtoms = Cache.Atoms <$> TM.new

mkCounters :: (Eq k, Hashable k) => [k] -> STM (TM.Map k (TVar Counter))
mkCounters keys = do
  m <- TM.new
  forM_ keys $ \k -> do
    c <- newTVar (Counter 0)
    TM.insert c k m
  return m

requestPks :: Show a => Monad m => [a] -> PropertyM m [a]
requestPks as = do
  ks <- pick $ sized $ \s -> do
          n <- choose (0, 10)
          vectorOf n (sublistOf as)
  return $ concat ks

inc :: (Eq k, Hashable k) => k -> TM.Map k (TVar Counter) -> STM ()
inc pk counters = do
  Just c <- TM.lookup pk counters
  modifyTVar' c $ \(Counter c') -> Counter (c' + 1)


prop_cache_or_io_should_invoke_io_exactly_once :: Property
prop_cache_or_io_should_invoke_io_exactly_once = monadicIO $ do
  randomEntries :: [Entry] <- pick arbitrary
  let entries = nubBy (\(k1, _, _) (k2, _, _)-> k1 == k2) randomEntries

  store <- run mkTestStore

  requestPks <- requestPks $ map primaryKey entries
  counters <- run $ atomically (mkCounters requestPks)

  results <- run $ forM requestPks $ \pk ->
      async $ Cache.cachedOrIO store pk $ \pk -> do
          atomically $ inc pk counters
          return $ listToMaybe [ Cache.Val e (Cache.Version 0) | e@(pk', _, _) <- entries, pk' == pk ]

  run $ mapM_ wait results
  counters1 <- run $ atomically $ forM requestPks $ \pk -> do
    Just c <- TM.lookup pk counters
    (pk,) <$> readTVar c

  let (IdxSet (Store storeKV) _) = Cache.store store
  values <- run $ atomically $ forM requestPks $ \pk ->
    (pk,) <$> TM.lookup pk storeKV

  assert $ null counters1 || all (\(_, Counter c) -> c == 1) counters1
  assert $ all (\(pk, Just (Cache.Val (ik, _, b) _)) -> pk == ik) values
  ve   <- run $ atomically $ let (Cache.Atoms a) = Cache.valAtoms store in TM.null a
  -- idxe <- run $ atomically $ let (Atoms a) = idxAtoms in TM.null a
  assert ve
  -- assert idxe


prop_idx_cache_or_io_should_invoke_io_exactly_once :: Property
prop_idx_cache_or_io_should_invoke_io_exactly_once = monadicIO $ do
  randomEntries :: [Entry] <- pick arbitrary
  let entries = nubBy (\(k1, _, _) (k2, _, _)-> k1 == k2) randomEntries

  store <- run mkTestStore

  requestIdxKeys <- requestPks $ map (\(_, s, _) -> s) entries
  counters <- run $ atomically (mkCounters requestIdxKeys)

  results <- run $ forM requestIdxKeys $ \sk ->
      async $ Cache.indexCachedOrIO store sk $ \(s :: String) -> do
          atomically $ inc sk counters
          return [ (pk, Cache.Val e (Cache.Version 0)) | e@(pk, s', _) <- entries, s' == s ]

  run $ mapM_ wait results


  counters1 <- run $ atomically $ forM requestIdxKeys $ \pk -> do
    Just c <- TM.lookup pk counters
    (pk,) <$> readTVar c

  assert $ null counters1 || all (\(_, Counter c) -> c == 1) counters1
  ve   <- run $ atomically $ let (Cache.Atoms a) = Cache.valAtoms store in TM.null a
  -- idxe <- run $ atomically $ let (Cache.Atoms a) = Cache.idxAtoms store in TM.null a
  assert ve
  -- assert idxe



main :: IO ()
main = do
  quickCheck prop_cache_or_io_should_invoke_io_exactly_once
  quickCheck prop_idx_cache_or_io_should_invoke_io_exactly_once
