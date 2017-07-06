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

import Control.Exception hiding (assert)

import Data.Hashable
import Data.List
import Data.Maybe (listToMaybe)
import GHC.Generics (Generic)
import Data.Typeable

import qualified ListT as LT

import Test.Tasty
import Test.Tasty.QuickCheck as QC
import qualified Test.Tasty.HUnit as HU

import Test.QuickCheck.Monadic

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

uniqueEntries :: Monad m => PropertyM m [Entry]
uniqueEntries = nubBy (\(k1, _, _) (k2, _, _)-> k1 == k2) <$> pick QC.arbitrary

atomsAreEmpty :: Cache.CacheStore IKey (Cache.Val Entry) EntryIdxs -> STM (Bool, Bool, Bool)
atomsAreEmpty store = do
  let (Cache.Atoms valAtoms) = Cache.valAtoms store
  let (Cache.AtomIdx (Cache.Atoms sIdx)) = Cache.getIdxAtom store ("" :: String)
  let (Cache.AtomIdx (Cache.Atoms bIdx)) = Cache.getIdxAtom store (True :: Bool)
  (,,) <$> TM.null bIdx <*> TM.null sIdx <*> TM.null valAtoms


prop_cache_or_io_should_invoke_io_exactly_once :: QC.Property
prop_cache_or_io_should_invoke_io_exactly_once = monadicIO $ do
  entries <- uniqueEntries
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
  (be, se, ve) <- run $ atomically $ atomsAreEmpty store
  assert be
  assert se
  assert ve


prop_idx_cache_or_io_should_invoke_io_exactly_once :: QC.Property
prop_idx_cache_or_io_should_invoke_io_exactly_once = monadicIO $ do
  entries <- uniqueEntries

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
  (be, se, ve) <- run $ atomically $ atomsAreEmpty store
  assert be
  assert se
  assert ve


data SomeIOProblem = SomeIOProblem deriving (Show, Typeable)

instance Exception SomeIOProblem

ioWithError :: TVar Counter -> IKey -> IO (Maybe (Cache.Val Entry))
ioWithError c pk = do
    atomically $ modifyTVar' c $ \(Counter c') -> Counter (c' + 1)
    throw SomeIOProblem

cache_or_io_much_not_cache_errors_by_default :: HU.Assertion
cache_or_io_much_not_cache_errors_by_default = do
  let e = (IKey 1, "1", False)
  let pk = primaryKey e

  store <- mkTestStore
  counters <- atomically (mkCounters [pk])
  c <- atomically $ newTVar (Counter 0)

  r1 :: Either SomeException (Maybe (Cache.Val Entry)) <- Cache.cachedOrIO store pk (ioWithError c)
  r2 :: Either SomeException (Maybe (Cache.Val Entry)) <- Cache.cachedOrIO store pk (ioWithError c)

  Counter n <- readTVarIO c
  HU.assert $ n == 2
  (be, se, ve) <- atomically $ atomsAreEmpty store
  HU.assert be
  HU.assert se
  HU.assert ve


cache_or_io_must_cache_errors_for_period_of_time :: HU.Assertion
cache_or_io_must_cache_errors_for_period_of_time = do
  let e = (IKey 1, "1", False)
  let pk = primaryKey e

  store <- mkTestStore
  counters <- atomically (mkCounters [pk])
  c <- atomically $ newTVar (Counter 0)

  r1 :: Either SomeException (Maybe (Cache.Val Entry)) <- Cache.cachedOrIO store pk (ioWithError c)
  r2 :: Either SomeException (Maybe (Cache.Val Entry)) <- Cache.cachedOrIO store pk (ioWithError c)

  Counter n <- readTVarIO c
  HU.assert $ n == 2
  (be, se, ve) <- atomically $ atomsAreEmpty store
  HU.assert be
  HU.assert se
  HU.assert ve



qcProps = testGroup "Cache properties"
  [ QC.testProperty "IO should happen exactly once" prop_cache_or_io_should_invoke_io_exactly_once
  , QC.testProperty "IO should happen exactly once for IDX queries" prop_idx_cache_or_io_should_invoke_io_exactly_once
  ]

unitTests = testGroup "Unit tests"
  [ HU.testCase "Fermat's last theorem" cache_or_io_much_not_cache_errors_by_default
  ]


main :: IO ()
main = defaultMain $ testGroup "Tests" [qcProps, unitTests]

tests :: TestTree
tests = testGroup "Tests" [qcProps, unitTests]
