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
import GHC.Generics (Generic)

import qualified ListT as LT

import Test.QuickCheck (arbitrary, Property, Arbitrary, quickCheck, choose, sized, vectorOf)
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

newtype Counter = Counter Int deriving (Eq, Ord, Show, Generic)

ioByKey :: TVar Counter -> IKey -> IO (Maybe (Val Entry))
ioByKey c pk = atomically $ do
  modifyTVar' c $ \(Counter c') -> Counter (c' + 1)
  return $ Just $ Val (pk, "s:" ++ show pk, False) (Version 0)

mkTestStore = do
  store :: Store IKey (Val Entry) <- atomically mkStore
  atomically $ do
    sIdx <- idxFun (\(Val (_, s, _) _) -> s)
    bIdx <- idxFun (\(Val (_, _, b) _) -> b)
    return $ IdxSet store (sIdx :+: bIdx :+: Nil)

primaryKey :: Entry -> IKey
primaryKey (pk, _, _) = pk

mkValAtoms :: STM (Atoms pk (ValAtom v))
mkValAtoms = Atoms <$> TM.new

mkIdxAtoms :: STM (Atoms i (IdxAtom (pk, Val v)))
mkIdxAtoms = Atoms <$> TM.new

mkCounters :: [IKey] -> STM [(IKey, TVar Counter)]
mkCounters keys = forM keys $ \k -> (k,) <$> newTVar (Counter 0)

prop_cache_or_io_should_invoke_io_exactly_once :: Property
prop_cache_or_io_should_invoke_io_exactly_once = monadicIO $ do
  randomEntries :: [Entry] <- pick arbitrary
  let entries = nubBy (\(k1, _, _) (k2, _, _)-> k1 == k2) randomEntries

  delays :: [Int] <- pick arbitrary

  gStore <- run mkTestStore
  (valAtoms, idxAtoms) <- run $ atomically $ (,) <$> mkValAtoms <*> mkIdxAtoms

  requestPks :: [IKey] <- pick $ sized $ \s -> do
                        n <- choose (0, 10)
                        vectorOf n arbitrary

  args <- run $ atomically (mkCounters requestPks)

  results <- run $ forM args $ \(pk, c) ->
      async $ cachedOrIO valAtoms gStore pk (ioByKey c)

  run $ mapM_ wait results
  counters <- run $ atomically $ forM args $ \(_, c) -> readTVar c
  assert $ null counters || all (\(Counter c) -> c == 1) counters


main :: IO ()
main = do
  quickCheck prop_cache_or_io_should_invoke_io_exactly_once
