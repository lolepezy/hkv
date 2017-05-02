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

import Data.Hashable
import GHC.Generics (Generic)

import Data.Store.KV
import Data.Store.Cache

newtype IKey = IKey Int deriving (Eq, Generic)
instance Hashable IKey

type Entry = (IKey, String, Bool)
type EntryIdxs = '[String, Bool]

type Store_ = GenStore EntryIdxs IKey Entry

main :: IO ()
main = do
  store :: Store IKey Entry <- atomically mkStore
  gStore :: Store_ <- atomically $ do
    sIdx <- idxFun (\(_, s, _) -> s)
    bIdx <- idxFun (\(_, _, b) -> b)
    return $ IdxSet store (sIdx :+: bIdx :+: Nil)

  let x = getIdx gStore ("1" :: String)
  let y = getIdx gStore True

  atomically $ updateStore gStore (IKey 10) (IKey 10, "something", True)

  print "-----------"
