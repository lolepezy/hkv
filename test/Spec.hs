import qualified STMContainers.Set as TS
import qualified STMContainers.Map as TM

import Control.Concurrent.STM

main :: IO ()
main = do
  t <- atomically $ do
    t' <- TM.new
    TM.insert (1 :: Int) ("2" :: String) t'
    TM.insert (3 :: Int) ("2" :: String) t'
    return t'

  print =<< atomically (TM.lookup ("2" :: String) t)
