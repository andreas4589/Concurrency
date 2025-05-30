--
-- INFOB3CC Concurrency
-- Practical 1: IBAN calculator
--
-- http://ics.uu.nl/docs/vakken/b3cc/assessment.html
--
module Main where

import IBAN

import Data.List                                          ( elemIndex )
import Data.Maybe                                         ( fromJust )
import Data.Word
import System.Environment
import System.IO
import qualified Data.ByteString                          as B

import GHC.Conc


main :: IO ()
main = do
  hSetBuffering stdout NoBuffering
  args <- getArgs
  let (mode, config) = parseConfig args
  setNumCapabilities (cfgThreads config)
  case mode of
    Count    -> count config >>= print
    List     -> list stdout config
    Search q -> do
      mresult <- search config q
      case mresult of
        Nothing -> putStrLn "not found"
        Just res -> print res

parseConfig :: [String] -> (Mode, Config)
parseConfig (b : e : m : threads : mode' : rest) =
  (mode, Config (read b) (read e) (read m) (read threads))
  where
    mode = case (mode', rest) of
             ("count",  [])  -> Count
             ("list",   [])  -> List
             ("search", [q]) -> Search (B.pack $ readHash q)
             _               -> error "Illegal mode or wrong number of arguments"
parseConfig _ = error "Wrong number of arguments"

readHash :: String -> [Word8]
readHash []         = []
readHash [_]        = error "Illegal hash"
readHash (c1:c2:cs) = v1 * 16 + v2 : readHash cs
  where
    v1      = value c1
    v2      = value c2
    value c = fromIntegral
            $ fromJust
            $ c `elemIndex` (['0'..'9'] ++ ['a'..'f'] ++ error "Illegal hash")
