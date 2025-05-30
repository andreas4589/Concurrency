{-# LANGUAGE NumericUnderscores #-}
{-# LANGUAGE OverloadedStrings  #-}
{-# LANGUAGE RecordWildCards    #-}

module Main where

import IBAN                                                         as IBAN

import BSWriteHandle

import Control.DeepSeq
import Control.Monad
import Control.Concurrent                                           ( setNumCapabilities )
import Data.List                                                    ( sort, elemIndex )
import Data.Maybe                                                   ( fromJust )
import Data.Word                                                    ( Word8 )
import Data.Void
import Test.Tasty                                                   hiding ( defaultMain )
import Test.Tasty.Bench
import Test.Tasty.HUnit
import Test.Tasty.Runners
import Text.Megaparsec
import Text.Megaparsec.Char
import Text.Printf
import qualified Data.ByteString                                    as B
import qualified Data.ByteString.Char8                              as B8
import qualified Text.Megaparsec.Char.Lexer                         as L


main :: IO ()
main = defaultMain
  [ localOption (NumThreads 1)                            -- run each test sequentially with many cores
  $ localOption (mkTimeout 20_000_000)                    -- timeout each test after 20 s
  $ testGroup "test"
      [ testGroup "count"
        [ test_count 1 (Config 139483  928234   11 1) 71705
        , test_count 1 (Config 2824374 25823728 24 1) 958317
        , test_count 5 (Config 2824374 25823728 24 2) 958317
        , test_count 5 (Config 2824374 25823728 24 4) 958317
        , test_count 5 (Config 2824374 25823728 24 16) 958317
        , test_count 5 (Config 2824374 25823728 24 7) 958317
        , test_count 1 (Config 0 10 1 3) 10
        , test_count 1 (Config 0 10 1 17) 10
        ]
      , let expected1 =
              [ 123456804, 123456812, 123456820, 123456889, 123456897
              , 123456901, 123456978, 123456986, 123456994, 123457003
              , 123457011, 123457088, 123457096, 123457100, 123457169
              , 123457177, 123457185, 123457193, 123457258, 123457266
              , 123457274, 123457282, 123457290, 123457339, 123457347
              , 123457355, 123457363, 123457371, 123457428, 123457436
              , 123457444, 123457452, 123457460, 123457509, 123457517
              , 123457525, 123457533, 123457541, 123457606, 123457614
              , 123457622, 123457630, 123457699, 123457703, 123457711
              , 123457788 ]
            expected2 = [ 0, 1, 2, 3, 4, 5, 6, 7, 8, 9 ]
        in
        testGroup "list"
          [ test_list 1 (Config 123456789 123457789 21 1) expected1
          , test_list 5 (Config 123456789 123457789 21 2) expected1
          , test_list 5 (Config 123456789 123457789 21 3) expected1
          , test_list 5 (Config 123456789 123457789 21 8) expected1
          , test_list 5 (Config 123456789 123466789 1 8) [123456789..123466788]
          , test_list 1 (Config 0 10 1 3) expected2
          , test_list 1 (Config 0 10 1 17) expected2
          ]
      , testGroup "search"
        [ test_search 1 (Config 274856170 274856190 11 4) "ba9cd915c8e359d9733edcfe9c61e5aca92afb" Nothing
        , test_search 1 (Config 274856170 274856190 12 4) "c736ca9048d0967a27ec3833832f7ffb571ebd2f" Nothing
        , test_search 1 (Config 274856170 274856190 11 4) "c736ca9048d0967a27ec3833832f7ffb571ebd2f" (Just 274856182)
        , test_search 1 (Config 261756170 274896190 47 4) "a8428d9c87323e977978a67ee48827ca154bd84b" (Just 274886190)
        , test_search 1 (Config 261756170 262035744 1 1) "356a192b7913b04c54574d18c28d46e6395428ab" Nothing
        , test_search 5 (Config 261756170 262035744 1 2) "356a192b7913b04c54574d18c28d46e6395428ab" Nothing
        , test_search 5 (Config 261756170 262035744 1 4) "356a192b7913b04c54574d18c28d46e6395428ab" Nothing
        ]
      ]
  , localOption WallTime                                  -- benchmark using wall-time rather than CPU-time
  $ localOption (NumThreads 1)                            -- run each test sequentially with many cores
  $ localOption (mkTimeout 20_000_000)                    -- timeout each test after 20 s
  $ bgroup "bench"
      [ bgroup "count"
        [                                      bench "N1" $ nfAppIO_N 1 IBAN.count (Config 2824374 25823728 24 1)
        , bcompareWithin 0.4  0.8 "count.N1" $ bench "N2" $ nfAppIO_N 2 IBAN.count (Config 2824374 25823728 24 2)
        , bcompareWithin 0.2  0.6 "count.N1" $ bench "N4" $ nfAppIO_N 4 IBAN.count (Config 2824374 25823728 24 4)
        , bcompareWithin 0.09 0.5 "count.N1" $ bench "N8" $ nfAppIO_N 8 IBAN.count (Config 2824374 25823728 24 8)
        ]
      , bgroup "search"
        [                                       bench "N1" $ nfAppIO_N 1 (IBAN.search (Config 261756170 264551910 1 1)) "356a192b7913b04c54574d18c28d46e6395428ab"
        , bcompareWithin 0.4  0.8 "search.N1" $ bench "N2" $ nfAppIO_N 2 (IBAN.search (Config 261756170 264551910 1 2)) "356a192b7913b04c54574d18c28d46e6395428ab"
        , bcompareWithin 0.2  0.6 "search.N1" $ bench "N4" $ nfAppIO_N 4 (IBAN.search (Config 261756170 264551910 1 4)) "356a192b7913b04c54574d18c28d46e6395428ab"
        , bcompareWithin 0.09 0.5 "search.N1" $ bench "N8" $ nfAppIO_N 8 (IBAN.search (Config 261756170 264551910 1 8)) "356a192b7913b04c54574d18c28d46e6395428ab"
        ]
      ]
  ]

test_count
    :: Int                  -- test repeats
    -> Config               -- input
    -> Int                  -- expected
    -> TestTree
test_count repeats config expected =
  let test = do
        setNumCapabilities (IBAN.cfgThreads config)
        actual <- IBAN.count config
        expected @=? actual
  in
  if repeats > 1
    then testGroup (mkName config) $ map (\i -> testCase (show i) test) [1 .. repeats]
    else testCase  (mkName config) test

test_list
    :: Int                  -- repeats
    -> Config               -- input
    -> [Int]                -- expected (sorted)
    -> TestTree
test_list repeats config expected =
  let test = do
        ((), result) <- withBSWriteHandle $ \handle -> IBAN.list handle config
        let resultstr = B8.unpack result
        case runParser (sc *> many pair <* eof) "iban-calculator" resultstr of
          Left err     -> assertFailure (errorBundlePretty err)
          Right actual -> do
            let (seqs, accs) = unzip actual
                n            = length expected
            --
            assertEqual "sequence numbers are incorrect" [1..n] seqs
            assertEqual "bank account numbers are incorrect" expected (sort accs)
  in
  if repeats > 1
    then testGroup (mkName config) $ map (\i -> testCase (show i) test) [1 .. repeats]
    else testCase  (mkName config) test

test_search
    :: Int                  -- repeats
    -> Config               -- input
    -> String               -- target (in hex)
    -> Maybe Int            -- expected
    -> TestTree
test_search repeats config target mexpected =
  let test = do
        mactual <- IBAN.search config (B.pack (readHash target))
        mexpected @=? mactual
  in
  if repeats > 1
    then testGroup (mkName config) $ map (\i -> testCase (show i) test) [1 .. repeats]
    else testCase  (mkName config) test

-- | Same as 'nfAppIO', but set the number of capabilities to the given number
-- before calling the function.
nfAppIO_N :: NFData b => Int -> (a -> IO b) -> a -> Benchmarkable
nfAppIO_N num fun arg = nfAppIO (\x -> setNumCapabilities num >> fun x) arg

mkName :: Config -> TestName
mkName Config{..} = printf "%d %d %d %d" cfgLower cfgUpper cfgModulus cfgThreads

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

type Parser = Parsec Void String

sc :: Parser ()
sc = L.space space1 comment empty

comment :: Parser ()
comment = L.skipLineComment "#" >> (void eol <|> eof) >> return ()

symbol :: String -> Parser String
symbol = L.symbol sc

lexeme :: Parser a -> Parser a
lexeme = L.lexeme sc

int :: Parser Int
int = L.signed sc (lexeme L.decimal)

pair :: Parser (Int, Int)
pair = do
  x <- int <* sc
  y <- int <* sc
  return (x,y)
