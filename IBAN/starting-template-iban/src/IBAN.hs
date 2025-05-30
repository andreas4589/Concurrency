--
-- INFOB3CC Concurrency
-- Practical 1: IBAN calculator
--
-- http://ics.uu.nl/docs/vakken/b3cc/assessment.html
--
module IBAN (

  Mode(..), Config(..),
  count, list, search

) where

import Control.Concurrent
import Control.Monad
import Crypto.Hash.SHA1
import Data.Atomics                                       ( readForCAS, casIORef, peekTicket )
import Data.IORef
import Data.List                                          ( elemIndex, find)
import Data.Word
import Data.Maybe                                         ( fromJust )
import System.Environment
import System.IO
import Data.ByteString.Char8                              ( ByteString )
import qualified Data.ByteString                          as B
import qualified Data.ByteString.Char8                    as B8
import GHC.IORef                                          (atomicSwapIORef)

-- -----------------------------------------------------------------------------
-- 0. m-test
-- -----------------------------------------------------------------------------

-- Perform the m-test on 'number'. Use `div` and `mod` to extract digits from
-- the number; do not use `show`, as it is too slow.

mtest :: Int -> Int -> Bool
mtest m number = weightedSum number 1 `mod` m == 0
  where
    weightedSum 0 _      = 0
    weightedSum n weight = (n `mod` 10) * weight + weightedSum (n `div` 10) (weight + 1)

-- -----------------------------------------------------------------------------
-- 1. Counting mode (3pt)
-- -----------------------------------------------------------------------------

-- Count the number of valid elements within the ranges split among threads.
count :: Config -> IO Int
count config = do
  counter <- newIORef 0 

  let numTs   = cfgThreads config
      ranges  = splitWork config
      m       = cfgModulus config

  forkThreads numTs $ \threadId -> do
        let (begin, end)  = ranges !! threadId
            validCount    = length (filter (mtest m) [begin..end])
        evaluate validCount
        addToCounter counter validCount
  
  readIORef counter

-- Split the workload into ranges for multi-threading.
splitWork :: Config -> [(Int,Int)]
splitWork config = 
  let upperBound = cfgUpper config
      lowerBound = cfgLower config
      numTs      = cfgThreads config
      rangeSize  = upperBound - lowerBound
      chunkSize  = rangeSize `div` numTs
      remainder  = rangeSize `mod` numTs
      
  in [ (lowerBound + i * chunkSize + min i remainder,
        lowerBound + (i + 1) * chunkSize + min (i + 1) remainder - 1)
        | i <- [0..numTs-1] ]

-- Atomically add a value to the counter using Compare-and-Swap (CAS) for safety.
-- Retries the CAS operation until successful to handle concurrent updates.
addToCounter :: IORef Int -> Int -> IO ()
addToCounter counter value = do
  ticket <- readForCAS counter

  let oldValue = peekTicket ticket
      newValue = oldValue + value

  (success,_) <- casIORef counter ticket newValue -- Attempt to update the counter atomically.

  if success then writeIORef counter newValue   -- Write new value on success.
  else addToCounter counter value               -- Retry if the CAS operation failed due to contention.

-- -----------------------------------------------------------------------------
-- 2. List mode (3pt)
-- -----------------------------------------------------------------------------

-- List all valid elements from the ranges, writing them to a file handle sequentially.
list :: Handle -> Config -> IO ()
list handle config = do
  seqNum      <- newMVar 1  
  outputLock  <- newMVar () 

  let numTs   = cfgThreads config
      ranges  = splitWork config
      m       = cfgModulus config

  forkThreads numTs $ \threadId -> do
      let (begin, end) = ranges !! threadId
      evaluate begin
      evaluate end
      processRange handle begin end seqNum outputLock m

processRange :: Handle -> Int -> Int -> MVar Int -> MVar () -> Int -> IO ()
processRange handle begin end seqVar outputLock m = do
  mapM_ processNumber [begin .. end]
  where
    processNumber num = do
      when (mtest m num) $ do       -- Check if the number is valid.
        seqNum <- takeMVar seqVar   -- Safely get and update the sequence number.
        putMVar seqVar (seqNum + 1)
        writeOutput handle outputLock (show seqNum ++ " " ++ show num)

-- Uses a mutex lock to prevent simultaneous writes by multiple threads.
writeOutput :: Handle -> MVar () -> String -> IO ()
writeOutput handle lock output = do
  takeMVar lock
  hPutStrLn handle output
  putMVar lock ()

-- -----------------------------------------------------------------------------
-- 3. Search mode (4pt)
-- -----------------------------------------------------------------------------

data Queue a =
  Queue (MVar (List a))
        (MVar (List a))
type List a = MVar (Item a)
data Item a = Item a (List a)

-- Initializes both read and write ends to the same empty `MVar`.
newQueue :: IO (Queue a)
newQueue = do
  hole      <- newEmptyMVar
  readLock  <- newMVar hole
  writeLock <- newMVar hole
  return (Queue readLock writeLock)

-- Creates a new empty hole for the queue tail and links it to the new item.
enqueue :: Queue a -> a -> IO ()
enqueue (Queue _ writeLock) val = do
  newHole   <- newEmptyMVar
  let item = Item val newHole
  oldHole   <- takeMVar writeLock
  putMVar oldHole item
  putMVar writeLock newHole

-- Advances the `readLock` to the next item, effectively dequeuing the front element.
dequeue :: Queue a -> IO a
dequeue (Queue readLock _) = do
  oldHole           <- takeMVar readLock
  Item val newHole  <- takeMVar oldHole
  putMVar readLock newHole
  return val

search :: Config -> ByteString -> IO (Maybe Int)
search config query = do
  let (lowerBound, upperBound, numThreads, m) =
        (cfgLower config, cfgUpper config, cfgThreads config, cfgModulus config)

  queue         <- newQueue
  stopFlag      <- newIORef False  -- Flag to signal threads to stop when a result is found. Is set to False because no match is found yet.
  pendingtasks  <- newIORef 1      -- Keep track of the about of tasks in the queue, starts at 1 because the entire range is added to the queue as one element.
  resultVar     <- newMVar Nothing -- MVar to keep track of the result, Is set to Nothing, so if no match is found it returns Nothing.
  
  enqueue queue (lowerBound, upperBound)

  forkThreads numThreads $ \_ ->
    workerLoop queue stopFlag pendingtasks resultVar m query

  readMVar resultVar

cutoffPoint :: Int
cutoffPoint = 3000  -- Minimum size of a range to split further

workerLoop :: Queue (Int, Int) -> IORef Bool -> IORef Int -> MVar (Maybe Int) -> Int -> ByteString -> IO ()
workerLoop queue stopFlag pendingTasks resultVar m query = do
  isFound   <- readIORef stopFlag
  numTasks  <- readIORef pendingTasks

  unless isFound $ do -- When a match is found, stop all threads from searching
    if numTasks == 0  -- If the number of tasks is 0 (Queue is empty), signal all threads to stop by swapping stopflag from False to True.
      then do
        atomicSwapIORef stopFlag True
        return ()
    else do
      (begin, end) <- dequeue queue
      atomicModifyIORef' pendingTasks (\x -> (x - 1, ())) -- Dequeued so lower the amount of numbers with 1
      if (end - begin) < cutoffPoint -- If the range is small enough to do itself:
        then do
          let validCandidates = filter (mtest m) [begin .. end]
          case find (checkHash query . intToString) validCandidates of
            Just match -> do
              atomicSwapIORef stopFlag True -- if a match is found, signal all threads to stop and swap the result MVar to the match  
              swapMVar resultVar (Just match) 
              return ()
            Nothing -> workerLoop queue stopFlag pendingTasks resultVar m query -- else continue recursion
      else do
        let mid = (begin + end) `div` 2 -- when the range is not small enough we split the the work in 2 and but both back in the queue
        enqueue queue (begin, mid)
        enqueue queue (mid + 1, end)
        atomicModifyIORef' pendingTasks (\x -> (x + 2, ()))
        workerLoop queue stopFlag pendingTasks resultVar m query -- continue recursion

intToString :: Int -> String -- Quick helper function to make a String out of an Int thats quicker than show.
intToString n
  | n < 10    = [toEnum (n + 48)]        
  | otherwise = intToString (n `div` 10) ++ [toEnum ((n `mod` 10) + 48)]

-- -----------------------------------------------------------------------------
-- Starting framework
-- -----------------------------------------------------------------------------

data Mode = Count | List | Search ByteString
  deriving Show

data Config = Config
  { cfgLower   :: !Int
  , cfgUpper   :: !Int
  , cfgModulus :: !Int
  , cfgThreads :: !Int
  }
  deriving Show

-- Evaluates a term, before continuing with the next IO operation.
--
evaluate :: a -> IO ()
evaluate x = x `seq` return ()

-- Forks 'n' threads. Waits until those threads have finished. Each thread
-- runs the supplied function given its thread ID in the range [0..n).
--
forkThreads :: Int -> (Int -> IO ()) -> IO ()
forkThreads n work = do
  -- Fork the threads and create a list of the MVars which
  -- per thread tell whether the work has finished.
  finishVars <- mapM work' [0 .. n - 1]
  -- Wait on all MVars
  mapM_ takeMVar finishVars
  where
    work' :: Int -> IO (MVar ())
    work' index = do
      var <- newEmptyMVar
      _   <- forkOn index (work index >> putMVar var ())
      return var

-- Checks whether 'value' has the expected hash.
--
checkHash :: ByteString -> String -> Bool
checkHash expected value = expected == hash (B8.pack value)
