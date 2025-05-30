{-# LANGUAGE RecordWildCards  #-}
--
-- INFOB3CC Concurrency
-- Practical 2: Single Source Shortest Path
--
--    Δ-stepping: A parallelisable shortest path algorithm
--    https://www.sciencedirect.com/science/article/pii/S0196677403000762
--
-- https://ics.uu.nl/docs/vakken/b3cc/assessment.html
--
-- https://cs.iupui.edu/~fgsong/LearnHPC/sssp/deltaStep.html
--

module DeltaStepping (

  Graph, Node, Distance,
  deltaStepping,

) where

import Sample
import Utils

import Control.Concurrent
import Control.Concurrent.MVar
import Control.Monad
import Data.Bits
import Data.Graph.Inductive                                         ( Gr )
import Data.IORef
import Data.IntMap.Strict                                           ( IntMap )
import Data.IntSet                                                  ( IntSet )
import Data.Vector.Storable                                         ( Vector )
import Data.Word
import Foreign.Ptr
import Foreign.Storable
import Text.Printf
import qualified Data.Graph.Inductive                               as G
import qualified Data.IntMap.Strict                                 as Map
import qualified Data.IntSet                                        as Set
import qualified Data.Vector.Mutable                                as V
import qualified Data.Vector.Storable                               as S ( unsafeFreeze, replicate )
import qualified Data.Vector.Storable.Mutable                       as M
import qualified Data.Graph.Inductive.Internal.Heap as Data.IntSet
import Data.Primitive (emptyArray)
import qualified Data.IntMap as IntMap


type Graph    = Gr String Distance  -- Graphs have nodes labelled with Strings and edges labelled with their distance
type Node     = Int                 -- Nodes (vertices) in the graph are integers in the range [0..]
type Distance = Float               -- Distances between nodes are (positive) floating point values


-- | Find the length of the shortest path from the given node to all other nodes
-- in the graph. If the destination is not reachable from the starting node the
-- distance is 'Infinity'.
--
-- Nodes must be numbered [0..]
--
-- Negative edge weights are not supported.
--
-- NOTE: The type of the 'deltaStepping' function should not change (since that
-- is what the test suite expects), but you are free to change the types of all
-- other functions and data structures in this module as you require.
--
deltaStepping
    :: Bool                             -- Whether to print intermediate states to the console, for debugging purposes
    -> Graph                            -- graph to analyse
    -> Distance                         -- delta (step width, bucket width)
    -> Node                             -- index of the starting node
    -> IO (Vector Distance)
deltaStepping verbose graph delta source = do
  threadCount <- getNumCapabilities             -- the number of (kernel) threads to use: the 'x' in '+RTS -Nx'

  -- Initialise the algorithm
  (buckets, distances)  <- initialise graph delta source
  printVerbose verbose "initialse" graph delta buckets distances

  let
    -- The algorithm loops while there are still non-empty buckets
    loop = do
      done <- allBucketsEmpty buckets
      if done
      then return ()
      else do
        printVerbose verbose "result" graph delta buckets distances
        step verbose threadCount graph delta buckets distances
        loop
  loop

  printVerbose verbose "result" graph delta buckets distances
  -- Once the tentative distances are finalised, convert into an immutable array
  -- to prevent further updates. It is safe to use this "unsafe" function here
  -- because the mutable vector will not be used any more, so referential
  -- transparency is preserved for the frozen immutable vector.
  --
  -- NOTE: The function 'Data.Vector.convert' can be used to translate between
  -- different (compatible) vector types (e.g. boxed to storable)
  --
  S.unsafeFreeze distances

-- Initialise algorithm state
--
initialise
    :: Graph
    -> Distance
    -> Node
    -> IO (Buckets, TentativeDistances)
initialise graph delta source = do
  let numNodes = length $ G.nodes graph
  tentativeDistances <- M.replicate numNodes infinity
  M.write tentativeDistances source 0

  let numBuckets = max 1 (ceiling (fromIntegral numNodes / delta))
  bucketArray <- V.replicate numBuckets Set.empty
  let sourceBucket = 0
  V.modify bucketArray (Set.insert source) sourceBucket
  firstBucket <- newIORef sourceBucket

  return (Buckets firstBucket bucketArray, tentativeDistances)


-- Take a single step of the algorithm.
-- That is, one iteration of the outer while loop.
--
step
    :: Bool
    -> Int
    -> Graph
    -> Distance
    -> Buckets
    -> TentativeDistances
    -> IO ()
step verbose threadCount graph delta buckets distances = do
  i <- findNextBucket buckets -- (* Smallest nonempty bucket *)
  r <- newIORef Set.empty     -- (* No nodes deleted for bucket B[i] yet *)

  let
    loop = do                 -- (* New phase *)
      bucket <- V.read (bucketArray buckets) i  
      let done = Set.null bucket

      if done then return ()
      else do
        printVerbose verbose "inner step" graph delta buckets distances
        req <- findRequests threadCount (<= delta) graph bucket distances -- (* Create requests for light edges *)
        rCon <- readIORef r
        writeIORef r (Set.union bucket rCon)                              -- (* Remember deleted nodes *)
        V.write (bucketArray buckets) i Set.empty                         -- (* Current bucket empty *)
        relaxRequests threadCount buckets distances delta req             -- (* Do relaxations, nodes may (re)enter B[i] *)
        loop
  loop

  rCon <- readIORef r 
  req <- findRequests threadCount (> delta) graph rCon distances   -- (* Create requests for heavy edges *)
  relaxRequests threadCount buckets distances delta req            -- (* Relaxations will not refill B[i] *)

-- Once all buckets are empty, the tentative distances are finalised and the
-- algorithm terminates.
--
allBucketsEmpty :: Buckets -> IO Bool
allBucketsEmpty Buckets{..} = do
   let numBuckets = V.length bucketArray
   foldM
     (\ acc idx
        -> do bucket <- V.read bucketArray idx
              return $ acc && Set.null bucket)
     True [0 .. numBuckets - 1]

-- Return the index of the smallest non-empty bucket. Assumes that there is at
-- least one non-empty bucket remaining.
--
findNextBucket :: Buckets -> IO Int
findNextBucket buckets = do
  go 0
  where
    go index = do
      bucket <- V.read (bucketArray buckets) index
      if Set.null bucket
        then go (index + 1)
        else return index


-- Create requests of (node, distance) pairs that fulfil the given predicate
--
findRequests
    :: Int
    -> (Distance -> Bool)
    -> Graph
    -> IntSet
    -> TentativeDistances
    -> IO (IntMap Distance)
findRequests threadCount p graph v' distances = do
  -- Fold over the set of nodes to build the request map
  foldM processNode IntMap.empty (Set.toList v')
  where
    -- Process a single node and accumulate requests
    processNode acc v = do
      let edges = G.out graph v -- Outgoing edges of node v

      -- Fold over edges to update the map
      foldM (processEdge v) acc edges

    -- Process a single edge and add to the map if it meets the predicate
    processEdge v acc (_, w, c) = do
      -- Calculate the new distance: tent(v) + c(v, w)
      tentV <- M.read distances v
      let newDistance = tentV + c

      -- Check if the distance meets the predicate
      if p newDistance
        then return $ IntMap.insert w newDistance acc
        else return acc

printIntMap :: Show v => IntMap.IntMap v -> IO ()
printIntMap = mapM_ print . IntMap.toList

testFindRequestsSample1 :: IO ()
testFindRequestsSample1 = do
  let graph = sample1
      v' = Set.fromList [0, 1, 2] -- Nodes to consider
      predicate = (< 10)            -- Predicate for filtering distances
      delta = 1.0                   -- Arbitrary delta
  distances <- M.replicate 7 0.0    -- Initialize tentative distances

  -- Set some distances to test with
  M.write distances 0 0.0
  M.write distances 1 3.0
  M.write distances 2 6.0

  -- Run findRequests
  result <- findRequests 1 predicate graph v' distances

  -- Print the result
  putStrLn "Test Case 1 - Sample 1:"
  printIntMap result

testFindRequestsSample2 :: IO ()
testFindRequestsSample2 = do
  let graph = sample2
      v' = Set.fromList [0, 1, 6] -- Nodes to consider
      predicate = (<= 5)            -- Predicate for filtering distances
      delta = 1.0                   -- Arbitrary delta
  distances <- M.replicate 37 0.0   -- Initialize tentative distances

  -- Set some distances to test with
  M.write distances 0 1.0
  M.write distances 1 2.0
  M.write distances 6 3.0

  -- Run findRequests
  result <- findRequests 2 predicate graph v' distances

  -- Print the result
  putStrLn "Test Case 2 - Sample 2:"
  printIntMap result


testFindRequestsEmptySet :: IO ()
testFindRequestsEmptySet = do
  let graph = sample1
      v' = Set.empty -- No nodes to consider
      predicate = (< 10) -- Predicate for filtering distances
      delta = 1.0        -- Arbitrary delta
  distances <- M.replicate 7 0.0 -- Initialize tentative distances

  -- Run findRequests
  result <- findRequests 1 predicate graph v' distances

  -- Print the result
  putStrLn "Test Case 3 - Empty Set:"
  printIntMap result

testFindRequestsHighThreshold :: IO ()
testFindRequestsHighThreshold = do
  let graph = sample2
      v' = Set.fromList [3, 4, 6] -- Nodes to consider
      predicate = (<= 20)           -- High threshold
      delta = 1.0                   -- Arbitrary delta
  distances <- M.replicate 37 0.0   -- Initialize tentative distances

  -- Set some distances to test with
  M.write distances 3 3.0
  M.write distances 4 4.0
  M.write distances 6 5.0

  -- Run findRequests
  result <- findRequests 1 predicate graph v' distances

  -- Print the result
  putStrLn "Test Case 4 - High Threshold:"
  printIntMap result
-- Execute requests for each of the given (node, distance) pairs
--
relaxRequests
    :: Int
    -> Buckets
    -> TentativeDistances
    -> Distance
    -> IntMap Distance
    -> IO ()
relaxRequests threadCount buckets distances delta req = do
  IntMap.foldrWithKey
    (\node newDistance acc -> acc >> relax buckets distances delta (node, newDistance))
    (return ())
    req

-- Execute a single relaxation, moving the given node to the appropriate bucket
-- as necessary
--
relax :: Buckets
      -> TentativeDistances
      -> Distance
      -> (Node, Distance) -- (w, x) in the paper
      -> IO ()
relax buckets distances delta (node, newDistance) = do
  oldDistance <- M.read distances node
  when (newDistance < oldDistance) $ do -- (* Insert or move w in B if x < tent(w) *)
    let oldIndex = floor (oldDistance / delta)
        newIndex = floor (newDistance / delta)
        bArray   = bucketArray buckets
    
    oldBucket <- V.read bArray oldIndex -- (* If in, remove from old bucket *)
    let updatedBucket = Set.delete node oldBucket
    V.write bArray oldIndex updatedBucket
    
    newBucket <- V.read bArray newIndex -- (* Insert into new bucket *)
    let updatedBucket2 = Set.insert node newBucket
    V.write bArray newIndex updatedBucket2

    M.write distances node newDistance  -- tent(w) := x


-- -----------------------------------------------------------------------------
-- Starting framework
-- -----------------------------------------------------------------------------
--
-- Here are a collection of (data)types and utility functions that you can use.
-- You are free to change these as necessary.
--

type TentativeDistances = M.IOVector Distance

data Buckets = Buckets
  { firstBucket   :: {-# UNPACK #-} !(IORef Int)           -- real index of the first bucket (j)
  , bucketArray   :: {-# UNPACK #-} !(V.IOVector IntSet)   -- cyclic array of buckets
  }


-- The initial tentative distance, or the distance to unreachable nodes
--
infinity :: Distance
infinity = 1/0


-- Forks 'n' threads. Waits until those threads have finished. Each thread
-- runs the supplied function given its thread ID in the range [0..n).
--
forkThreads :: Int -> (Int -> IO ()) -> IO ()
forkThreads n action = do
  -- Fork the threads and create a list of the MVars which per thread tell
  -- whether the action has finished.
  finishVars <- mapM work [0 .. n - 1]
  -- Once all the worker threads have been launched, now wait for them all to
  -- finish by blocking on their signal MVars.
  mapM_ takeMVar finishVars
  where
    -- Create a new empty MVar that is shared between the main (spawning) thread
    -- and the worker (child) thread. The main thread returns immediately after
    -- spawning the worker thread. Once the child thread has finished executing
    -- the given action, it fills in the MVar to signal to the calling thread
    -- that it has completed.
    --
    work :: Int -> IO (MVar ())
    work index = do
      done <- newEmptyMVar
      _    <- forkOn index (action index >> putMVar done ())  -- pin the worker to a given CPU core
      return done


printVerbose :: Bool -> String -> Graph -> Distance -> Buckets -> TentativeDistances -> IO ()
printVerbose verbose title graph delta buckets distances = when verbose $ do
  putStrLn $ "# " ++ title
  printCurrentState graph distances
  printBuckets graph delta buckets distances
  putStrLn "Press enter to continue"
  _ <- getLine
  return ()

-- Print the current state of the algorithm (tentative distance to all nodes)
--
printCurrentState
    :: Graph
    -> TentativeDistances
    -> IO ()
printCurrentState graph distances = do
  printf "  Node  |  Label  |  Distance\n"
  printf "--------+---------+------------\n"
  forM_ (G.labNodes graph) $ \(v, l) -> do
    x <- M.read distances v
    if isInfinite x
       then printf "  %4d  |  %5v  |  -\n" v l
       else printf "  %4d  |  %5v  |  %f\n" v l x
  --
  printf "\n"

printBuckets
    :: Graph
    -> Distance
    -> Buckets
    -> TentativeDistances
    -> IO ()
printBuckets graph delta Buckets{..} distances = do
  first <- readIORef firstBucket
  mapM_
    (\idx -> do
      let idx' = first + idx
      printf "Bucket %d: [%f, %f)\n" idx' (fromIntegral idx' * delta) ((fromIntegral idx'+1) * delta)
      b <- V.read bucketArray (idx' `rem` V.length bucketArray)
      printBucket graph b distances
    )
    [ 0 .. V.length bucketArray - 1 ]

-- Print the current bucket
--
printCurrentBucket
    :: Graph
    -> Distance
    -> Buckets
    -> TentativeDistances
    -> IO ()
printCurrentBucket graph delta Buckets{..} distances = do
  j <- readIORef firstBucket
  b <- V.read bucketArray (j `rem` V.length bucketArray)
  printf "Bucket %d: [%f, %f)\n" j (fromIntegral j * delta) (fromIntegral (j+1) * delta)
  printBucket graph b distances

-- Print a given bucket
--
printBucket
    :: Graph
    -> IntSet
    -> TentativeDistances
    -> IO ()
printBucket graph bucket distances = do
  printf "  Node  |  Label  |  Distance\n"
  printf "--------+---------+-----------\n"
  forM_ (Set.toAscList bucket) $ \v -> do
    let ml = G.lab graph v
    x <- M.read distances v
    case ml of
      Nothing -> printf "  %4d  |   -   |  %f\n" v x
      Just l  -> printf "  %4d  |  %5v  |  %f\n" v l x
  --
  printf "\n"


