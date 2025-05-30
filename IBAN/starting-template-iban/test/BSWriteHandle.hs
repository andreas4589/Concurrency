{-|
A write-only 'Handle' writing to a 'BS.ByteString'.

The code here is inspired by, and simplified from, the @bytestring-handle@
package <https://hackage.haskell.org/package/bytestring-handle>.
-}

module BSWriteHandle (
  withBSWriteHandle,
) where

import qualified Data.ByteString.Short as BSS
import qualified Data.ByteString as BS
import Data.IORef (IORef, newIORef, readIORef, atomicModifyIORef')
import Foreign.Ptr (castPtr)
import GHC.IO.Buffer (BufferState(..), newByteBuffer)
import GHC.IO.BufferedIO (BufferedIO(..), writeBuf, writeBufNonBlocking)
import GHC.IO.Device (RawIO(..), IODevice(..), IODeviceType(..))
import GHC.IO.Exception (ioException, unsupportedOperation)
import GHC.IO.Handle (Handle, mkFileHandle)
import System.IO (IOMode(..), BufferMode(..),
                  noNewlineTranslation, hSetBuffering, hFlush)


newtype WriteState = WS
  { -- | in reverse order: last chunk is head of list
    wsBuf :: IORef [BSS.ShortByteString]
  }

newWS :: IO WriteState
newWS = WS <$> newIORef []

renderWS :: WriteState -> IO BS.ByteString
renderWS ws = BS.concat . reverse . map BSS.fromShort <$> readIORef (wsBuf ws)

instance RawIO WriteState where
  read _ _ _ _ = ioException unsupportedOperation
  readNonBlocking _ _ _ _ = ioException unsupportedOperation
  -- In the base FD implementation, the offset is also ignored. I'll take that
  -- as licence to ignore it here too.
  write ws src _offset num = do
    chunk <- BSS.packCStringLen (castPtr src, num)
    atomicModifyIORef' (wsBuf ws) (\l -> (chunk : l, ()))
  writeNonBlocking ws src offset num = do
    write ws src offset num
    return num

instance IODevice WriteState where
  ready _ws False _msecs = return False  -- has data to read
  ready _ws True _msecs = return True    -- has space to write
  close _ws = return ()           -- why close?
  devType _ws = return RegularFile

instance BufferedIO WriteState where
  newBuffer _ ReadBuffer = ioException unsupportedOperation
  newBuffer _ws WriteBuffer = newByteBuffer (64 * 1024) WriteBuffer
  fillReadBuffer _ _ = ioException unsupportedOperation
  fillReadBuffer0 _ _ = ioException unsupportedOperation
  flushWriteBuffer = writeBuf
  flushWriteBuffer0 = writeBufNonBlocking

-- | The created handle is in "binary mode": neither text encoding nor newline
-- translation is performed. The handle can not be read from, nor seeked in.
withBSWriteHandle :: (Handle -> IO a) -> IO (a, BS.ByteString)
withBSWriteHandle action = do
  ws <- newWS
  handle <- mkFileHandle ws "<bsWriteHandle>" WriteMode Nothing noNewlineTranslation
  hSetBuffering handle NoBuffering
  res <- action handle
  hFlush handle
  output <- renderWS ws
  return (res, output)
