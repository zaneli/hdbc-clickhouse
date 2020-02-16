module Database.HDBC.ClickHouse.Protocol.Codec.Decoder where

import Control.Exception
import Data.Bits
import Data.Int
import Data.List ((\\))
import Data.Word
import Database.HDBC.ClickHouse.Exception
import Network.Socket (Socket)
import Network.Socket.ByteString (recv)

import qualified Codec.Binary.UTF8.String as C
import qualified Data.ByteString as B
import qualified Data.ByteString.Char8 as B8

readString :: Socket -> IO String
readString sock = do
  size <- readNum sock
  bs <- if size == 0
    then return B.empty
    else recv sock $ fromIntegral size
  return $ C.decodeString $ B8.unpack bs

readNum :: Socket -> IO Word64
readNum sock = readNum' sock 0 0
  where
    readNum' :: Socket -> Word64 -> Int -> IO Word64
    readNum' sock n s = do
      bs <- recv sock 1
      case (B.unpack bs) of
        [b] | b < 0x80  -> return $ fromIntegral (n .|. (fromIntegral b) `shiftL` s)
            | otherwise -> readNum' sock (fromIntegral (n .|. ((fromIntegral b) .&. 0x7f) `shiftL` s)) (s + 7)
        xs -> throwIO $ unexpectedResponse "Number" xs

readBool :: Socket -> IO Bool
readBool sock = do
  bs <- recv sock 1
  case (B.unpack bs) of
    [1] -> return True
    [0] -> return False
    xs  -> throwIO $ unexpectedResponse "Bool" xs

readInt32 :: Socket -> IO Int32
readInt32 sock = do
  bs <- recv sock 4
  let (i, _) =  B.foldl (\(n, s) b -> (n .|. ((fromIntegral b) `shiftL` s), s + 8)) (0::Int32, 0) bs
  return i

readWord64 :: Socket -> IO Word64
readWord64 sock = do
  bs <- recv sock 8
  let (i, _) =  B.foldl (\(n, s) b -> (n .|. ((fromIntegral b) `shiftL` s), s + 8)) (0::Word64, 0) bs
  return i

readException :: Socket -> IO ClickHouseException
readException sock = do
  code <- readInt32 sock
  name <- readString sock
  message <- readString sock
  stackTrace <- readString sock
  hasNested <- readBool sock
  nested <- if hasNested
    then fmap Just (readException sock)
    else return Nothing
  return ServerException {
    code = fromIntegral code,
    name = name,
    message = message \\ (name ++ ": "),
    stackTrace = stackTrace,
    nested = nested
  }

readAll :: Socket -> IO B.ByteString
readAll sock =
  readAll' sock B.empty
  where
    readAll' sock bs = do
      r <- recv sock 1024
      if (B.length r == 0 || B.last r == 4)
        then return $ bs `B.append` r
        else readAll' sock $ bs `B.append` r
