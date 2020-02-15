module Database.HDBC.ClickHouse.Protocol.Codec.Decoder (decodeString, decodeNum, readString, readNum, readAll) where

import Control.Exception
import Data.Bits
import Data.Word
import Network.Socket (Socket)
import Network.Socket.ByteString (sendAll, recv)

import qualified Codec.Binary.UTF8.String as C
import qualified Data.ByteString as B
import qualified Data.ByteString.Char8 as B8

decodeString :: B.ByteString -> (String, B.ByteString)
decodeString bs =
  let (num, rest) = decodeNum bs
      size        = fromIntegral num
  in (C.decodeString $ B8.unpack $ B.take size rest, B.drop size rest)

decodeNum :: B.ByteString -> (Word64, B.ByteString)
decodeNum bs = decodeNum' bs 0 0
  where
    decodeNum' :: B.ByteString -> Word64 -> Int -> (Word64, B.ByteString)
    decodeNum' bs' n s =
      case (B.uncons bs') of
        Just (h, t) | h < 0x80  -> (fromIntegral (n .|. (fromIntegral h) `shiftL` s), t)
                    | otherwise -> decodeNum' t (fromIntegral (n .|. ((fromIntegral h) .&. 0x7f) `shiftL` s)) (s + 7)
        Nothing -> (0, B.empty) -- TODO: error handling

readString :: Socket -> IO String
readString sock = do
  size <- readNum sock
  bs <- recv sock $ fromIntegral size
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
        _ -> throwIO $ userError $ "Unexpected Empty Response"

readAll :: Socket -> IO B.ByteString
readAll sock =
  readAll' sock B.empty
  where
    readAll' sock bs = do
      r <- recv sock 1024
      if (B.length r == 0 || B.last r == 4)
        then return $ bs `B.append` r
        else readAll' sock $ bs `B.append` r
