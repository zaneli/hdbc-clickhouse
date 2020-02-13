module Database.HDBC.ClickHouse.Protocol.Decoder (decodeString, decodeNum) where

import Data.Bits
import Data.Word

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
