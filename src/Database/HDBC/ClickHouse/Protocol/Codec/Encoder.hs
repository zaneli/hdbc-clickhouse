module Database.HDBC.ClickHouse.Protocol.Codec.Encoder where

import Data.Bits
import Data.Word

import qualified Codec.Binary.UTF8.String as C
import qualified Data.ByteString as B
import qualified Data.ByteString.Char8 as B8
import qualified Data.ByteString.Lazy as L
import qualified Data.ByteString.Builder as Builder

encodeString :: String -> B.ByteString
encodeString str =
  let encoded = B8.pack $ C.encodeString str
      size    = B.singleton $ fromIntegral $ B.length encoded
  in size `B8.append` encoded

encodeNum :: (Integral a, Bits a) => a -> B.ByteString
encodeNum num =
  B.unfoldr f num
    where f 0 = Nothing
          f x | x >= 0x80 = Just (fromIntegral (x .|. 0x80), x `shiftR` 7)
              | otherwise = Just (fromIntegral x, 0)

encodeWord32 :: Word32 -> B.ByteString
encodeWord32 w32 =
  L.toStrict $ Builder.toLazyByteString $ Builder.word32BE w32
