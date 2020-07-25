module Database.HDBC.ClickHouse.Codec.Encoder where

import Data.Bits (Bits, shiftR, (.|.))
import Data.Int (Int16, Int32, Int64)
import Data.Word (Word16, Word32, Word64)

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
encodeNum 0   = B.singleton 0
encodeNum num = encodeNum' num
  where
    encodeNum' num =
      B.unfoldr f num
        where f 0 = Nothing
              f x | x >= 0x80 = Just (fromIntegral (x .|. 0x80), x `shiftR` 7)
                  | otherwise = Just (fromIntegral x, 0)

encodeWord16 :: Word16 -> B.ByteString
encodeWord16 w16 =
  L.toStrict $ Builder.toLazyByteString $ Builder.word16LE w16

encodeWord32 :: Word32 -> B.ByteString
encodeWord32 w32 =
  L.toStrict $ Builder.toLazyByteString $ Builder.word32LE w32

encodeWord64 :: Word64 -> B.ByteString
encodeWord64 w64 =
  L.toStrict $ Builder.toLazyByteString $ Builder.word64LE w64

encodeInt16 :: Int16 -> B.ByteString
encodeInt16 i16 =
  L.toStrict $ Builder.toLazyByteString $ Builder.int16LE i16

encodeInt32 :: Int32 -> B.ByteString
encodeInt32 i32 =
  L.toStrict $ Builder.toLazyByteString $ Builder.int32LE i32

encodeInt64 :: Int64 -> B.ByteString
encodeInt64 i64 =
  L.toStrict $ Builder.toLazyByteString $ Builder.int64LE i64

encodeFloat :: Float -> B.ByteString
encodeFloat f =
  L.toStrict $ Builder.toLazyByteString $ Builder.floatLE f

encodeDouble :: Double -> B.ByteString
encodeDouble d =
  L.toStrict $ Builder.toLazyByteString $ Builder.doubleLE d
