module Database.HDBC.ClickHouse.Data.Writer (encodeValue) where

import Data.Either
import Data.IP (fromIPv4, fromIPv6b)
import Data.Time.Clock.POSIX (utcTimeToPOSIXSeconds)
import Database.HDBC.SqlValue
import Database.HDBC.ClickHouse.Data.Column
import Text.Printf

import qualified Codec.Binary.UTF8.String as C
import qualified Data.ByteString as B
import qualified Data.ByteString.Char8 as B8
import qualified Database.HDBC.ClickHouse.Codec.Encoder as E

encodeValue :: Column -> SqlValue -> Either String B.ByteString
encodeValue (StringColumn _) (SqlString v) =
  Right $ E.encodeString v

encodeValue column@(Int8Column _) value@(SqlInt32 v) =
  checkRange column value v B.singleton (-128) 127
encodeValue column@(Int8Column _) value@(SqlInt64 v) =
  checkRange column value v B.singleton (-128) 127

encodeValue column@(Int16Column _) value@(SqlInt32 v) =
  checkRange column value v E.encodeInt16 (-32768) 32767
encodeValue column@(Int16Column _) value@(SqlInt64 v) =
  checkRange column value v E.encodeInt16 (-32768) 32767

encodeValue (Int32Column _) (SqlInt32 v) =
  Right $ E.encodeInt32 v
encodeValue column@(Int32Column _) value@(SqlInt64 v) =
  checkRange column value v E.encodeInt32 (-2147483648) 2147483647

encodeValue (Int64Column _) (SqlInt64 v) =
  Right $ E.encodeInt64 v

encodeValue column@(UInt8Column _) value@(SqlWord32 v) =
  checkRange column value v B.singleton 0 255
encodeValue column@(UInt8Column _) value@(SqlWord64 v) =
  checkRange column value v B.singleton 0 255
encodeValue column@(UInt8Column _) value@(SqlInt32 v) =
  checkRange column value v B.singleton 0 255
encodeValue column@(UInt8Column _) value@(SqlInt64 v) =
  checkRange column value v B.singleton 0 255

encodeValue column@(UInt16Column _) value@(SqlWord32 v) =
  checkRange column value v E.encodeWord16 0 65535
encodeValue column@(UInt16Column _) value@(SqlWord64 v) =
  checkRange column value v E.encodeWord16 0 65535
encodeValue column@(UInt16Column _) value@(SqlInt32 v) =
  checkRange column value v E.encodeWord16 0 65535
encodeValue column@(UInt16Column _) value@(SqlInt64 v) =
  checkRange column value v E.encodeWord16 0 65535

encodeValue (UInt32Column _) (SqlWord32 v) =
  Right $ E.encodeWord32 v
encodeValue column@(UInt32Column _) value@(SqlWord64 v) =
  checkRange column value v E.encodeWord32 0 4294967295
encodeValue column@(UInt32Column _) value@(SqlInt32 v) =
  checkRange column value v E.encodeWord32 0 2147483647 -- Literal 4294967295 is out of the GHC.Int.Int32 range -2147483648..2147483647
encodeValue column@(UInt32Column _) value@(SqlInt64 v) =
  checkRange column value v E.encodeWord32 0 4294967295

encodeValue (UInt64Column _) (SqlWord64 v) =
  Right $ E.encodeWord64 v
encodeValue column@(UInt64Column _) value@(SqlInt64 v) =
  checkRange column value v E.encodeWord64 0 9223372036854775807 -- Literal 18446744073709551615 is out of the GHC.Int.Int64 range -9223372036854775808..9223372036854775807

encodeValue column@(Float32Column _) value@(SqlDouble v) =
  let f = (realToFrac v)::Float in
  if (isNaN f || isInfinite f)
    then Left $ printf "unsupported value: column=%s, value=%s" (show column) (show value)
    else Right $ E.encodeFloat f
encodeValue (Float64Column _) (SqlDouble v) =
  Right $ E.encodeDouble v

encodeValue (DateColumn _) (SqlUTCTime v) =
  Right $ E.encodeInt16 $ fromIntegral $ truncate $ (\n -> n / 24 / 3600) $ toRational $ utcTimeToPOSIXSeconds v
encodeValue (DateTimeColumn _) (SqlUTCTime v) =
  Right $ E.encodeInt32 $ fromIntegral $ truncate $ toRational $ utcTimeToPOSIXSeconds v

encodeValue (UUIDColumn _) (SqlString v) =
  Left "encode uuid not implemented"

encodeValue (IPv4Column _) (SqlString v) =
  Right $ B.concat $ map (B.singleton . fromIntegral) $ reverse $ fromIPv4 $ read v
encodeValue (IPv6Column _) (SqlString v) =
  Right $ B.concat $ map (B.singleton . fromIntegral) $ fromIPv6b $ read v

encodeValue (FixedStringColumn _ size) (SqlString v) =
  Right $ B8.pack $ C.encodeString $ (take size v) ++ (replicate (size - (length v)) '\0')

encodeValue (ArrayColumn _ _) (SqlString v) =
  Left "encode array not implemented"

encodeValue (NullableColumn _ item) SqlNull =
  fmap (\v -> B.singleton 1 `B.append` v) (encodeValue item $ nullValue item)
encodeValue (NullableColumn _ item) value =
  fmap (\v -> B.singleton 0 `B.append` v) (encodeValue item value)

encodeValue _ (SqlByteString v) =
  Right v

encodeValue column value =
  Left $ printf "unsupported type: column=%s, value=%s" (show column) (show value)

checkRange :: (Integral a, Integral b) => Column -> SqlValue -> a -> (b -> B.ByteString) -> a -> a -> Either String B.ByteString
checkRange column value n f min max =
  if (n >= min && n <= max)
    then Right $ f $ fromIntegral n
    else Left $ printf "unsupported value: column=%s, value=%s" (show column) (show value)
