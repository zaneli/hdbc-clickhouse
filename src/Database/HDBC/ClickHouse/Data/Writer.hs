module Database.HDBC.ClickHouse.Data.Writer (encodeValue) where

import Data.IP (fromIPv4, fromIPv6b, toIPv4, toIPv6)
import Data.Time.Clock.POSIX (posixSecondsToUTCTime, utcTimeToPOSIXSeconds)
import Database.HDBC.SqlValue
import Database.HDBC.ClickHouse.Data.Column

import qualified Codec.Binary.UTF8.String as C
import qualified Data.ByteString as B
import qualified Data.ByteString.Char8 as B8
import qualified Database.HDBC.ClickHouse.Codec.Encoder as E

-- TODO: fix handle overflow/underflow
encodeValue :: Column -> SqlValue -> B.ByteString
encodeValue (StringColumn _) (SqlString v) =
  E.encodeString v
encodeValue (Int8Column _) (SqlInt32 v) =
  B.singleton $ fromIntegral v
encodeValue (Int8Column _) (SqlInt64 v) =
  B.singleton $ fromIntegral v
encodeValue (Int16Column _) (SqlInt32 v) =
  E.encodeInt16 $ fromIntegral v
encodeValue (Int16Column _) (SqlInt64 v) =
  E.encodeInt16 $ fromIntegral v
encodeValue (Int32Column _) (SqlInt32 v) =
  E.encodeInt32 v
encodeValue (Int32Column _) (SqlInt64 v) =
  E.encodeInt32 $ fromIntegral v
encodeValue (Int64Column _) (SqlInt64 v) =
  E.encodeInt64 v
encodeValue (UInt8Column _) (SqlWord32 v) =
  B.singleton $ fromIntegral v
encodeValue (UInt8Column _) (SqlWord64 v) =
  B.singleton $ fromIntegral v
encodeValue (UInt8Column _) (SqlInt32 v) =
  B.singleton $ fromIntegral v
encodeValue (UInt8Column _) (SqlInt64 v) =
  B.singleton $ fromIntegral v
encodeValue (UInt16Column _) (SqlWord32 v) =
  E.encodeWord16 $ fromIntegral v
encodeValue (UInt16Column _) (SqlWord64 v) =
  E.encodeWord16 $ fromIntegral v
encodeValue (UInt16Column _) (SqlInt32 v) =
  E.encodeWord16 $ fromIntegral v
encodeValue (UInt16Column _) (SqlInt64 v) =
  E.encodeWord16 $ fromIntegral v
encodeValue (UInt32Column _) (SqlWord32 v) =
  E.encodeWord32 v
encodeValue (UInt32Column _) (SqlWord64 v) =
  E.encodeWord32 $ fromIntegral v
encodeValue (UInt32Column _) (SqlInt32 v) =
  E.encodeWord32 $ fromIntegral v
encodeValue (UInt32Column _) (SqlInt64 v) =
  E.encodeWord32 $ fromIntegral v
encodeValue (UInt64Column _) (SqlWord64 v) =
  E.encodeWord64 v
encodeValue (UInt64Column _) (SqlInt64 v) =
  E.encodeWord64 $ fromIntegral v
encodeValue (Float32Column _) (SqlDouble v) =
  E.encodeFloat $ realToFrac v
encodeValue (Float64Column _) (SqlDouble v) =
  E.encodeDouble v
encodeValue (DateColumn _) (SqlUTCTime v) =
  E.encodeInt16 $ fromIntegral $ truncate $ (\n -> n / 24 / 3600) $ toRational $ utcTimeToPOSIXSeconds v
encodeValue (DateTimeColumn _) (SqlUTCTime v) =
  E.encodeInt32 $ fromIntegral $ truncate $ toRational $ utcTimeToPOSIXSeconds v
encodeValue (IPv4Column _) (SqlString v) =
  B.concat $ map (B.singleton . fromIntegral) $ reverse $ fromIPv4 $ read v
encodeValue (IPv6Column _) (SqlString v) =
  B.concat $ map (B.singleton . fromIntegral) $ fromIPv6b $ read v
encodeValue (FixedStringColumn _ size) (SqlString v) =
  B8.pack $ C.encodeString $ (take size v) ++ (replicate (size - (length v)) '\0')
encodeValue (NullableColumn _ item) SqlNull =
  B.singleton 1 `B.append` (encodeNullValue item)
encodeValue (NullableColumn _ item) value =
  (B.singleton 0) `B.append` (encodeValue item value)
-- TODO: fix all types

encodeNullValue column@(StringColumn _) =
  encodeValue column $ SqlString ""
encodeNullValue column@(Int8Column _) =
  encodeValue column $ SqlInt32 0
encodeNullValue column@(Int16Column _) =
  encodeValue column $ SqlInt32 0
encodeNullValue column@(Int32Column _) =
  encodeValue column $ SqlInt32 0
encodeNullValue column@(Int64Column _) =
  encodeValue column $ SqlInt64 0
encodeNullValue column@(UInt8Column _) =
  encodeValue column $ SqlWord32 0
encodeNullValue column@(UInt16Column _) =
  encodeValue column $ SqlWord32 0
encodeNullValue column@(UInt32Column _) =
  encodeValue column $ SqlWord32 0
encodeNullValue column@(UInt64Column _) =
  encodeValue column $ SqlWord64 0
encodeNullValue column@(Float32Column _) =
  encodeValue column $ SqlDouble 0
encodeNullValue column@(Float64Column _) =
  encodeValue column $ SqlDouble 0
encodeNullValue column@(DateColumn _) =
  encodeValue column $ SqlUTCTime $ posixSecondsToUTCTime 0
encodeNullValue column@(DateTimeColumn _) =
  encodeValue column $ SqlUTCTime $ posixSecondsToUTCTime 0
encodeNullValue column@(UUIDColumn _) =
  encodeValue column $ SqlString "00000000-0000-0000-0000-000000000000"
encodeNullValue column@(IPv4Column _) =
  encodeValue column $ SqlString $ show $ toIPv4 [0, 0, 0, 0]
encodeNullValue column@(IPv6Column _) =
  encodeValue column $ SqlString $ show $ toIPv6 [0, 0, 0, 0, 0, 0, 0, 0]
encodeNullValue column@(FixedStringColumn _ size) =
  encodeValue column $ SqlString ""
