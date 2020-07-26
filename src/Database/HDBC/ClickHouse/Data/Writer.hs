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

-- TODO: fix handle overflow/underflow
encodeValue :: Column -> SqlValue -> Either String B.ByteString
encodeValue (StringColumn _) (SqlString v) =
  Right $ E.encodeString v
encodeValue (Int8Column _) (SqlInt32 v) =
  Right $ B.singleton $ fromIntegral v
encodeValue (Int8Column _) (SqlInt64 v) =
  Right $ B.singleton $ fromIntegral v
encodeValue (Int16Column _) (SqlInt32 v) =
  Right $ E.encodeInt16 $ fromIntegral v
encodeValue (Int16Column _) (SqlInt64 v) =
  Right $ E.encodeInt16 $ fromIntegral v
encodeValue (Int32Column _) (SqlInt32 v) =
  Right $ E.encodeInt32 v
encodeValue (Int32Column _) (SqlInt64 v) =
  Right $ E.encodeInt32 $ fromIntegral v
encodeValue (Int64Column _) (SqlInt64 v) =
  Right $ E.encodeInt64 v
encodeValue (UInt8Column _) (SqlWord32 v) =
  Right $ B.singleton $ fromIntegral v
encodeValue (UInt8Column _) (SqlWord64 v) =
  Right $ B.singleton $ fromIntegral v
encodeValue (UInt8Column _) (SqlInt32 v) =
  Right $ B.singleton $ fromIntegral v
encodeValue (UInt8Column _) (SqlInt64 v) =
  Right $ B.singleton $ fromIntegral v
encodeValue (UInt16Column _) (SqlWord32 v) =
  Right $ E.encodeWord16 $ fromIntegral v
encodeValue (UInt16Column _) (SqlWord64 v) =
  Right $ E.encodeWord16 $ fromIntegral v
encodeValue (UInt16Column _) (SqlInt32 v) =
  Right $ E.encodeWord16 $ fromIntegral v
encodeValue (UInt16Column _) (SqlInt64 v) =
  Right $ E.encodeWord16 $ fromIntegral v
encodeValue (UInt32Column _) (SqlWord32 v) =
  Right $ E.encodeWord32 v
encodeValue (UInt32Column _) (SqlWord64 v) =
  Right $ E.encodeWord32 $ fromIntegral v
encodeValue (UInt32Column _) (SqlInt32 v) =
  Right $ E.encodeWord32 $ fromIntegral v
encodeValue (UInt32Column _) (SqlInt64 v) =
  Right $ E.encodeWord32 $ fromIntegral v
encodeValue (UInt64Column _) (SqlWord64 v) =
  Right $ E.encodeWord64 v
encodeValue (UInt64Column _) (SqlInt64 v) =
  Right $ E.encodeWord64 $ fromIntegral v
encodeValue (Float32Column _) (SqlDouble v) =
  Right $ E.encodeFloat $ realToFrac v
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
