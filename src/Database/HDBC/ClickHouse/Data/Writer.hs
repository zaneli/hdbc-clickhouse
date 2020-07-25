module Database.HDBC.ClickHouse.Data.Writer (encodeValue) where

import Database.HDBC.SqlValue
import Database.HDBC.ClickHouse.Data.Column

import qualified Data.ByteString as B
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
-- TODO: fix all types
