module Database.HDBC.ClickHouse.Data.Writer (encodeValue) where

import Database.HDBC.SqlValue
import Database.HDBC.ClickHouse.Data.Column

import qualified Data.ByteString as B
import qualified Database.HDBC.ClickHouse.Codec.Encoder as E

encodeValue :: Column -> SqlValue -> B.ByteString
encodeValue (StringColumn _) (SqlString v) =
  E.encodeString v
-- TODO: fix all types
