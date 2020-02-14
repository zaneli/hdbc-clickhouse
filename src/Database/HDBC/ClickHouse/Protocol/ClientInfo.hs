module Database.HDBC.ClickHouse.Protocol.ClientInfo where

import Data.Word

name :: String
name = "hdbc-clickhouse"

majorVersion :: Word8
majorVersion = 1

minorVersion :: Word8
minorVersion = 1

reversion :: Int
reversion = 54431
