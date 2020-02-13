module Database.HDBC.ClickHouse.Protocol.Hello (request, response) where

import Data.Word

import qualified Data.ByteString as B
import qualified Data.ByteString.Char8 as B8
import qualified Database.HDBC.ClickHouse.Protocol.Encoder as E
import qualified Database.HDBC.ClickHouse.Protocol.Decoder as D

request :: String -> String -> String -> B.ByteString
request database username password =
  B8.concat [
    B.singleton requestType,
    E.encodeString "ClickHouse hdbc-clickhouse",
    B.singleton majorVersion,
    B.singleton minorVersion,
    E.encodeNum clientReversion,
    E.encodeString database,
    E.encodeString username,
    E.encodeString password
  ]

response bs =
  let (serverName, rest1) = D.decodeString bs
      (majorVersion, rest2) = D.decodeNum rest1
      (minorVersion, rest3) = D.decodeNum rest2
      (revision, rest4) = D.decodeNum rest3
      (timezone, _) = if revision >= minRevisionWithServerTimeZone then D.decodeString rest4 else ("", rest4)
  in (serverName, majorVersion, minorVersion, revision, timezone)

requestType :: Word8
requestType = 0

majorVersion :: Word8
majorVersion = 1

minorVersion :: Word8
minorVersion = 1

clientReversion :: Word64
clientReversion = 54431

minRevisionWithServerTimeZone :: Word64
minRevisionWithServerTimeZone = 54058
