module Database.HDBC.ClickHouse.Protocol.Hello (request, response) where

import Control.Exception
import Data.Word
import Network.Socket (Socket)
import Network.Socket.ByteString (recv)

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

response :: Socket -> IO (String, Int, Int, Int, Maybe String)
response sock = do
  bs <- recv sock 1
  case (B.unpack bs) of
    [responseType] -> return ()
    xs             -> throwIO $ userError $ "Unexpected Response: " ++ (show xs)
  serverName <- D.readString sock
  majorVersion <- D.readNum sock
  minorVersion <- D.readNum sock
  revision <- D.readNum sock
  timezone <- if revision >= minRevisionWithServerTimeZone
    then fmap Just $ D.readString sock
    else return Nothing

  D.readAll sock

  return (serverName, fromIntegral majorVersion, fromIntegral minorVersion, fromIntegral revision, timezone)

requestType :: Word8
requestType = 0

responseType :: Word8
responseType = 0

majorVersion :: Word8
majorVersion = 1

minorVersion :: Word8
minorVersion = 1

clientReversion :: Word64
clientReversion = 54431

minRevisionWithServerTimeZone :: Word64
minRevisionWithServerTimeZone = 54058
