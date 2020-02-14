module Database.HDBC.ClickHouse.Protocol.Hello (send) where

import Control.Exception
import Data.Word
import Network.Socket (Socket)
import Network.Socket.ByteString (sendAll, recv)

import qualified Data.ByteString as B
import qualified Data.ByteString.Char8 as B8
import qualified Database.HDBC.ClickHouse.Protocol.Codec.Encoder as E
import qualified Database.HDBC.ClickHouse.Protocol.Codec.Decoder as D
import qualified Database.HDBC.ClickHouse.Protocol.PacketTypes.Client as Client
import qualified Database.HDBC.ClickHouse.Protocol.PacketTypes.Server as Server
import qualified Database.HDBC.ClickHouse.Protocol.ClientInfo as ClientInfo
import qualified Database.HDBC.ClickHouse.Protocol.ServerInfo as ServerInfo

send :: Socket -> String -> String -> String -> Bool -> IO (String, Int, Int, Int, Maybe String)
send sock database username password debug = do
  sendAll sock $ request database username password
  res <- response sock
  D.readAll sock
  if debug
    then print res
    else return ()
  return res

request :: String -> String -> String -> B.ByteString
request database username password =
  B8.concat [
    B.singleton Client.hello,
    E.encodeString ClientInfo.name,
    B.singleton ClientInfo.majorVersion,
    B.singleton ClientInfo.minorVersion,
    E.encodeNum ClientInfo.reversion,
    E.encodeString database,
    E.encodeString username,
    E.encodeString password
  ]

response :: Socket -> IO (String, Int, Int, Int, Maybe String)
response sock = do
  bs <- recv sock 1
  case (B.unpack bs) of
    [x] | x == Server.hello -> return ()
    xs                      -> throwIO $ userError $ "Unexpected Response: " ++ (show xs)
  serverName <- D.readString sock
  majorVersion <- D.readNum sock
  minorVersion <- D.readNum sock
  revision <- fmap fromIntegral $ D.readNum sock
  timezone <- if revision >= ServerInfo.minRevisionWithServerTimeZone
    then fmap Just $ D.readString sock
    else return Nothing
  return (serverName, fromIntegral majorVersion, fromIntegral minorVersion, fromIntegral revision, timezone)
