module Database.HDBC.ClickHouse.Protocol.Hello (send) where

import Control.Exception
import Data.Word
import Network.Socket (Socket)
import Network.Socket.ByteString (sendAll, recv)
import Database.HDBC.ClickHouse.Protocol

import qualified Data.ByteString as B
import qualified Data.ByteString.Char8 as B8
import qualified Database.HDBC.ClickHouse.Protocol.Codec.Encoder as E
import qualified Database.HDBC.ClickHouse.Protocol.Codec.Decoder as D
import qualified Database.HDBC.ClickHouse.Protocol.PacketTypes.Client as Client
import qualified Database.HDBC.ClickHouse.Protocol.PacketTypes.Server as Server

send :: Socket -> Config -> IO ServerInfo
send sock config = do
  request sock config
  res <- response sock
  if (debug config)
    then print res
    else return ()
  return res

request :: Socket -> Config -> IO ()
request sock config =
  sendAll sock $ B8.concat [
      B.singleton Client.hello,
      E.encodeString $ clientName clientInfo,
      B.singleton $ fromIntegral $ clientMajorVersion clientInfo,
      B.singleton $ fromIntegral $ clientMinorVersion clientInfo,
      E.encodeNum $ clientRevision clientInfo,
      E.encodeString $ database config,
      E.encodeString $ username config,
      E.encodeString $ password config
    ]

response :: Socket -> IO ServerInfo
response sock = do
  bs <- recv sock 1
  case (B.unpack bs) of
    [x] | x == Server.hello -> return ()
    xs                      -> throwIO $ userError $ "Unexpected Response: " ++ (show xs)
  serverName <- D.readString sock
  majorVersion <- D.readNum sock
  minorVersion <- D.readNum sock
  revision <- fmap fromIntegral $ D.readNum sock
  timeZone <- if hasServerTimeZone clientInfo revision
    then fmap Just $ D.readString sock
    else return Nothing
  patchVersion <- if hasPatchVersion clientInfo revision
    then fmap Just $ D.readString sock
    else return Nothing
  _ <- if not (hasPatchVersion clientInfo revision) && (hasServerDisplayName clientInfo revision)
    then D.readString sock
    else return ""
  return ServerInfo {
    serverName = serverName,
    serverMajorVersion = fromIntegral majorVersion,
    serverMinorVersion = fromIntegral minorVersion,
    serverPatchVersion = patchVersion,
    serverRevision = fromIntegral revision,
    serverTimeZone = timeZone
  }
