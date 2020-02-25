module Database.HDBC.ClickHouse.Protocol.Hello (send) where

import Control.Exception
import Data.Word
import Network.Socket (Socket)
import Network.Socket.ByteString (sendAll, recv)
import Database.HDBC.ClickHouse.Exception
import Database.HDBC.ClickHouse.Protocol
import Text.Printf

import qualified Data.ByteString as B
import qualified Data.ByteString.Char8 as B8
import qualified Database.HDBC.ClickHouse.Protocol.Codec.Encoder as E
import qualified Database.HDBC.ClickHouse.Protocol.Codec.Decoder as D
import qualified Database.HDBC.ClickHouse.Protocol.PacketTypes.Client as Client
import qualified Database.HDBC.ClickHouse.Protocol.PacketTypes.Server as Server

send :: Socket -> ClientInfo -> Config -> IO ServerInfo
send sock clientInfo config = do
  request sock clientInfo config
  res <- response sock clientInfo
  if (debug config)
    then printf "[hello] serverName=%s, majorVersion=%d, minorVersion=%d, patchVersion=%s, revision=%d, timeZone=%s\n"
                  (serverName res)
                  (serverMajorVersion res)
                  (serverMinorVersion res)
                  (show $ serverPatchVersion res)
                  (serverRevision res)
                  (show $ serverTimeZone res)
    else return ()
  return res

request :: Socket -> ClientInfo -> Config -> IO ()
request sock clientInfo config =
  sendAll sock $ B8.concat [
      B.singleton Client.hello,
      E.encodeString $ clientName clientInfo,
      E.encodeNum $ clientMajorVersion clientInfo,
      E.encodeNum $ clientMinorVersion clientInfo,
      E.encodeNum $ clientRevision clientInfo,
      E.encodeString $ database config,
      E.encodeString $ username config,
      E.encodeString $ password config
    ]

response :: Socket -> ClientInfo -> IO ServerInfo
response sock clientInfo = do
  bs <- recv sock 1
  case (B.unpack bs) of
    [x] | x == Server.hello     -> return ()
        | x == Server.exception -> (D.readException sock) >>= throwIO
    xs                          -> throwIO $ unexpectedPacketType xs
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
