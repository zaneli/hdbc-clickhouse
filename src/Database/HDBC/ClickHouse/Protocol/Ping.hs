module Database.HDBC.ClickHouse.Protocol.Ping (send) where

import Control.Exception
import Data.Word
import Network.Socket (Socket)
import Network.Socket.ByteString (sendAll)

import qualified Data.ByteString as B
import qualified Database.HDBC.ClickHouse.Protocol.Codec.Decoder as D
import qualified Database.HDBC.ClickHouse.Protocol.PacketTypes.Client as Client
import qualified Database.HDBC.ClickHouse.Protocol.PacketTypes.Server as Server

send :: Socket -> IO String
send sock = do
  request sock
  response sock

request :: Socket -> IO ()
request sock =
  sendAll sock $ B.singleton Client.ping

response :: Socket -> IO String
response sock = do
  bs <- D.readAll sock
  case (B.unpack bs) of
    [x] | x == Server.pong -> return "pong"
    xs                     -> throwIO $ userError $ "Unexpected Response: " ++ (show xs)
