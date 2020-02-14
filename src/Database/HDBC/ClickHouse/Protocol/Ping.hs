module Database.HDBC.ClickHouse.Protocol.Ping (send) where

import Control.Exception
import Data.Word
import Network.Socket (Socket)
import Network.Socket.ByteString (sendAll)

import qualified Data.ByteString as B
import qualified Database.HDBC.ClickHouse.Protocol.Decoder as D

send :: Socket -> IO String
send sock = do
  sendAll sock $ request
  response sock

request :: B.ByteString
request = B.singleton requestType

response :: Socket -> IO String
response sock = do
  bs <- D.readAll sock
  case (B.unpack bs) of
    [responseType] -> return "pong"
    xs             -> throwIO $ userError $ "Unexpected Response: " ++ (show xs)

requestType :: Word8
requestType = 4

responseType :: Word8
responseType = 4
