module Database.HDBC.ClickHouse.Protocol.Ping (request, response) where

import Control.Exception
import Data.Word
import Network.Socket (Socket)

import qualified Data.ByteString as B
import qualified Database.HDBC.ClickHouse.Protocol.Decoder as D

request :: B.ByteString
request = B.singleton requestType

response :: Socket -> IO String
response sock = do
  bs <- D.readAll sock
  case (B.unpack bs) of
    [responseType] -> return "pong"
    x             -> throwIO $ userError $ "Unexpected Response: " ++ (show x)

requestType :: Word8
requestType = 4

responseType :: Word8
responseType = 4
