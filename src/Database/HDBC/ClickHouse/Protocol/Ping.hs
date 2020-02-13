module Database.HDBC.ClickHouse.Protocol.Ping (request, response) where

import Data.Word

import qualified Data.ByteString as B

request = B.singleton requestType

response bs =
  case (B.unpack bs) of
    [requestType] -> "pong"
    otherwise     -> "" -- TODO: error handling

requestType :: Word8
requestType = 4
