module Database.HDBC.ClickHouse.Exception where

import Control.Exception
import Data.Word

import qualified Data.ByteString as B

data ClickHouseException = ServerException {
  code :: Int,
  name :: String,
  message :: String,
  stackTrace :: String,
  nested :: Maybe ClickHouseException
} | ClientException {
  message :: String
} | UnexpectedPacketTypeException {
  packets :: [Word8]
} | UnexpectedResponseException {
  dataType :: String,
  packets :: [Word8]
}

instance Show ClickHouseException where
  show (ServerException code name message _ _) =
          "ServerException {code = " ++ (show code)
            ++ ", name = \"" ++ name
            ++ "\", message = \"" ++ message ++ "\"}"
  show (ClientException message) =
          "ClientException {message = \"" ++ message ++ "\"}"
  show (UnexpectedPacketTypeException packets) =
          "UnexpectedPacketTypeException {message = \"Unexpected Packet Type: " ++ (show packets) ++ "\"}"
  show (UnexpectedResponseException dataType packets) =
          "UnexpectedResponseException {message = \"Unexpected " ++ dataType ++ " Response: " ++ (show packets) ++ "\"}"

instance Exception ClickHouseException

unexpectedPacketType :: [Word8] -> ClickHouseException
unexpectedPacketType packets =
  UnexpectedPacketTypeException { packets = packets }

unexpectedResponse :: String -> [Word8] -> ClickHouseException
unexpectedResponse dataType packets =
  UnexpectedResponseException { dataType = dataType, packets = packets }
