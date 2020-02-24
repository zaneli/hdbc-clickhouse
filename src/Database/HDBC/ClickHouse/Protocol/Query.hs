module Database.HDBC.ClickHouse.Protocol.Query (send, request, response) where

import Control.Concurrent.MVar
import Control.Exception
import Control.Monad
import Data.List (transpose)
import Data.Word
import Database.HDBC.SqlValue
import Network.Socket (Socket)
import Network.Socket.ByteString (sendAll, recv)
import Network.HostName
import Database.HDBC.ClickHouse.Data
import Database.HDBC.ClickHouse.Data.Creation
import Database.HDBC.ClickHouse.Data.Reader
import Database.HDBC.ClickHouse.Exception
import Database.HDBC.ClickHouse.Protocol
import Text.Printf

import qualified Data.ByteString as B
import qualified Data.ByteString.Char8 as B8
import qualified Database.HDBC.ClickHouse.Protocol.Codec.Encoder as E
import qualified Database.HDBC.ClickHouse.Protocol.Codec.Decoder as D
import qualified Database.HDBC.ClickHouse.Protocol.PacketTypes.Client as Client
import qualified Database.HDBC.ClickHouse.Protocol.PacketTypes.Compression as Compression
import qualified Database.HDBC.ClickHouse.Protocol.PacketTypes.Server as Server

send :: Socket -> String -> ServerInfo -> Config -> IO [(Column, [SqlValue])]
send sock query serverInfo config = do
  request sock query serverInfo
  response' sock config

request sock query serverInfo = do
  host <- getHostName
  request' sock host query serverInfo

request' :: Socket -> String -> String -> ServerInfo -> IO ()
request' sock host query serverInfo =
  sendAll sock $ B8.concat [
      B.singleton Client.query,
      E.encodeString "",
      B.singleton 1,
      E.encodeString "",
      E.encodeString "",
      E.encodeString "[::ffff:127.0.0.1]:0",
      E.encodeNum ifaceTypeTCP,
      E.encodeString host,
      E.encodeString host,
      E.encodeString $ clientName clientInfo,
      E.encodeNum $ clientMajorVersion clientInfo,
      E.encodeNum $ clientMinorVersion clientInfo,
      E.encodeNum $ clientRevision clientInfo,
      if hasQuotaKeyInClientInfo serverInfo
        then (E.encodeString "")
        else B.empty,
      encodeSettings,
      B.singleton stateComplete,
      B.singleton Compression.disable,
      E.encodeString query,
      encodeBlock
    ]

encodeSettings :: B.ByteString
encodeSettings =
    -- TODO: encode settings
    -- empty string is a marker of the end of the settings
  E.encodeString ""

encodeBlock :: B.ByteString
encodeBlock =
  B8.concat [
    B.singleton Client.blockOfData,
    E.encodeString "", -- temporary table
    encodeBlockInfo,
    B.singleton 0,
    B.singleton 0
  ]

encodeBlockInfo :: B.ByteString
encodeBlockInfo =
  B8.concat [
    B.singleton 1,
    B.singleton 0,
    B.singleton 2,
    E.encodeWord32 4294967295, -- -1 to Word32
    B.singleton 0
  ]

stateComplete :: Word8
stateComplete = 2

ifaceTypeTCP :: Word8
ifaceTypeTCP = 1

response :: Socket -> Config -> MVar [Column] -> MVar [[SqlValue]] -> IO (Maybe [SqlValue])
response sock config mColumns mValues = do
  values <- tryTakeMVar mValues
  case values of
    Just (v:vs) -> do
      putMVar mValues vs
      return $ Just v
    _ -> do
      tryTakeMVar mColumns
      bs <- recv sock 1
      case (B.unpack bs) of
        [x] | x == Server.blockOfData -> readFromSocket sock config mColumns mValues
            | x == Server.profileInfo ->  (readProfileInfo sock config) >>= (\_ -> response sock config mColumns mValues)
            | x == Server.progress    ->  (readProgress sock config) >>= (\_ -> response sock config mColumns mValues)
            | x == Server.exception   -> (D.readException sock) >>= throwIO
            | x == Server.endOfStream -> return Nothing
        xs -> throwIO $ unexpectedPacketType xs

response' :: Socket -> Config -> IO [(Column, [SqlValue])]
response' sock config = fetch sock config []
  where
    fetch sock config values = do
      bs <- recv sock 1
      case (B.unpack bs) of
        [x] | x == Server.blockOfData -> (readBlock sock config) >>= (\vs -> fetch sock config $ values ++ vs)
            | x == Server.profileInfo ->  (readProfileInfo sock config) >>= (\_ -> fetch sock config values)
            | x == Server.progress    ->  (readProgress sock config) >>= (\_ -> fetch sock config values)
            | x == Server.exception   -> (D.readException sock) >>= throwIO
            | x == Server.endOfStream -> return values
        xs -> throwIO $ unexpectedPacketType xs

readFromSocket :: Socket -> Config -> MVar [Column] -> MVar [[SqlValue]] -> IO (Maybe [SqlValue])
readFromSocket sock config mColumns mValues = do
  D.readString sock

  -- block info
  num1 <- D.readNum sock
  isOverflows <- D.readBool sock
  num2 <- D.readNum sock
  bucketNum <- D.readInt32 sock
  num3 <- D.readNum sock

  numColumns <- D.readNum sock
  numRows <- D.readNum sock

  if (debug config)
    then printf "[Data] bucketNum=%d, numColumns=%d, numRows=%d\n" bucketNum numColumns numRows
    else return ()

  (columns, values) <- fmap unzip $ mapM (\i -> readColumn sock config $ fromIntegral numRows) [1..numColumns]
  putMVar mColumns columns
  putMVar mValues $ transpose values
  response sock config mColumns mValues

readBlock :: Socket -> Config -> IO [(Column, [SqlValue])]
readBlock sock config = do
  D.readString sock

  -- block info
  num1 <- D.readNum sock
  isOverflows <- D.readBool sock
  num2 <- D.readNum sock
  bucketNum <- D.readInt32 sock
  num3 <- D.readNum sock

  numColumns <- D.readNum sock
  numRows <- D.readNum sock

  if (debug config)
    then printf "[Data] bucketNum=%d, numColumns=%d, numRows=%d\n" bucketNum numColumns numRows
    else return ()

  columns <- mapM (\i -> readColumn sock config $ fromIntegral numRows) [1..numColumns]
  return columns

readColumn :: Socket -> Config -> Int -> IO (Column, [SqlValue])
readColumn sock config numRows = do
  columnName <- D.readString sock
  columnType <- D.readString sock
  let column = createColumn columnName columnType
  values <- readValue sock column config numRows
  return (column, values)

readProfileInfo :: Socket -> Config -> IO ()
readProfileInfo sock config = do
  rows <- D.readNum sock
  blocks <- D.readNum sock
  bytes <- D.readNum sock
  appliedLimit <- D.readBool sock
  rowsBeforeLimit <- D.readNum sock
  calculatedRowsBeforeLimit <- D.readBool sock
  if (debug config)
    then printf "[ProfileInfo] rows=%d, blocks=%d, bytes=%d, appliedLimit=%s, rowsBeforeLimit=%d, calculatedRowsBeforeLimit=%s\n"
                  rows blocks bytes (show appliedLimit) rowsBeforeLimit (show calculatedRowsBeforeLimit)
    else return ()

readProgress :: Socket -> Config -> IO ()
readProgress sock config = do
  rows <- D.readNum sock
  bytes <- D.readNum sock
  totalRows <- D.readNum sock
  if (debug config)
    then printf "[Progress] rows=%d, bytes=%d, totalRows=%d\n" rows bytes totalRows
    else return ()
