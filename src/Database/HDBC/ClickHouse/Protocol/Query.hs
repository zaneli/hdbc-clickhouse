module Database.HDBC.ClickHouse.Protocol.Query (receiveAllColumnAndValues, receiveColumnAndValues, receiveColumns, sendBlock, sendEmptyBlock, sendQuery) where

import Control.Concurrent.MVar
import Control.Exception
import Control.Monad
import Data.List (transpose)
import Data.Word
import Database.HDBC.SqlValue
import Network.Socket (Socket)
import Network.Socket.ByteString (sendAll, recv)
import Database.HDBC.ClickHouse.Data
import Database.HDBC.ClickHouse.Data.Block
import Database.HDBC.ClickHouse.Data.Column
import Database.HDBC.ClickHouse.Data.Writer (encodeValue)
import Database.HDBC.ClickHouse.Exception
import Database.HDBC.ClickHouse.Protocol
import Text.Printf

import qualified Data.ByteString as B
import qualified Data.ByteString.Char8 as B8
import qualified Database.HDBC.ClickHouse.Codec.Encoder as E
import qualified Database.HDBC.ClickHouse.Codec.Decoder as D
import qualified Database.HDBC.ClickHouse.Protocol.PacketTypes.Client as Client
import qualified Database.HDBC.ClickHouse.Protocol.PacketTypes.Compression as Compression
import qualified Database.HDBC.ClickHouse.Protocol.PacketTypes.Server as Server

sendQuery :: Socket -> String -> ClientInfo -> ServerInfo -> Config -> IO ()
sendQuery sock query clientInfo serverInfo config =
  request sock query clientInfo serverInfo config

sendBlock sock block =
  sendAll sock $ encodeBlock block

sendEmptyBlock sock =
  sendBlock sock emptyBlock

receiveColumnAndValues :: Socket -> Config -> MVar [Column] -> MVar [[SqlValue]] -> IO (Maybe [SqlValue])
receiveColumnAndValues sock config mColumns mValues = 
  response sock config mColumns mValues

receiveColumns :: Socket -> Config -> IO (Maybe [Column])
receiveColumns sock config = do
  mColumns <- newEmptyMVar
  mValues <- newEmptyMVar
  f mColumns mValues
  where
    f mColumns mValues = do
      bs <- recv sock 1
      case (B.unpack bs) of
        [x] | x == Server.blockOfData -> (readBlock sock config mColumns mValues) >>= (\_ -> tryTakeMVar mColumns)
            | x == Server.profileInfo -> (readProfileInfo sock config) >>= (\_ -> f mColumns mValues)
            | x == Server.progress    -> (readProgress sock config) >>= (\_ -> f mColumns mValues)
            | x == Server.exception   -> (D.readException sock) >>= throwIO
            | x == Server.endOfStream -> return Nothing
        xs -> throwIO $ unexpectedPacketType xs

receiveAllColumnAndValues :: Socket -> Config -> IO ([Column], [[SqlValue]])
receiveAllColumnAndValues sock config = do
  mColumns <- newEmptyMVar
  mValues <- newEmptyMVar
  values <- fetchAllRows mColumns mValues
  columns <- takeMVar mColumns
  return $ (columns, values)
    where
      fetchAllRows mColumns mValues = do
        row <- response sock config mColumns mValues
        case row of
          Nothing -> return []
          Just x  -> do remainder <- fetchAllRows mColumns mValues
                        return (x : remainder)

request :: Socket -> String -> ClientInfo -> ServerInfo -> Config -> IO ()
request sock query clientInfo serverInfo config = do
  if (debug config)
    then printf "[Query] query=\"%s\"\n" query
    else return ()
  sendAll sock $ B8.concat [
      B.singleton Client.query,
      E.encodeString "",
      B.singleton 1,
      E.encodeString "",
      E.encodeString "",
      E.encodeString "[::ffff:127.0.0.1]:0",
      E.encodeNum ifaceTypeTCP,
      E.encodeString $ clientHost clientInfo,
      E.encodeString $ clientHost clientInfo,
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
      encodeBlock emptyBlock
    ]

encodeSettings :: B.ByteString
encodeSettings =
    -- TODO: encode settings
    -- empty string is a marker of the end of the settings
  E.encodeString ""

encodeBlock :: Block -> B.ByteString
encodeBlock block =
  B8.concat $ [
      B.singleton Client.blockOfData,
      E.encodeString "", -- temporary table
      encodeBlockInfo,
      E.encodeNum $ length $ columns block,
      E.encodeNum $ length $ rows block
    ] ++
      (map (\(c, vs) -> B8.concat ([E.encodeString (columnName c), E.encodeString (columnTypeName c)] ++ (map (\v -> encodeValue c v) vs))) $ zip (columns block) $ transpose (rows block))

encodeBlockInfo :: B.ByteString
encodeBlockInfo =
  B8.concat [
    B.singleton 1,
    B.singleton 0,
    B.singleton 2,
    E.encodeInt32 (-1),
    B.singleton 0
  ]

stateComplete :: Word8
stateComplete = 2

ifaceTypeTCP :: Word8
ifaceTypeTCP = 1

emptyBlock :: Block
emptyBlock = Block { columns = [], rows = [] }

response :: Socket -> Config -> MVar [Column] -> MVar [[SqlValue]] -> IO (Maybe [SqlValue])
response sock config mColumns mValues = do
  values <- tryTakeMVar mValues
  case values of
    Just (v:vs) -> do
      putMVar mValues vs
      return $ Just v
    _ -> do
      bs <- recv sock 1
      case (B.unpack bs) of
        [x] | x == Server.blockOfData -> (readBlock sock config mColumns mValues) >>= (\_ -> response sock config mColumns mValues)
            | x == Server.profileInfo -> (readProfileInfo sock config) >>= (\_ -> response sock config mColumns mValues)
            | x == Server.progress    -> (readProgress sock config) >>= (\_ -> response sock config mColumns mValues)
            | x == Server.exception   -> (D.readException sock) >>= throwIO
            | x == Server.endOfStream -> return Nothing
        xs -> throwIO $ unexpectedPacketType xs

readBlock :: Socket -> Config -> MVar [Column] -> MVar [[SqlValue]] -> IO ()
readBlock sock config mColumns mValues = do
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
  isEmptyColumns <- isEmptyMVar mColumns
  if (isEmptyColumns && (not $ null columns))
    then do
      tryTakeMVar mColumns
      putMVar mColumns columns
    else
      return ()
  putMVar mValues $ transpose values

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
