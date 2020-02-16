module Database.HDBC.ClickHouse.Protocol.Query (send) where

import Control.Exception
import Control.Monad
import Data.Word
import Network.Socket (Socket)
import Network.Socket.ByteString (sendAll, recv)
import Network.HostName
import Database.HDBC.ClickHouse.Protocol
import Database.HDBC.ClickHouse.Protocol.Data
import Text.Printf

import qualified Data.ByteString as B
import qualified Data.ByteString.Char8 as B8
import qualified Database.HDBC.ClickHouse.Protocol.Codec.Encoder as E
import qualified Database.HDBC.ClickHouse.Protocol.Codec.Decoder as D
import qualified Database.HDBC.ClickHouse.Protocol.PacketTypes.Client as Client
import qualified Database.HDBC.ClickHouse.Protocol.PacketTypes.Compression as Compression
import qualified Database.HDBC.ClickHouse.Protocol.PacketTypes.Server as Server

send :: Socket -> String -> ServerInfo -> IO ()
send sock query serverInfo = do
  host <- getHostName
  request sock host query serverInfo
  response sock
  return ()

request sock host query serverInfo =
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

encodeSettings =
    -- TODO: encode settings
    -- empty string is a marker of the end of the settings
  E.encodeString ""

encodeBlock =
  B8.concat [
    B.singleton Client.blockOfData,
    E.encodeString "", -- temporary table
    encodeBlockInfo,
    B.singleton 0,
    B.singleton 0
  ]

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

response :: Socket -> IO ()
response sock = do
  bs <- recv sock 1
  case (B.unpack bs) of
    [x] | x == Server.blockOfData -> (readBlock sock) >>= (\_ -> response sock)
        | x == Server.profileInfo ->  (readProfileInfo sock) >>= (\_ -> response sock)
        | x == Server.progress ->  (readProgress sock) >>= (\_ -> response sock)
        | x == Server.endOfStream -> return ()
    xs -> throwIO $ userError $ "Unexpected Response: " ++ (show xs)

readBlock sock = do
  D.readString sock

  -- block info
  num1 <- D.readNum sock
  isOverflows <- D.readBool sock
  num2 <- D.readNum sock
  bucketNum <- D.readInt32 sock
  num3 <- D.readNum sock

  numColumns <- D.readNum sock
  numRows <- D.readNum sock

  printf "[Data] bucketNum=%d, numColumns=%d, numRows=%d\n" bucketNum numColumns numRows

  columns <- mapM (\i -> readColumns sock) [1..numColumns]

  return ()

readColumns :: Socket -> IO Column
readColumns sock = do
  columnName <- D.readString sock
  columnType <- D.readString sock
  return Column { columnName = columnName, columnType = columnType }

readProfileInfo :: Socket -> IO ()
readProfileInfo sock = do
  rows <- D.readNum sock
  blocks <- D.readNum sock
  bytes <- D.readNum sock
  appliedLimit <- D.readBool sock
  rowsBeforeLimit <- D.readNum sock
  calculatedRowsBeforeLimit <- D.readBool sock
  printf "[ProfileInfo] rows=%d, blocks=%d, bytes=%d, appliedLimit=%s, rowsBeforeLimit=%d, calculatedRowsBeforeLimit=%s\n"
           rows blocks bytes (show appliedLimit) rowsBeforeLimit (show calculatedRowsBeforeLimit)

readProgress :: Socket -> IO ()
readProgress sock = do
  rows <- D.readNum sock
  bytes <- D.readNum sock
  totalRows <- D.readNum sock
  printf "[Progress] rows=%d, bytes=%d, totalRows=%d\n" rows bytes totalRows
