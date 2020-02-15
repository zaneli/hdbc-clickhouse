module Database.HDBC.ClickHouse.Protocol.Query (send) where

import Data.Word
import Network.Socket (Socket)
import Network.Socket.ByteString (sendAll, recv)
import Network.HostName
import Database.HDBC.ClickHouse.Protocol

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
  res <- D.readAll sock
  print res
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
