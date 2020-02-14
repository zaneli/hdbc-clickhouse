module Database.HDBC.ClickHouse.Protocol.PacketTypes.Client where

import Data.Word

hello :: Word8
hello = 0

query :: Word8
query = 1

blockOfData :: Word8
blockOfData = 2

cancel :: Word8
cancel = 3

ping :: Word8
ping = 4

tablesStatusRequest :: Word8
tablesStatusRequest = 5

keepAlive :: Word8
keepAlive = 6

scalar :: Word8
scalar = 7
