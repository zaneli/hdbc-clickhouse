module Database.HDBC.ClickHouse.Protocol.PacketTypes.Server where

import Data.Word

hello :: Word8
hello = 0

blockOfData :: Word8
blockOfData = 1

exception :: Word8
exception = 2

progress :: Word8
progress = 3

pong :: Word8
pong = 4

endOfStream :: Word8
endOfStream = 5

profileInfo :: Word8
profileInfo = 6

totals :: Word8
totals = 7

extremes :: Word8
extremes = 8

tablesStatusResponse :: Word8
tablesStatusResponse = 9

log :: Word8
log = 10

tableColumns :: Word8
tableColumns = 11
