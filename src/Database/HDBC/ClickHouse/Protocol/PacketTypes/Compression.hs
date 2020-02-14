module Database.HDBC.ClickHouse.Protocol.PacketTypes.Compression where

import Data.Word

disable :: Word8
disable = 0

enable :: Word8
enable = 1
