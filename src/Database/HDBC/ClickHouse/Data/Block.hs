module Database.HDBC.ClickHouse.Data.Block (encodeBlock, encodeEmptyBlock, Block(..)) where

import Data.Either
import Data.List (transpose)
import Database.HDBC.SqlValue
import Database.HDBC.ClickHouse.Data.Column
import Database.HDBC.ClickHouse.Data.Writer (encodeValue)

import qualified Data.ByteString as B
import qualified Data.ByteString.Char8 as B8
import qualified Database.HDBC.ClickHouse.Codec.Encoder as E

import qualified Database.HDBC.ClickHouse.Protocol.PacketTypes.Client as Client

data Block = Block {
    columns :: [Column],
    rows    :: [[SqlValue]]
}

encodeBlock :: Block -> Either String B.ByteString
encodeBlock block = do
  values <- encode block
  return $ B8.concat $ [
      B.singleton Client.blockOfData,
      E.encodeString "", -- temporary table
      encodeBlockInfo,
      E.encodeNum $ length $ columns block,
      E.encodeNum $ length $ rows block
    ] ++ values

encodeBlockInfo :: B.ByteString
encodeBlockInfo =
  B8.concat [
    B.singleton 1,
    B.singleton 0,
    B.singleton 2,
    E.encodeInt32 (-1),
    B.singleton 0
  ]

encode :: Block -> Either String [B.ByteString]
encode block =
  mapM encode' $ zip (columns block) (transpose $ rows block)
    where encode' (c, vs) = fmap (concat c) $ mapM (\v -> encodeValue c v) vs
          concat c bs = B8.concat $ [E.encodeString (columnName c), E.encodeString (columnTypeName c)] ++ bs

encodeEmptyBlock :: B.ByteString
encodeEmptyBlock = fromRight B.empty $ encodeBlock $ Block { columns = [], rows = [] }
