module Database.HDBC.ClickHouse.Protocol.Data where

data Column = Column {
    columnName :: String,
    columnType :: String
} deriving Show
