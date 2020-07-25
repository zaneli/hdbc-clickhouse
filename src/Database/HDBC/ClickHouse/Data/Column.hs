module Database.HDBC.ClickHouse.Data.Column where

data Column = StringColumn {
    columnName :: String
} | Int8Column {
    columnName :: String
} | Int16Column {
    columnName :: String
} | Int32Column {
    columnName :: String
} | Int64Column {
    columnName :: String
} | UInt8Column {
    columnName :: String
} | UInt16Column {
    columnName :: String
} | UInt32Column {
    columnName :: String
} | UInt64Column {
    columnName :: String
} | Float32Column {
    columnName :: String
} | Float64Column {
    columnName :: String
} | DateColumn {
    columnName :: String
} | DateTimeColumn {
    columnName :: String
} | UUIDColumn {
    columnName :: String
} | IPv4Column {
    columnName :: String
} | IPv6Column {
    columnName :: String
} | FixedStringColumn {
    columnName :: String,
    fixedStringSize :: Int
} | ArrayColumn {
    columnName :: String,
    itemType :: Column
} | NullableColumn {
    columnName :: String,
    itemType :: Column
} deriving Show

columnTypeName :: Column -> String
columnTypeName (StringColumn _)           = "String"
columnTypeName (Int8Column _)             = "Int8"
columnTypeName (Int16Column _)            = "Int16"
columnTypeName (Int32Column _)            = "Int32"
columnTypeName (Int64Column _)            = "Int64"
columnTypeName (UInt8Column _)            = "UInt8"
columnTypeName (UInt16Column _)           = "UInt16"
columnTypeName (UInt32Column _)           = "UInt32"
columnTypeName (UInt64Column _)           = "UInt64"
columnTypeName (Float32Column _)          = "Float32"
columnTypeName (Float64Column _)          = "Float64"
columnTypeName (DateColumn _)             = "Date"
columnTypeName (DateTimeColumn _)         = "DateTime"
columnTypeName (UUIDColumn _)             = "UUID"
columnTypeName (IPv4Column _)             = "IPv4"
columnTypeName (IPv6Column _)             = "IPv6"
columnTypeName (FixedStringColumn _ size) = "FixedString(" ++ (show size) ++ ")"
columnTypeName (ArrayColumn _ item)       = "Array(" ++ (columnTypeName item) ++ ")"
columnTypeName (NullableColumn _ item)    = "Nullable(" ++ (columnTypeName item) ++ ")"
