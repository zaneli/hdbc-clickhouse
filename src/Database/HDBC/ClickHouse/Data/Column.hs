module Database.HDBC.ClickHouse.Data.Column where

import Data.IP (toIPv4, toIPv6)
import Data.Time.Clock.POSIX (posixSecondsToUTCTime)
import Database.HDBC.SqlValue

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

nullValue :: Column -> SqlValue
nullValue (StringColumn _)           = SqlString ""
nullValue (Int8Column _)             = SqlInt32 0
nullValue (Int16Column _)            = SqlInt32 0
nullValue (Int32Column _)            = SqlInt32 0
nullValue (Int64Column _)            = SqlInt64 0
nullValue (UInt8Column _)            = SqlWord32 0
nullValue (UInt16Column _)           = SqlWord32 0
nullValue (UInt32Column _)           = SqlWord32 0
nullValue (UInt64Column _)           = SqlWord64 0
nullValue (Float32Column _)          = SqlDouble 0
nullValue (Float64Column _)          = SqlDouble 0
nullValue (DateColumn _)             = SqlUTCTime $ posixSecondsToUTCTime 0
nullValue (DateTimeColumn _)         = SqlUTCTime $ posixSecondsToUTCTime 0
nullValue (UUIDColumn _)             = SqlString "00000000-0000-0000-0000-000000000000"
nullValue (IPv4Column _)             = SqlString $ show $ toIPv4 [0, 0, 0, 0]
nullValue (IPv6Column _)             = SqlString $ show $ toIPv6 [0, 0, 0, 0, 0, 0, 0, 0]
nullValue (FixedStringColumn _ size) = SqlString ""
