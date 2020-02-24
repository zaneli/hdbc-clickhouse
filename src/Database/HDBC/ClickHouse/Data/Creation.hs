module Database.HDBC.ClickHouse.Data.Creation (createColumn) where

import Data.List (isPrefixOf, isSuffixOf)
import Database.HDBC.ClickHouse.Data

createColumn :: String -> String -> Column
createColumn name "String"   = StringColumn { columnName = name }
createColumn name "Int8"     = Int8Column { columnName = name }
createColumn name "Int16"    = Int16Column { columnName = name }
createColumn name "Int32"    = Int32Column { columnName = name }
createColumn name "Int64"    = Int64Column { columnName = name }
createColumn name "UInt8"    = UInt8Column { columnName = name }
createColumn name "UInt16"   = UInt16Column { columnName = name }
createColumn name "UInt32"   = UInt32Column { columnName = name }
createColumn name "UInt64"   = UInt64Column { columnName = name }
createColumn name "Float32"  = Float32Column { columnName = name }
createColumn name "Float64"  = Float64Column { columnName = name }
createColumn name "Date"     = DateColumn { columnName = name }
createColumn name "DateTime" = DateTimeColumn { columnName = name }
createColumn name "UUID"     = UUIDColumn { columnName = name }
createColumn name "IPv4"     = IPv4Column { columnName = name }
createColumn name "IPv6"     = IPv6Column { columnName = name }
createColumn name typ | isPrefixOf "FixedString(" typ && isSuffixOf ")" typ =
  FixedStringColumn { columnName = name, fixedStringSize = getFixedStringSize typ }
createColumn name typ | isPrefixOf "Array(" typ && isSuffixOf ")" typ =
  ArrayColumn { columnName = name, itemType = getItemType name (length "Array(") typ }
createColumn name typ | isPrefixOf "Nullable(" typ && isSuffixOf ")" typ =
  NullableColumn { columnName = name, itemType = getItemType name (length "Nullable(") typ }

getFixedStringSize :: String -> Int
getFixedStringSize typ =
  read $ (drop (length "FixedString(") . init) typ

getItemType :: String -> Int -> String -> Column
getItemType name prefixSize typ =
  createColumn ("[" ++ name ++ "]") $ (drop prefixSize . init) typ