module Database.HDBC.ClickHouse.Data.ColDesc (getSqlColDesc) where

import Database.HDBC.ColTypes
import Database.HDBC.ClickHouse.Data.Column

getSqlColDesc :: Column -> SqlColDesc
getSqlColDesc (StringColumn _) = SqlColDesc {
  colType = SqlVarCharT,
  colSize = Nothing,
  colOctetLength = Nothing,
  colDecDigits = Nothing,
  colNullable = Just False
}
getSqlColDesc (Int8Column _) = SqlColDesc {
  colType = SqlTinyIntT,
  colSize = Nothing,
  colOctetLength = Nothing,
  colDecDigits = Nothing,
  colNullable = Just False
}
getSqlColDesc (Int16Column _) = SqlColDesc {
  colType = SqlSmallIntT,
  colSize = Nothing,
  colOctetLength = Nothing,
  colDecDigits = Nothing,
  colNullable = Just False
}
getSqlColDesc (Int32Column _) = SqlColDesc {
  colType = SqlIntegerT,
  colSize = Nothing,
  colOctetLength = Nothing,
  colDecDigits = Nothing,
  colNullable = Just False
}
getSqlColDesc (Int64Column _) = SqlColDesc {
  colType = SqlBigIntT,
  colSize = Nothing,
  colOctetLength = Nothing,
  colDecDigits = Nothing,
  colNullable = Just False
}
getSqlColDesc (UInt8Column _) = SqlColDesc {
  colType = SqlTinyIntT,
  colSize = Nothing,
  colOctetLength = Nothing,
  colDecDigits = Nothing,
  colNullable = Just False
}
getSqlColDesc (UInt16Column _) = SqlColDesc {
  colType = SqlSmallIntT,
  colSize = Nothing,
  colOctetLength = Nothing,
  colDecDigits = Nothing,
  colNullable = Just False
}
getSqlColDesc (UInt32Column _) = SqlColDesc {
  colType = SqlIntegerT,
  colSize = Nothing,
  colOctetLength = Nothing,
  colDecDigits = Nothing,
  colNullable = Just False
}
getSqlColDesc (UInt64Column _) = SqlColDesc {
  colType = SqlBigIntT,
  colSize = Nothing,
  colOctetLength = Nothing,
  colDecDigits = Nothing,
  colNullable = Just False
}
getSqlColDesc (Float32Column _) = SqlColDesc {
  colType = SqlFloatT,
  colSize = Nothing,
  colOctetLength = Nothing,
  colDecDigits = Nothing,
  colNullable = Just False
}
getSqlColDesc (Float64Column _) = SqlColDesc {
  colType = SqlDoubleT,
  colSize = Nothing,
  colOctetLength = Nothing,
  colDecDigits = Nothing,
  colNullable = Just False
}
getSqlColDesc (DateColumn _) = SqlColDesc {
  colType = SqlDateT,
  colSize = Nothing,
  colOctetLength = Nothing,
  colDecDigits = Nothing,
  colNullable = Just False
}
getSqlColDesc (DateTimeColumn _) = SqlColDesc {
  colType = SqlTimeT,
  colSize = Nothing,
  colOctetLength = Nothing,
  colDecDigits = Nothing,
  colNullable = Just False
}
getSqlColDesc (UUIDColumn _) = SqlColDesc {
  colType = SqlCharT,
  colSize = Nothing,
  colOctetLength = Nothing,
  colDecDigits = Nothing,
  colNullable = Just False
}
getSqlColDesc (IPv4Column _) = SqlColDesc {
  colType = SqlCharT,
  colSize = Nothing,
  colOctetLength = Nothing,
  colDecDigits = Nothing,
  colNullable = Just False
}
getSqlColDesc (IPv6Column _) = SqlColDesc {
  colType = SqlCharT,
  colSize = Nothing,
  colOctetLength = Nothing,
  colDecDigits = Nothing,
  colNullable = Just False
}
getSqlColDesc (FixedStringColumn _ size) = SqlColDesc {
  colType = SqlCharT,
  colSize = Just size,
  colOctetLength = Nothing,
  colDecDigits = Nothing,
  colNullable = Just False
}
getSqlColDesc (ArrayColumn _ _) = SqlColDesc {
  colType = SqlUnknownT "Array",
  colSize = Nothing,
  colOctetLength = Nothing,
  colDecDigits = Nothing,
  colNullable = Just False
}
getSqlColDesc (NullableColumn _ itemType) = SqlColDesc {
  colType = colType itemSqlColDesc,
  colSize = colSize itemSqlColDesc,
  colOctetLength = Nothing,
  colDecDigits = Nothing,
  colNullable = Just True
}
  where itemSqlColDesc = getSqlColDesc itemType
