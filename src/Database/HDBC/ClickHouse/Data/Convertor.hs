{-# LANGUAGE MultiParamTypeClasses #-}
module Database.HDBC.ClickHouse.Data.Convertor where

import Data.Convertible.Base
import Data.IP
import Database.HDBC.SqlValue

instance Convertible IPv4 SqlValue where
  safeConvert ipv4 = return $ toSql ""

instance Convertible IPv6 SqlValue where
  safeConvert ipv6 = return $ toSql ""
