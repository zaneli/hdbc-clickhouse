module Database.HDBC.ClickHouse.ConnectionImpl (Connection(..)) where

import Control.Monad

import qualified Database.HDBC.Types as Types
import qualified Database.HDBC.ColTypes as ColTypes

data Connection =
  Connection {
    disconnect :: IO (),
    commit :: IO (),
    rollback :: IO (),
    run :: String -> [Types.SqlValue] -> IO Integer,
    runRaw :: String -> IO (),
    prepare :: String -> IO Types.Statement,
    clone :: IO Connection,
    hdbcDriverName :: String,
    hdbcClientVer :: String,
    proxiedClientName :: String,
    proxiedClientVer :: String,
    dbServerVer :: String,
    dbTransactionSupport :: Bool,
    getTables :: IO [String],
    describeTable :: String -> IO [(String, ColTypes.SqlColDesc)],
    ping :: IO String
  }

instance Types.IConnection Connection where
  disconnect = disconnect
  commit = commit
  rollback = rollback
  run = run
  runRaw = runRaw
  prepare = prepare
  clone = clone
  hdbcDriverName = hdbcDriverName
  hdbcClientVer = hdbcClientVer
  proxiedClientName = proxiedClientName
  proxiedClientVer = proxiedClientVer
  dbServerVer = dbServerVer
  dbTransactionSupport = dbTransactionSupport
  getTables = getTables
  describeTable = describeTable
