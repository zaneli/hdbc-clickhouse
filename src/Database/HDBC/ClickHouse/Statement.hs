module Database.HDBC.ClickHouse.Statement where

import Control.Concurrent.MVar
import Control.Exception
import Database.HDBC
import Database.HDBC.Statement
import Database.HDBC.ClickHouse.Exception
import Database.HDBC.ClickHouse.Protocol
import Database.HDBC.ClickHouse.Protocol.Data
import Network.Socket hiding (send, sendTo, recv, recvFrom)

import qualified Database.HDBC.ClickHouse.Protocol.Query as Query

fprepare :: Socket -> ServerInfo -> Config -> String -> IO Statement
fprepare sock serverInfo config sql = do
  mColumns <- newEmptyMVar
  mValues <- newEmptyMVar
  return Statement {
    execute = fexecute sock serverInfo sql,
    executeRaw = fexecuteRaw sock serverInfo sql,
    executeMany = fexecuteMany sock,
    finish = ffinish mColumns mValues,
    fetchRow = ffetchRow sock config mColumns mValues,
    getColumnNames = fgetColumnNames mColumns,
    originalQuery = sql,
    describeResult = fdescribeResult mColumns
  }

fexecute :: Socket -> ServerInfo -> String -> [SqlValue] -> IO Integer
fexecute sock serverInfo sql value = do
  Query.request sock sql serverInfo
  return 1

fexecuteRaw :: Socket -> ServerInfo -> String -> IO ()
fexecuteRaw sock serverInfo sql =
  Query.request sock sql serverInfo

fexecuteMany :: Socket -> [[SqlValue]] -> IO ()
fexecuteMany sock mValues = do
  throwIO $ userError "fexecuteMany not implemented"

ffinish :: MVar [Column] -> MVar [[SqlValue]] -> IO ()
ffinish mColumns mValues = do
  putMVar mColumns []
  putMVar mValues []

ffetchRow :: Socket -> Config -> MVar [Column] -> MVar [[SqlValue]] -> IO (Maybe [SqlValue])
ffetchRow sock config mColumns mValues = do
  Query.response sock config mColumns mValues

fgetColumnNames :: MVar [Column] -> IO [String]
fgetColumnNames mColumns = do
  cs <- tryReadMVar mColumns
  maybe (throwIO $ ClientException "failed to getColumnNames") (return . map columnName) cs

fdescribeResult :: MVar [Column] -> IO [(String, SqlColDesc)]
fdescribeResult mColumns = do
  cs <- tryReadMVar mColumns
  maybe (throwIO $ ClientException "failed to describeResult") (return . map (\c -> (columnName c, getSqlColDesc c))) cs
