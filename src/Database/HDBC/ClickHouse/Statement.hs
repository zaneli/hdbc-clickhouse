module Database.HDBC.ClickHouse.Statement where

import Control.Concurrent.MVar
import Control.Exception
import Database.HDBC
import Database.HDBC.Statement
import Database.HDBC.ClickHouse.Protocol
import Network.Socket hiding (send, sendTo, recv, recvFrom)

import qualified Database.HDBC.ClickHouse.Protocol.Query as Query

fprepare :: Socket -> ServerInfo -> Config -> String -> IO Statement
fprepare sock serverInfo config sql = do
  buffer <- newEmptyMVar
  return Statement {
    execute = fexecute sock serverInfo sql,
    executeRaw = fexecuteRaw sock,
    executeMany = fexecuteMany sock,
    finish = ffinish sock,
    fetchRow = ffetchRow sock config buffer,
    getColumnNames = fgetColumnNames sock,
    originalQuery = sql,
    describeResult = fdescribeResult sock
  }

fexecute :: Socket -> ServerInfo -> String -> [SqlValue] -> IO Integer
fexecute sock serverInfo sql value = do
  Query.request sock sql serverInfo
  return 1

fexecuteRaw :: Socket -> IO ()
fexecuteRaw sock = do
  throwIO $ userError "fexecuteRaw not implemented"

fexecuteMany :: Socket -> [[SqlValue]] -> IO ()
fexecuteMany sock values = do
  throwIO $ userError "fexecuteMany not implemented"

ffinish :: Socket -> IO ()
ffinish sock = do
  throwIO $ userError "ffinish not implemented"

ffetchRow :: Socket -> Config -> MVar [[SqlValue]] -> IO (Maybe [SqlValue])
ffetchRow sock config buffer = do
  Query.response sock config buffer

fgetColumnNames :: Socket -> IO [String]
fgetColumnNames sock = do
  throwIO $ userError "fgetColumnNames not implemented"

fdescribeResult :: Socket -> IO [(String, SqlColDesc)]
fdescribeResult sock = do
  throwIO $ userError "fdescribeResult not implemented"
