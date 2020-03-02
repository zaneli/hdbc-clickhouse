module Database.HDBC.ClickHouse.Statement where

import Control.Concurrent.MVar
import Control.Exception
import Database.HDBC
import Database.HDBC.Statement
import Database.HDBC.ClickHouse.Data
import Database.HDBC.ClickHouse.Exception
import Database.HDBC.ClickHouse.Protocol
import Network.Socket hiding (send, sendTo, recv, recvFrom)

import qualified Database.HDBC.ClickHouse.Protocol.Query as Query

fprepare :: Socket -> ClientInfo -> ServerInfo -> Config -> String -> IO Statement
fprepare sock clientInfo serverInfo config sql = do
  mColumns <- newEmptyMVar
  mValues <- newEmptyMVar
  return Statement {
    execute = fexecute sock clientInfo serverInfo config sql,
    executeRaw = fexecuteRaw sock clientInfo serverInfo config sql,
    executeMany = fexecuteMany sock clientInfo serverInfo config sql,
    finish = ffinish mColumns mValues,
    fetchRow = ffetchRow sock config mColumns mValues,
    getColumnNames = fgetColumnNames mColumns,
    originalQuery = sql,
    describeResult = fdescribeResult mColumns
  }

fexecute :: Socket -> ClientInfo -> ServerInfo -> Config -> String -> [SqlValue] -> IO Integer
fexecute sock clientInfo serverInfo config sql value = do
  q <- buildQuery sql value
  Query.request sock q clientInfo serverInfo config
  return 1

fexecuteRaw :: Socket -> ClientInfo -> ServerInfo -> Config -> String -> IO ()
fexecuteRaw sock clientInfo serverInfo config sql = do
  fexecute sock clientInfo serverInfo config sql []
  return ()

fexecuteMany :: Socket -> ClientInfo -> ServerInfo -> Config -> String -> [[SqlValue]] -> IO ()
fexecuteMany sock clientInfo serverInfo config sql values = do
  mapM_ (fexecute sock clientInfo serverInfo config sql) values

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

buildQuery :: String -> [SqlValue] -> IO String
buildQuery sql values = do
  let (q, isInsideOfText, vs) = (foldl append ("", False, values) sql)
  if isInsideOfText
    then (throwIO $ ClientException "invalid SQL. broken `'` quote.")
    else return ()
  if (not $ null vs)
    then (throwIO $ ClientException "invalid SQL. SqlValues do not match `?` placeholders.")
    else return ()
  return $ reverse q
    where
      append (xs, False,          v:vs) '?'  = ((reverse (quote v)) ++ xs, False,              vs)
      append (xs, isInsideOfText, vs  ) '\'' = ('\'':xs,                   not isInsideOfText, vs)
      append (xs, isInsideOfText, vs  )  x   = (x:xs,                      isInsideOfText,     vs)
      -- TODO: Handle all SqlValue properly
      quote (SqlString s) = "'" ++ s ++ "'"
      quote v             = fromSql v
