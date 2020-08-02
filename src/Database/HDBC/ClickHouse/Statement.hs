module Database.HDBC.ClickHouse.Statement where

import Control.Concurrent.MVar
import Control.Exception
import Data.Char (toUpper)
import Data.List (findIndex, intercalate)
import Database.HDBC
import Database.HDBC.Statement
import Database.HDBC.ClickHouse.Data
import Database.HDBC.ClickHouse.Data.Block
import Database.HDBC.ClickHouse.Exception
import Database.HDBC.ClickHouse.Protocol
import Network.Socket hiding (send, sendTo, recv, recvFrom)

import qualified Database.HDBC.ClickHouse.Protocol.Query as Query

fprepare :: Socket -> ClientInfo -> ServerInfo -> Config -> String -> IO Statement
fprepare sock clientInfo serverInfo config sql = do
  let isInsert = (map (map toUpper) $ take 2 $ words sql) == ["INSERT", "INTO"]
  mColumns <- newEmptyMVar
  mValues <- newEmptyMVar
  return Statement {
    execute = fexecute sock clientInfo serverInfo config sql isInsert,
    executeRaw = fexecuteRaw sock clientInfo serverInfo config sql isInsert,
    executeMany = fexecuteMany sock clientInfo serverInfo config sql isInsert,
    finish = ffinish mColumns mValues,
    fetchRow = ffetchRow sock config isInsert mColumns mValues,
    getColumnNames = fgetColumnNames mColumns,
    originalQuery = sql,
    describeResult = fdescribeResult mColumns
  }

fexecute :: Socket -> ClientInfo -> ServerInfo -> Config -> String -> Bool -> [SqlValue] -> IO Integer
fexecute sock clientInfo serverInfo config sql isInsert value =
  if isInsert then do
    insert sock clientInfo serverInfo config sql [value]
    return 1
  else do
    query <- buildQuery sql value
    Query.sendQuery sock query clientInfo serverInfo config
    return 0

fexecuteRaw :: Socket -> ClientInfo -> ServerInfo -> Config -> String -> Bool -> IO ()
fexecuteRaw sock clientInfo serverInfo config sql isInsert = do
  fexecute sock clientInfo serverInfo config sql isInsert []
  return ()

fexecuteMany :: Socket -> ClientInfo -> ServerInfo -> Config -> String -> Bool -> [[SqlValue]] -> IO ()
fexecuteMany sock clientInfo serverInfo config sql isInsert values = do
  if isInsert then
    fmap (\_ -> ()) $ insert sock clientInfo serverInfo config sql values
  else
    mapM_ (fexecute sock clientInfo serverInfo config sql isInsert) values

ffinish :: MVar [Column] -> MVar [[SqlValue]] -> IO ()
ffinish mColumns mValues = do
  putMVar mColumns []
  putMVar mValues []

ffetchRow :: Socket -> Config -> Bool -> MVar [Column] -> MVar [[SqlValue]] -> IO (Maybe [SqlValue])
ffetchRow sock config isInsert mColumns mValues = do
  if isInsert then
    fmap (\_ -> Nothing) (ffinish mColumns mValues)
  else
    Query.receiveColumnAndValues sock config mColumns mValues

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

insert sock clientInfo serverInfo config sql values = do
  let sqls = words sql
  let index = findIndex (\str -> (map toUpper str) == "VALUES") sqls
  sqlWithoutData <- case index of
    Just(idx) -> return $ (\i -> intercalate " " $ take (i + 1) sqls) idx
    Nothing -> throwIO $ ClientException "invalid SQL. not contains `VALUES` word."

  Query.sendQuery sock sqlWithoutData clientInfo serverInfo config
  columns <- Query.receiveColumns sock config
  case columns of
    Just(cs) -> do
      Query.sendBlock sock (Block { columns = cs, rows = values }) config
      Query.sendEmptyBlock sock config

      mColumns <- newEmptyMVar
      mValues <- newEmptyMVar
      Query.receiveColumnAndValues sock config mColumns mValues
    Nothing -> throwIO $ ClientException "failed to getColumns"
