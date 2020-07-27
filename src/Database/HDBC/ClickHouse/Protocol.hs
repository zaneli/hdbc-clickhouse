module Database.HDBC.ClickHouse.Protocol where

import Data.Either
import Data.List (intercalate, unfoldr)
import Database.HDBC.SqlValue

data Config = Config {
  host :: String,
  port :: Int,
  database :: String,
  username :: String,
  password :: String,
  debug :: Bool,
  joinSqlValues :: [SqlValue] -> SqlValue,
  splitSqlValue :: SqlValue -> Either String [SqlValue]
}

defaultJoinSqlValues :: [SqlValue] -> SqlValue
defaultJoinSqlValues = toSql . (\s -> "[" ++ s ++ "]") . intercalate "," . map fromSql

defaultSplitSqlValue :: SqlValue -> Either String [SqlValue]
defaultSplitSqlValue v
  | (length s >= 2) && (head s == '[') && (last s == ']') = Right $ map toSql $ split
  where
    s = fromSql v
    split = foldr f [] $ init $ tail s
      where
        f ',' xs   = [] : xs
        f c (x:xs) = (c:x):xs
        f c []     = [[c]]
defaultSplitSqlValue v = Left $ show v ++ " is not array format."

data ClientInfo = ClientInfo {
  clientName :: String,
  clientMajorVersion :: Int,
  clientMinorVersion :: Int,
  clientRevision :: Int,
  clientHost :: String
} deriving Show

createClientInfo :: String -> ClientInfo
createClientInfo host = ClientInfo {
  clientName = "hdbc-clickhouse",
  clientMajorVersion = 1,
  clientMinorVersion = 1,
  clientRevision = 54213,
  clientHost = host
}

clientVersion :: ClientInfo -> String
clientVersion clientInfo =
  (show $ clientMajorVersion clientInfo) ++ "." ++
    (show $ clientMinorVersion clientInfo) ++ "." ++
    (show $ clientRevision clientInfo)

data ServerInfo = ServerInfo {
  serverName :: String,
  serverMajorVersion :: Int,
  serverMinorVersion :: Int,
  serverPatchVersion :: Maybe String,
  serverRevision :: Int,
  serverTimeZone :: Maybe String
} deriving Show

serverVersion :: ServerInfo -> String
serverVersion serverInfo =
  (show $ serverMajorVersion serverInfo) ++ "." ++
    (show $ serverMinorVersion serverInfo) ++ "." ++
    (show $ serverRevision serverInfo) ++
    (maybe "" (\p -> " (" ++ p ++ ")") $ serverPatchVersion serverInfo)

-- https://github.com/ClickHouse/ClickHouse/blob/e9af153819d3b666673e583e15705f861cf88cef/dbms/src/Core/Defines.h#L52
hasServerDisplayName :: ClientInfo -> Int -> Bool
hasServerDisplayName clientInfo serverRevision =
  (clientRevision clientInfo) >= 54372 && serverRevision >= 54372

-- https://github.com/ClickHouse/ClickHouse/blob/e9af153819d3b666673e583e15705f861cf88cef/dbms/src/Core/Defines.h#L53
hasPatchVersion :: ClientInfo -> Int -> Bool
hasPatchVersion clientInfo serverRevision =
  (clientRevision clientInfo) >= 54401 && serverRevision >= 54401

-- https://github.com/ClickHouse/ClickHouse/blob/e9af153819d3b666673e583e15705f861cf88cef/dbms/src/Core/Defines.h#L48
hasServerTimeZone :: ClientInfo -> Int -> Bool
hasServerTimeZone clientInfo serverRevision =
  (clientRevision clientInfo) >= 54058 && serverRevision >= 54058

hasQuotaKeyInClientInfo :: ServerInfo -> Bool
hasQuotaKeyInClientInfo serverInfo =
  serverRevision serverInfo >= 54060
