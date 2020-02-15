module Database.HDBC.ClickHouse.Protocol where

data Config = Config {
  host :: String,
  port :: Int,
  database :: String,
  username :: String,
  password :: String,
  debug :: Bool
} deriving Show

data ClientInfo = ClientInfo {
  clientName :: String,
  clientMajorVersion :: Int,
  clientMinorVersion :: Int,
  clientRevision :: Int
} deriving Show

clientInfo :: ClientInfo
clientInfo = ClientInfo {
  clientName = "hdbc-clickhouse",
  clientMajorVersion = 1,
  clientMinorVersion = 1,
  clientRevision = 54213
}

data ServerInfo = ServerInfo {
  serverName :: String,
  serverMajorVersion :: Int,
  serverMinorVersion :: Int,
  serverPatchVersion :: Maybe String,
  serverRevision :: Int,
  serverTimeZone :: Maybe String
} deriving Show

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
