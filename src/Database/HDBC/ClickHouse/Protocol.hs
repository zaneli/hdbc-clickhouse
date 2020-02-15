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
  clientRevision = 54431
}

data ServerInfo = ServerInfo {
  serverName :: String,
  serverMajorVersion :: Int,
  serverMinorVersion :: Int,
  serverRevision :: Int,
  serverTimeZone :: Maybe String
} deriving Show

hasTimeZone :: Int -> Bool
hasTimeZone revision =
  revision >= 54058
