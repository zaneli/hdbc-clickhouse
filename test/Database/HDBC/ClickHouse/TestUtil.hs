module Database.HDBC.ClickHouse.TestUtil where

import Database.HDBC.ClickHouse (connectClickHouse, Config(..), Connection, defaultJoinSqlValues, defaultSplitSqlValue)
import System.Process

testConfig :: Config
testConfig = Config {
    host = "127.0.0.1",
    port = 9000,
    database = "default",
    username = "default",
    password = "",
    debug = True,
    joinSqlValues = defaultJoinSqlValues,
    splitSqlValue = defaultSplitSqlValue
  }

connect :: IO Connection
connect = connectClickHouse testConfig

executeQuery :: String -> IO String
executeQuery query = do
  let dockerCommand = "docker run --rm --link some-clickhouse-server:clickhouse-server yandex/clickhouse-client --host clickhouse-server"
  readCreateProcess (shell $ dockerCommand ++ " --query=\"" ++ query ++ "\"") ""
