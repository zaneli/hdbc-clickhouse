module Database.HDBC.ClickHouse.TestUtil where

import Database.HDBC.ClickHouse (connectClickHouse, Config(..), Connection, defaultJoinSqlValues, defaultSplitSqlValue)

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
