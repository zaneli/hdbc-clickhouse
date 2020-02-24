module Lib
    ( someFunc
    ) where

import Control.Monad
import Database.HDBC
import Database.HDBC.ClickHouse (connectClickHouse, Config(..), defaultJoinSqlValues, ping)

someFunc :: IO ()
someFunc = do
  let config = Config { host = "127.0.0.1", port = 9000, database = "default", username = "default", password = "", debug = True, joinSqlValues = defaultJoinSqlValues }
  c <- connectClickHouse config
  pong <- ping c
  print pong
  tables <- getTables c
  print tables
  r <- quickQuery c "select * from test_tbl order by UserID desc" []
  print $ length r
  mapM_ print r
  desc <- describeTable c "test_tbl"
  print desc
  r <- runRaw c "select * from test_tbl"
  print r
  r <- runRaw c "select * from test_tbl"
  print r
