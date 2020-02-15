module Lib
    ( someFunc
    ) where

import Database.HDBC.ClickHouse (connectClickHouse, Config(..), ping)

someFunc :: IO ()
someFunc = do
  let config = Config { host = "127.0.0.1", port = 9000, database = "default", username = "default", password = "", debug = True }
  c <- connectClickHouse config
  pong <- ping c
  print pong
  print 1
