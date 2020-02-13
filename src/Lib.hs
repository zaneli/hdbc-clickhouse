module Lib
    ( someFunc
    ) where

import Database.HDBC.ClickHouse

someFunc :: IO ()
someFunc = do
  c <- connectClickHouse "127.0.0.1" 9000 "default" "default" ""
  pong <- ping c
  print pong
