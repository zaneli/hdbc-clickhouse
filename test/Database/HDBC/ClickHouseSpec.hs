module Database.HDBC.ClickHouseSpec (spec) where

import Database.HDBC
import Database.HDBC.SqlValue
import Database.HDBC.ClickHouse (connectClickHouse, Config(..), defaultJoinSqlValues, ping)
import Test.Hspec

spec :: Spec
spec = do
  let config = Config {
    host = "127.0.0.1",
    port = 9000,
    database = "default",
    username = "default",
    password = "",
    debug = True,
    joinSqlValues = defaultJoinSqlValues
  }

  describe "ping" $
    it "ping" $ do
      con <- connectClickHouse config
      pong <- ping con
      pong `shouldBe` "pong"
