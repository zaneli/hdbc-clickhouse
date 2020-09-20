module Database.HDBC.ClickHouse.InquirySpec (spec) where

import Database.HDBC (describeTable, getTables, hdbcDriverName)
import Database.HDBC.ClickHouse (ping)
import Database.HDBC.ClickHouse.TestUtil (connect, executeQuery)
import Test.Hspec

spec :: Spec
spec = do
  describe "ping" $
    it "get pong" $ do
      con <- connect
      pong <- ping con
      pong `shouldBe` "pong"

  describe "hdbcDriverName" $
    it "get hdbc driver name" $ do
      con <- connect
      let name = hdbcDriverName con
      name `shouldBe` "clickhouse"

  describe "getTables" $
    it "get tables" $ do
      executeQuery "CREATE TABLE IF NOT EXISTS default.test_tbl_for_get_tables (id UInt32) ENGINE = MergeTree() ORDER BY (id, intHash32(id)) SAMPLE BY intHash32(id) SETTINGS index_granularity = 8192;"

      con <- connect
      tables <- getTables con
      executeQuery "DROP TABLE IF EXISTS default.test_tbl_for_get_tables"

      ((length tables) > 1) `shouldBe` True
      ("test_tbl_for_get_tables" `elem` tables) `shouldBe` True

  describe "describeTable" $
    it "get table meta data" $ do
      executeQuery "CREATE TABLE IF NOT EXISTS default.test_tbl_for_describe_table (id UInt32, title String, date Date, ip_v4 IPv4, ip_v6 IPv6, uuid UUID) ENGINE = MergeTree() ORDER BY (id, intHash32(id)) SAMPLE BY intHash32(id) SETTINGS index_granularity = 8192;"

      con <- connect
      descs <- describeTable con "test_tbl_for_describe_table"
      executeQuery "DROP TABLE IF EXISTS default.test_tbl_for_describe_table"

      (length descs) `shouldBe` 6
      let names = map (\(n, _) -> n) descs
      names `shouldBe` ["id", "title", "date", "ip_v4", "ip_v6", "uuid"]
