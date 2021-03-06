module Database.HDBC.ClickHouse.SelectSpec (spec) where

import Data.Time (defaultTimeLocale, parseTimeM, UTCTime, ZonedTime)
import Data.Word
import Database.HDBC
import Database.HDBC.SqlValue
import Database.HDBC.ClickHouse.TestUtil (connect)
import Test.Hspec

spec :: Spec
spec = do
  date1 <- parseTimeM False defaultTimeLocale "%F" "2020-01-01"
  date2 <- parseTimeM False defaultTimeLocale "%F" "2020-01-02"
  date3 <- parseTimeM False defaultTimeLocale "%F" "2020-01-03"

  let testRecords = [
                      [toSql (1::Word32), toSql "test1", toSql (date1 :: UTCTime), toSql "127.0.0.1", toSql "2001:db8:85a3::8a2e:370:7331", toSql "550e8400-e29b-41d4-a716-446655440001"],
                      [toSql (2::Word32), toSql "test2", toSql (date2 :: UTCTime), toSql "127.0.0.2", toSql "2001:db8:85a3::8a2e:370:7332", toSql "550e8400-e29b-41d4-a716-446655440002"],
                      [toSql (3::Word32), toSql "?", toSql (date3 :: UTCTime), toSql "127.0.0.3", toSql "2001:db8:85a3::8a2e:370:7333", toSql "550e8400-e29b-41d4-a716-446655440003"]
                    ]

  describe "prepare and execute" $ do
    it "no parameter" $ do
      con <- connect
      stmt <- prepare con "select * from test_tbl_for_select"
      execute stmt []
      results <- fetchAllRows stmt
      (length results) `shouldBe` 3
      results `shouldBe` testRecords

    it "int parameter" $ do
      con <- connect
      stmt <- prepare con "select * from test_tbl_for_select where id = ?"
      execute stmt [toSql (1::Int)]
      results <- fetchAllRows stmt
      (length results) `shouldBe` 1
      results `shouldBe` [head testRecords]

    it "string parameter" $ do
      con <- connect
      stmt <- prepare con "select * from test_tbl_for_select where title = ?"
      execute stmt [toSql "test1"]
      results <- fetchAllRows stmt
      (length results) `shouldBe` 1
      results `shouldBe` [head testRecords]

    it "placeholder character inside text" $ do
      con <- connect
      stmt <- prepare con "select * from test_tbl_for_select where title = '?'"
      execute stmt []
      results <- fetchAllRows stmt
      (length results) `shouldBe` 1
      results `shouldBe` [last testRecords]

    it "int and string parameters" $ do
      con <- connect
      stmt <- prepare con "select * from test_tbl_for_select where id = ? or title = ?"
      execute stmt [toSql (3::Int), toSql "test2"]
      results <- fetchAllRows stmt
      (length results) `shouldBe` 2
      results `shouldBe` (tail testRecords)

    it "specify columns" $ do
      con <- connect
      stmt <- prepare con "select ip_v4, uuid from test_tbl_for_select"
      execute stmt []
      results <- fetchAllRows stmt
      (length results) `shouldBe` 3
      results `shouldBe` (map (\record -> [record !! 3, record !! 5]) testRecords)

  describe "quickQuery" $ do
    it "quickQuery multiple times" $ do
      con <- connect
      results1 <- quickQuery con "select * from test_tbl_for_select" []
      (length results1) `shouldBe` 3
      results1 `shouldBe` testRecords

      results2 <- quickQuery con "select * from test_tbl_for_select where id = ? or title = ?" [toSql (3::Int), toSql "test2"]
      (length results2) `shouldBe` 2
      results2 `shouldBe` (tail testRecords)

    it "run and quickQuery" $ do
      con <- connect
      num <- run con "select * from test_tbl_for_select" []
      num `shouldBe` 0

      results <- quickQuery con "select * from test_tbl_for_select where id = ? or title = ?" [toSql (3::Int), toSql "test2"]
      (length results) `shouldBe` 2
      results `shouldBe` (tail testRecords)
