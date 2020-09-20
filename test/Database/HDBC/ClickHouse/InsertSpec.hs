module Database.HDBC.ClickHouse.InsertSpec (spec) where

import Data.Time (defaultTimeLocale, parseTimeM, UTCTime, ZonedTime)
import Data.UUID (toString)
import Data.UUID.V4 (nextRandom)
import Data.Word
import Database.HDBC.ClickHouse.TestUtil (connect, executeQuery)
import Database.HDBC
import Database.HDBC.SqlValue
import Test.Hspec

spec :: Spec
spec = before_ clearDB $ do
  describe "execute" $ do
    it "insert a record" $ do
      date <- parseTimeM False defaultTimeLocale "%F" "2020-08-01"
      uuid <- nextRandom

      con <- connect
      stmt <- prepare con "INSERT INTO test_tbl_for_insert VALUES (?, ?, ?, ?, ?, ?)"
      result <- execute stmt [
          toSql (123 :: Word32),
          toSql "test123",
          toSql (date :: UTCTime),
          toSql "127.0.0.1",
          toSql "2001:db8:85a3::8a2e:370:7331",
          toSql (toString uuid)
        ]
      result `shouldBe` 1

      count <- getCount "id = 123 AND title = 'test123'"
      count `shouldBe` 1

  describe "executeMany" $ do
    it "insert a record" $ do
      date <- parseTimeM False defaultTimeLocale "%F" "2020-09-01"
      uuid <- nextRandom

      con <- connect
      stmt <- prepare con "INSERT INTO test_tbl_for_insert VALUES (?, ?, ?, ?, ?, ?)"
      executeMany stmt [
          [toSql (234 :: Word32),
           toSql "test234",
           toSql (date :: UTCTime),
           toSql "127.0.0.1",
           toSql "2001:db8:85a3::8a2e:370:7331",
           toSql (toString uuid)]
        ]

      count <- getCount "id = 234 AND title = 'test234'"
      count `shouldBe` 1

    it "insert records" $ do
      date1 <- parseTimeM False defaultTimeLocale "%F" "2020-10-01"
      date2 <- parseTimeM False defaultTimeLocale "%F" "2020-10-02"

      uuid1 <- nextRandom
      uuid2 <- nextRandom

      con <- connect
      stmt <- prepare con "INSERT INTO test_tbl_for_insert VALUES (?, ?, ?, ?, ?, ?)"
      executeMany stmt [
          [toSql (345 :: Word32),
           toSql "test345",
           toSql (date1 :: UTCTime),
           toSql "127.0.0.1",
           toSql "2001:db8:85a3::8a2e:370:7331",
           toSql (toString uuid1)],
          [toSql (456 :: Word32),
           toSql "test456",
           toSql (date2 :: UTCTime),
           toSql "127.0.0.1",
           toSql "2001:db8:85a3::8a2e:370:7331",
           toSql (toString uuid2)]
        ]

      count1 <- getCount "id = 345 AND title = 'test345'"
      count1 `shouldBe` 1

      count2 <- getCount "id = 456 AND title = 'test456'"
      count2 `shouldBe` 1

  describe "quickQuery" $ do
    it "insert a record" $ do
      date <- parseTimeM False defaultTimeLocale "%F" "2020-11-01"
      uuid <- nextRandom

      con <- connect
      results <- quickQuery con "INSERT INTO test_tbl_for_insert VALUES (?, ?, ?, ?, ?, ?)" [
          toSql (567 :: Word32),
          toSql "test567",
          toSql (date :: UTCTime),
          toSql "127.0.0.1",
          toSql "2001:db8:85a3::8a2e:370:7331",
          toSql (toString uuid)
        ]
      (length results) `shouldBe` 0

      count <- getCount "id = 567 AND title = 'test567'"
      count `shouldBe` 1

clearDB :: IO ()
clearDB = do
  let dropQuery = "DROP TABLE IF EXISTS default.test_tbl_for_insert"
  let createQuery = "\
    \CREATE TABLE \
    \  default.test_tbl_for_insert \
    \( \
    \  id    UInt32, \
    \  title String, \
    \  date  Date, \
    \  ip_v4 IPv4, \
    \  ip_v6 IPv6, \
    \  uuid  UUID \
    \) \
    \ENGINE = MergeTree() \
    \PARTITION BY toYYYYMM(date) \
    \ORDER BY (id, intHash32(id)) \
    \SAMPLE BY intHash32(id) \
    \SETTINGS index_granularity = 8192;"

  executeQuery dropQuery
  executeQuery createQuery
  return ()

getCount :: String -> IO Int
getCount condition = do
  let selectQuery = "SELECT COUNT(*) FROM default.test_tbl_for_insert WHERE " ++ condition

  result <- executeQuery selectQuery
  return $ (read . reverse . dropWhile (== '\n') . reverse) result
