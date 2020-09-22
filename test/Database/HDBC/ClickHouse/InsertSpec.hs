module Database.HDBC.ClickHouse.InsertSpec (spec) where

import Data.Time (defaultTimeLocale, formatTime, parseTimeM, UTCTime, ZonedTime)
import Data.UUID (toString)
import Data.UUID.V4 (nextRandom)
import Data.Word
import Database.HDBC.ClickHouse.TestUtil (connect, executeQuery)
import Database.HDBC
import Database.HDBC.SqlValue
import Test.Hspec
import Text.Printf

spec :: Spec
spec = do
  describe "execute" $ do
    it "insert a record" $ do
      let id = 123 :: Word32
      let title = "test123"
      date <- parseTimeM False defaultTimeLocale "%F" "2020-08-01"
      let ipV4 = "127.0.0.1"
      let ipV6 = "2001:db8:85a3::8a2e:370:7331"
      uuid <- fmap toString nextRandom

      con <- connect
      count <- getCount id title date ipV4 ipV6 uuid
      count `shouldBe` 0

      stmt <- prepare con "INSERT INTO test_tbl_for_insert VALUES (?, ?, ?, ?, ?, ?)"
      result <- execute stmt [
          toSql id,
          toSql title,
          toSql date,
          toSql ipV4,
          toSql ipV6,
          toSql uuid
        ]
      result `shouldBe` 1

      count <- getCount id title date ipV4 ipV6 uuid
      count `shouldBe` 1

  describe "executeMany" $ do
    it "insert a record" $ do
      let id = 234 :: Word32
      let title = "test234"
      date <- parseTimeM False defaultTimeLocale "%F" "2020-09-01"
      let ipV4 = "127.0.0.2"
      let ipV6 = "2001:db8:85a3::8a2e:370:7332"
      uuid <- fmap toString nextRandom

      con <- connect
      count <- getCount id title date ipV4 ipV6 uuid
      count `shouldBe` 0

      stmt <- prepare con "INSERT INTO test_tbl_for_insert VALUES (?, ?, ?, ?, ?, ?)"
      executeMany stmt [
          [toSql id,
           toSql title,
           toSql date,
           toSql ipV4,
           toSql ipV6,
           toSql uuid]
        ]

      count <- getCount id title date ipV4 ipV6 uuid
      count `shouldBe` 1

    it "insert records" $ do
      let id1 = 345 :: Word32
      let title1 = "test345"
      date1 <- parseTimeM False defaultTimeLocale "%F" "2020-10-01"
      let ipV41 = "127.0.0.3"
      let ipV61 = "2001:db8:85a3::8a2e:370:7333"
      uuid1 <- fmap toString nextRandom

      let id2 = 456 :: Word32
      let title2 = "test456"
      date2 <- parseTimeM False defaultTimeLocale "%F" "2020-10-02"
      let ipV42 = "127.0.0.4"
      let ipV62 = "2001:db8:85a3::8a2e:370:7334"
      uuid2 <- fmap toString nextRandom

      con <- connect
      count1 <- getCount id1 title1 date1 ipV41 ipV61 uuid1
      count1 `shouldBe` 0

      count2 <- getCount id2 title2 date2 ipV42 ipV62 uuid2
      count2 `shouldBe` 0

      stmt <- prepare con "INSERT INTO test_tbl_for_insert VALUES (?, ?, ?, ?, ?, ?)"
      executeMany stmt [
          [toSql id1,
           toSql title1,
           toSql date1,
           toSql ipV41,
           toSql ipV61,
           toSql uuid1],
          [toSql id2,
           toSql title2,
           toSql date2,
           toSql ipV42,
           toSql ipV62,
           toSql uuid2]
        ]

      count1 <- getCount id1 title1 date1 ipV41 ipV61 uuid1
      count1 `shouldBe` 1

      count2 <- getCount id2 title2 date2 ipV42 ipV62 uuid2
      count2 `shouldBe` 1

  describe "quickQuery" $ do
    it "insert a record" $ do
      let id = 567 :: Word32
      let title = "test567"
      date <- parseTimeM False defaultTimeLocale "%F" "2020-11-01"
      let ipV4 = "127.0.0.5"
      let ipV6 = "2001:db8:85a3::8a2e:370:7335"
      uuid <- fmap toString nextRandom

      con <- connect
      count <- getCount id title date ipV4 ipV6 uuid
      count `shouldBe` 0

      results <- quickQuery con "INSERT INTO test_tbl_for_insert VALUES (?, ?, ?, ?, ?, ?)" [
          toSql id,
          toSql title,
          toSql date,
          toSql ipV4,
          toSql ipV6,
          toSql uuid
        ]
      (length results) `shouldBe` 0

      count <- getCount id title date ipV4 ipV6 uuid
      count `shouldBe` 1

getCount :: (Num a, PrintfArg a) => a -> String -> UTCTime -> String -> String -> String -> IO Int
getCount id title date ipV4 ipV6 uuid = do
  let selectQuery = "SELECT COUNT(*) FROM default.test_tbl_for_insert WHERE id = %d AND title = '%s' AND date = '%s' AND IPv4NumToString(ip_v4) = '%s' AND IPv6NumToString(ip_v6) = '%s' AND uuid = '%s'"
  result <- executeQuery $ printf selectQuery id title (formatTime defaultTimeLocale "%F" date) ipV4 ipV6 uuid
  return $ (read . reverse . dropWhile (== '\n') . reverse) result
