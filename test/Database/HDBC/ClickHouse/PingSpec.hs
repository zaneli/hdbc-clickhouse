module Database.HDBC.ClickHouse.PingSpec (spec) where

import Database.HDBC.ClickHouse (ping)
import Database.HDBC.ClickHouse.TestUtil (connect)
import Test.Hspec

spec :: Spec
spec = do
  describe "ping" $
    it "ping" $ do
      con <- connect
      pong <- ping con
      pong `shouldBe` "pong"
