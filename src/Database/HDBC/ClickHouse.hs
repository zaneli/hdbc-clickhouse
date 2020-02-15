module Database.HDBC.ClickHouse
    (
      connectClickHouse, Config(..), Connection(), ping
    )
  where

import Database.HDBC.ClickHouse.Connection (connectClickHouse, Connection(), ping)
import Database.HDBC.ClickHouse.Protocol (Config(..))
