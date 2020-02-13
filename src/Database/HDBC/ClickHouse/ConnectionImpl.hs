module Database.HDBC.ClickHouse.ConnectionImpl (Connection(..)) where

import qualified Database.HDBC.Statement as Types
import qualified Database.HDBC.Types as Types
import Database.HDBC.ColTypes as ColTypes
import Control.Monad

data Connection =
    Connection {
        disconnect :: IO (),
        commit :: IO (),
        rollback :: IO (),
        run :: String -> [Types.SqlValue] -> IO Integer,
        runRaw :: String -> IO (),
        ping :: IO String
    }
