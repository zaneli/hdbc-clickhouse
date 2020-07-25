module Database.HDBC.ClickHouse.Data.Block where

import Database.HDBC.SqlValue
import Database.HDBC.ClickHouse.Data.Column

data Block = Block {
    columns :: [Column],
    rows    :: [[SqlValue]]
}
