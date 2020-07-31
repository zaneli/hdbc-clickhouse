# hdbc-clickhouse

[![Build Status](https://travis-ci.org/zaneli/hdbc-clickhouse.svg?branch=master)](https://travis-ci.org/zaneli/hdbc-clickhouse)

---

## Setup

* stack.yaml

```yaml
extra-deps:
- HDBC-2.4.0.3
- git: https://github.com/zaneli/hdbc-clickhouse
  commit: 7d35797817d9cfea95ccf995e6b0af075af9aeb9
```

## Usage

```hs
import Database.HDBC
import Database.HDBC.ClickHouse (connectClickHouse, Config(..), defaultJoinSqlValues, defaultSplitSqlValue)

exec = do
  let config = Config {
    host = "127.0.0.1",
    port = 9000,
    database = "default",
    username = "default",
    password = "",
    debug = True,
    joinSqlValues = defaultJoinSqlValues,
    splitSqlValue = defaultSplitSqlValue
  }
  con  <- connectClickHouse config

  res <- quickQuery con "SELECT * FROM test_tbl_1 WHERE date = ? ORDER BY date DESC" [toSql "2020-01-01"]
  print $ length res
  mapM_ print res

  stmt <- prepare con "INSERT INTO test_tbl_2 VALUES (?, ?)"
  executeMany stmt [[toSql "aaa", toSql "bbb"], [toSql "ccc", toSql "ddd"]]
```

while referencing [clickhouse-go](https://github.com/ClickHouse/clickhouse-go), [hdbc-sqlite3](https://github.com/hdbc/hdbc-sqlite3), and so on.
