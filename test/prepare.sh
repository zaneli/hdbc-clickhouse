#!/bin/bash

docker run -it --rm --link some-clickhouse-server:clickhouse-server yandex/clickhouse-client --host clickhouse-server --query="
  CREATE TABLE
    default.test_tbl_1
  (
    id    UInt32,
    title String,
    date  Date,
    ip_v4 IPv4,
    ip_v6 IPv6,
    uuid  UUID
  )
  ENGINE = MergeTree()
  PARTITION BY toYYYYMM(date)
  ORDER BY (id, intHash32(id))
  SAMPLE BY intHash32(id)
  SETTINGS index_granularity = 8192;
"

docker run -it --rm --link some-clickhouse-server:clickhouse-server yandex/clickhouse-client --host clickhouse-server --query="
  INSERT INTO
    default.test_tbl_1
  VALUES
    (1, 'test1', '2020-01-01', '127.0.0.1', '2001:db8:85a3::8a2e:370:7331', '550e8400-e29b-41d4-a716-446655440001'),
    (2, 'test2', '2020-01-02', '127.0.0.2', '2001:db8:85a3::8a2e:370:7332', '550e8400-e29b-41d4-a716-446655440002'),
    (3, '?', '2020-01-03', '127.0.0.3', '2001:db8:85a3::8a2e:370:7333', '550e8400-e29b-41d4-a716-446655440003');
"
