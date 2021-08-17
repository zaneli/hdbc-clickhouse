#!/bin/bash


healthCheck () {
  docker run --rm --link some-clickhouse-server:clickhouse-server yandex/clickhouse-client --host clickhouse-server --query="select 1" > /dev/null
}

NEXT_WAIT_TIME=0
RETRY_LIMIT=4

until healthCheck || [ $NEXT_WAIT_TIME -eq $RETRY_LIMIT ]; do
   sleep $(( NEXT_WAIT_TIME++ ))
done
