#!/bin/sh
set -e

if [ -z "$GOPATH" ]; then
  GOPATH=`pwd`
fi

if [ -z "$STF_QUEUE_TYPE" ]; then
  STF_QUEUE_TYPE=q4m
fi

export GOPATH

if [ -z "$SKIP_DEPS" ]; then
  DEPS="
    code.google.com/p/gcfg
    github.com/bradfitz/gomemcache/memcache
    github.com/braintree/manners
    github.com/dustin/randbo
    github.com/go-sql-driver/mysql
    github.com/lestrrat/go-apache-logformat
    github.com/lestrrat/go-file-rotatelogs
    github.com/lestrrat/go-server-starter-listener
    github.com/lestrrat/go-test-mysqld
    github.com/vmihailenco/msgpack
    github.com/vmihailenco/redis/v2
  "
  for dep in $DEPS; do
    OIFS=$IFS
    IFS=':'
    for path in $GOPATH; do
      if [ ! -e $path/src/$dep ]; then
        echo " + fetching $dep..."
        go get $dep
      fi
    done
    IFS=$OIFS
  done
fi

WORKERS="
  delete_object
  repair_object
  storage_health
"
for executable in $WORKERS; do
  echo "Building in bin/stf-worker-$executable"
  go build -tags $STF_QUEUE_TYPE -a -o bin/stf-worker-$executable cli/stf-worker-$executable.go
done

EXECUTABLES="
  stf-worker
  dispatcher
  storage
"
for executable in $EXECUTABLES; do
  echo "Building bin/$executable"
  go build -tags $STF_QUEUE_TYPE -a -o bin/$executable cli/$executable.go
done

echo "Build done"
