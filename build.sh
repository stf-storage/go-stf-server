#!/bin/sh
set -e

if [ -z "$GOPATH" ]; then
  GOPATH=`pwd`
fi

if [ -z "$STF_QUEUE_TYPE" ]; then
  STF_QUEUE_TYPE=q4m
fi

export GOPATH

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
"
for dep in $DEPS; do
  OIFS=$IFS
  IFS=':'
  for path in $GOPATH; do
    echo "Checking $path/src/$dep"
    if [ ! -e $path/src/$dep ]; then
      echo " + fetching $dep..."
      go get $dep
    fi
  done
  IFS=$OIFS
done

EXECUTABLES="
  worker
  worker_storage_health
  worker_repair_object
  worker_delete_object
  dispatcher
  storage
"
for executable in $EXECUTABLES; do
  echo "Building bin/$executable"
  go build -tags $STF_QUEUE_TYPE -a -o bin/$executable cli/$executable.go
done

echo "Build done"
