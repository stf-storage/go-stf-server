#!/bin/sh
set -e

if [ -z "$GOPATH" ]; then
  GOPATH=`pwd`
fi

export GOPATH

DEPS="
  code.google.com/p/gcfg
  github.com/bradfitz/gomemcache/memcache
  github.com/dustin/randbo
  github.com/go-sql-driver/mysql
  github.com/vmihailenco/msgpack
"
for dep in $DEPS; do
  IFS=':'
  for path in $GOPATH; do
    echo "Checking $path/src/$dep"
    if [ ! -e $path/src/$dep ]; then
      echo " + fetching $dep..."
      go get $dep
    fi
  done
done

echo "Building bin/storage"
go build -o bin/storage cli_storage.go

echo "Building bin/dispatcher"
go build -o bin/dispatcher cli_dispatcher.go

echo "Build done"
