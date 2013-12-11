#!/bin/sh
set -e

if [ -z "$GOPATH" ]; then
  GOPATH=`pwd`
fi

export GOPATH

DEPS="
  code.google.com/p/gcfg
  github.com/bradfitz/gomemcache/memcache
  github.com/braintree/manners
  github.com/dustin/randbo
  github.com/go-sql-driver/mysql
  github.com/lestrrat/go-server-starter-listener
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

for executable in worker storage dispatcher; do
  echo "Building bin/$executable"
  go build -a -o bin/$executable src/cli_$executable.go
done

echo "Build done"
