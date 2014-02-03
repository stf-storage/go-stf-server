#!/bin/sh

set -e
if [ -z "$SKIP_BUILD" ]; then
  export SKIP_DEPS=1
  sh ./build.sh
fi

if [ -z "$STF_QUEUE_TYPE" ]; then
  STF_QUEUE_TYPE=q4m
fi

export STF_DEBUG="1:`pwd`/test.log"
export STF_HOME=`pwd`
exec go test -tags $STF_QUEUE_TYPE -run Basic -v github.com/stf-storage/go-stf-server/stftest