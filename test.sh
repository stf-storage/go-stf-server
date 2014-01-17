#!/bin/sh

export SKIP_DEPS=1
sh ./build.sh

if [ -z "$STF_QUEUE_TYPE" ]; then
  STF_QUEUE_TYPE=q4m
fi

export STF_DEBUG="1:`pwd`/test.log"
exec go test -tags $STF_QUEUE_TYPE -v .