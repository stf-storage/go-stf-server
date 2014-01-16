#!/bin/sh

export SKIP_DEPS=1
sh ./build.sh
export STF_DEBUG="1:`pwd`/test.log"
exec go test -v .