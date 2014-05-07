#!/bin/bash
PROGNAME=$(basename $0)
queue_type=q4m
prefix=/usr/local/stf

usage() {
	echo "Usage: $PROGNAME [OPTIONS]"
	echo "  Installer script for go-stf-server"
	echo
	echo "Options:"
	echo "  -h, --help"
	echo "  -q, --queue [ q4m | redo ] (default: $queue_type)"
	echo "      --prefix ARG (default: $prefix)"
	echo
	exit 1
}


for OPT in "$@"
do
	case "$OPT" in
	'-h'|'--help')
		usage
		exit 1
		;;
	'-q'|'--queue')
		queue_type=$2
		shift 2
		;;
	'--prefix')
		prefix=$2
		shift 2
		;;
	'--'|'-' )
		shift 1
		param+=( "$@" )
		break
		;;
	-*)
		echo "$PROGNAME: illegal option -- '$(echo $1 | sed 's/^-*//')'" 1>&2
		exit 1
		;;
	*)
		if [[ ! -z "$1" ]] && [[ ! "$1" =~ ^-+ ]]; then
		    #param=( ${param[@]} "$1" )
		    param+=( "$1" )
		    shift 1
		fi
		;;
	esac
done

# Check we have hg
output=$(hg --version)
if [ "$?" != "0" ]; then 
	echo "hg is required to install go-stf-server's dependencies"
	exit 1
fi

# Check we have git
output=$(git --version)
if [ "$?" != "0" ]; then 
	echo "git is required to install go-stf-server"
	exit 1
fi

dir=$(mktemp -d -t go-stf-server)
if [ "$?" != "0" ]; then
	echo "failed to create temporary directory"
	exit 1
fi

export GOPATH=$dir
go get -tags $queue_type -v github.com/stf-storage/go-stf-server/...

CMDS="dispatcher storage stf-worker stf-worker-delete_object stf-worker-repair_object stf-worker-replicate_object stf-worker-storage_health"
for cmd in $CMDS
do
	echo "building $cmd..."
	go build -tags $queue_type -o $prefix/bin/$cmd github.com/stf-storage/go-stf-server/cli/$cmd
done

rm -rf $dir

echo "Installed go-stf-server (queue: $queue_type) in $prefix"
