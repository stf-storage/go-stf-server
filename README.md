go-stf-server
=============

[![Build Status](https://travis-ci.org/stf-storage/go-stf-server.png?branch=master)](https://travis-ci.org/stf-storage/go-stf-server)

[![Coverage Status](https://coveralls.io/repos/stf-storage/go-stf-server/badge.png)](https://coveralls.io/r/stf-storage/go-stf-server)

Go Implementation of STF Distributed Object Store

# Current Status

STF is a simple, amazon S3-like distributed object store, aimed for people who want to run such systems on their own data centers, on commodity hardware.

While the [Perl version](https://github.com/stf-storage/stf) is used in production for over 3 years handling billions of hits every day, this go version is still in heavy development.

## Features

* Simple GET/POST/PUT/DELETE to perform CRUD operations
* Multiple copies of the data are stored in the backend storage to prevent data-loss
* Supports Q4M/Redis as queue backends (use '-tags redis' or '-tags q4m' when compiling)
* Automatic storage failure detection

## Done, so far

* Simple object creation works.
* Simple object fetch works.
* Simple object deletion (on the surface) works.
* Workers: Auto-balancing via simple leader-election
* Workers: RepairObject / ReplicateObject / DeleteObject done

## TODO

* Move objects
* Web UI
