go-stf-server
=============

[![Build Status](https://travis-ci.org/stf-storage/go-stf-server.png?branch=master)](https://travis-ci.org/stf-storage/go-stf-server)

[![Coverage Status](https://coveralls.io/repos/stf-storage/go-stf-server/badge.png)](https://coveralls.io/r/stf-storage/go-stf-server)

Go Implementation of STF Distributed Object Store

# Current Status

go-stf-server is still in heavy development

## Done

* Simple object creation works.
* Simple object fetch works.
* Simple object deletion (on the surface) works.
* Workers: Auto-balancing via simple leader-election
* Workers: RepairObject / DeleteObject done

## TODO

* Workers
* Move objects
* Frontend
