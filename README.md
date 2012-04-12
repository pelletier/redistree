# Redistree

[![Build Status](https://secure.travis-ci.org/pelletier/redistree.png?branch=master)](http://travis-ci.org/pelletier/redistree)

Redistree is a simple Python library designed to create a virtual filesystem on
top of Redis.


## Run tests

You can run tests using nose:

    nosetests (--rednose)

If you want to time tests, use `nose-timetests`:

    python nose-timetests.py --with-test-timer -sv (--rednose)
