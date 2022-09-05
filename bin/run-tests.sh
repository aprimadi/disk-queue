#!/bin/sh

cargo test -- --test-threads=1
rm test.db

