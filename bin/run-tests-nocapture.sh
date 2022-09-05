#!/bin/sh

cargo test -- --test-threads=1 --nocapture
rm test.db

