#!/bin/bash
set -e
docker build -t test_lvfs -f test.dockerfile .
docker run --rm test_lvfs python3 -m pytest