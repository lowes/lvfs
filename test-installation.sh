#!/bin/bash
set -e
docker build -t lvfs-test-installation -f test-installation.dockerfile .
docker run --rm lvfs-test-installation python3 -m pytest