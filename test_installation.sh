#!/bin/bash
set -e
docker build -t lvfs_test_installation -f test_installation.dockerfile .
docker run lvfs_test_installation /home/nemo/lvfs/tests/test_installation_methods.sh