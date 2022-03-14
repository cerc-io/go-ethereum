#!/bin/bash
# This script will run everthing for you. Sit back and enjoy they show.

set -e

./compile-geth.sh
docker-compose up --build