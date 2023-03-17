#!/bin/bash

set -e

mkdir -p out

# Remove existing docker-tsdb directory
rm -rf out/docker-tsdb/

# Copy over files to setup TimescaleDB
ID=$(docker create vulcanize/ipld-eth-db:v4.1.1-alpha)
docker cp $ID:/app/docker-tsdb out/docker-tsdb/
docker rm -v $ID

# Spin up TimescaleDB
docker-compose -f out/docker-tsdb/docker-compose.test.yml -f docker-compose.yml up ipld-eth-db
sleep 45

# Run unit tests
go clean -testcache
make statedifftest

# Clean up
docker-compose -f out/docker-tsdb/docker-compose.test.yml -f docker-compose.yml down --remove-orphans --volumes
rm -rf out/docker-tsdb/
