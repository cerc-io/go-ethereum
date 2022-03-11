#!/bin/bash
set -e

GREEN='\033[0;32m'
RED='\033[0;31m'
NC='\033[0m'

start_path=$(pwd)
cd ../../../
echo -e "${GREEN}Building geth!${NC}"
docker build -t vulcanize/go-ethereum -f Dockerfile .
docker run --rm --entrypoint cat vulcanize/go-ethereum /usr/local/bin/geth > foundry/deployments/local-private-network/geth-linux-amd64
chmod +x foundry/deployments/local-private-network/geth-linux-amd64

echo -e "${GREEN}geth build complete!${NC}"
cd $start_path
