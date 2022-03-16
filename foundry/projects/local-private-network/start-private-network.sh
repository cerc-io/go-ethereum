#!/bin/bash

set -ex

# clean up
trap 'killall geth && rm -rf "$TMPDIR"' EXIT
trap "exit 1" SIGINT SIGTERM

TMPDIR=$(mktemp -d)
/bin/bash deploy-local-network.sh --rpc-addr 0.0.0.0 --chain-id 4 --db-user $DB_USER --db-password $DB_PASSWORD --db-name $DB_NAME \
  --db-host $DB_HOST --db-port $DB_PORT --db-write $DB_WRITE --dir "$TMPDIR" --address $ADDRESS \
  --db-type $DB_TYPE --db-driver $DB_DRIVER --db-waitforsync $DB_WAIT_FOR_SYNC --chain-id $CHAIN_ID &
echo "sleeping 90 sec"
# give it a few secs to start up
sleep 90

# Run tests
cd stateful
forge build
forge test --fork-url http://localhost:8545

# Deploy contracts

ETH_KEYSTORE_FILES=()
echo "ETH KEYSTORE: $TMPDIR/8545/keystore"
for entry in `ls $TMPDIR/8545/keystore`; do
    ETH_KEYSTORE_FILES+=("${TMPDIR}/8545/keystore/${entry}")
done

echo "ETH_KEYSTORE_FILES: $ETH_KEYSTORE_FILES"
ETH_KEYSTORE_FILE=${ETH_KEYSTORE_FILES[0]}

if [ "${#ETH_KEYSTORE_FILES[@]}" -eq 1 ]; then
    echo "Only one KEYSTORE"
else
    echo "WARNING: More than one file in keystore: ${ETH_KEYSTORE_FILES}"
fi

DEPLOYED_ADDRESS=$(forge create --keystore $ETH_KEYSTORE_FILE --rpc-url http://127.0.0.1:8545 --constructor-args 1 --password "" --legacy /root/stateful/src/Stateful.sol:Stateful | grep "Deployed to:" | cut -d " " -f 3)
echo "Contract has been deployed to: $DEPLOYED_ADDRESS"

# Call a transaction

TX_OUT=$(cast send --keystore $ETH_KEYSTORE_FILE --rpc-url http://127.0.0.1:8545 --password "" --legacy $DEPLOYED_ADDRESS "off()")

echo "TX OUTPUT: $TX_OUT"


# Run forever
tail -f /dev/null