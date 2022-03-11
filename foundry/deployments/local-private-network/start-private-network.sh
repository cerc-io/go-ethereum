#!/bin/bash

set -ex

# clean up
trap 'killall geth && rm -rf "$TMPDIR"' EXIT
trap "exit 1" SIGINT SIGTERM

TMPDIR=$(mktemp -d)
/bin/bash deploy-local-network.sh --rpc-addr 0.0.0.0 --chain-id 4 --db-user $DB_USER --db-password $DB_PASSWORD --db-name $DB_NAME \
  --db-host $DB_HOST --db-port $DB_PORT --db-write $DB_WRITE --dir "$TMPDIR" --address $ADDRESS \
  --db-type $DB_TYPE --db-driver $DB_DRIVER --db-waitforsync $DB_WAIT_FOR_SYNC &
echo "sleeping 90 sec"
# give it a few secs to start up
sleep 90

#read -r ACC BAL <<< "$(seth ls --keystore "$TMPDIR/8545/keystore")"
#echo $ACC
#echo $BAL
#
#
## Deploy a contract:
#solc --bin --bin-runtime docker/stateful.sol -o "$TMPDIR"
#A_ADDR=$(seth send --create "$(<"$TMPDIR"/A.bin)" "constructor(uint y)" 1 --from "$ACC" --keystore "$TMPDIR"/8545/keystore --password /dev/null --gas 0xfffffff)
#
#echo $A_ADDR
#
## Call transaction
#
#TX=$(seth send "$A_ADDR" "off()" --gas 0xffff --password /dev/null --from "$ACC" --keystore "$TMPDIR"/8545/keystore --async)
#echo $TX
#RESULT=$(seth run-tx "$TX")
#echo $RESULT

# Run forever
tail -f /dev/null