#!/bin/bash
set -e

OPTS="./deploy-local-network.sh [<options>] <args>...
./deploy-local-network.sh --help
--
db-user=name          database user
db-password=password  database password
db-name=name          database name
db-host=address       database host
db-port=port          database port
db-write=bool         turn on database write mode
db-type=name          the type of database
db-driver=name        the driver used for the database
db-waitforsync=bool   Should the statediff service start once geth has synced to head (default: false)
rpc-port=port         change RPC port (default: 8545)
rpc-addr=address      change RPC address (default: 127.0.0.1)
chain-id=number       change chain ID (default: 99)
period=seconds        use a block time instead of instamine
accounts=number       create multiple accounts (default: 1)
address=address       eth address to add to genesis
save=name             after finishing, save snapshot
load=name             start from a previously saved snapshot
dir=directory         testnet directory
"

eval "$(
  git rev-parse --parseopt -- "$@" <<<"$OPTS" || echo exit $?
)"

DB_USER=vdbm
DB_PASSWORD=password
DB_NAME=vulcanize_public
DB_HOST=127.0.0.1
DB_PORT=5432
DB_TYPE=postgres
DB_DRIVER=sqlx
DB_WAIT_FOR_SYNC=false
RPC_PORT=8545
RPC_ADDRESS=127.0.0.1
PERIOD=0
CHAINID=99
ACCOUNTS=0
ADDRESS=
gethdir=$HOME/testnet

while [[ $1 ]]; do
  case $1 in
    --)              shift; break;;
    --db-user)       shift; DB_USER=$1;;
    --db-password)   shift; DB_PASSWORD=$1;;
    --db-name)       shift; DB_NAME=$1;;
    --db-host)       shift; DB_HOST=$1;;
    --db-port)       shift; DB_PORT=$1;;
    --db-write)      shift; DB_WRITE=$1;;
    --db-type)       shift; DB_TYPE=$1;;
    --db-driver)     shift; DB_DRIVER=$1;;
    --db-waitforsync) shift; DB_WAIT_FOR_SYNC=$1;;
    --rpc-port)      shift; RPC_PORT=$1;;
    --rpc-addr)      shift; RPC_ADDRESS=$1;;
    --chain-id)      shift; CHAINID=$1;;
    --period)        shift; PERIOD=$1;;
    --accounts)      shift; ACCOUNTS=$1;;
    --save)          shift; SAVE=$1;;
    --address)       shift; ADDRESS=$1;;
    --load)          shift; LOAD=$1;;
    --dir)           shift; gethdir=$1;;
    *) printf "${0##*/}: internal error: %q\\n" "$1"; exit 1
  esac; shift
done

chaindir=$gethdir/$RPC_PORT
#while true; do
#  if [[ ! -d "$gethdir/$CHAINID" ]]; then break; fi
#  CHAINID=$((CHAINID + 1))
#done

mkdir -p "$chaindir/config"
#if [ -n "$ADDRESS" ]; then
#  balance+=(-n {} -s "0xffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff" -i balance \
#    -i "$ADDRESS")
#fi
for i in $(seq 0 "$ACCOUNTS"); do
  address+=( "$(
    geth 2>/dev/null account new --datadir "$chaindir" --password=<(exit) 2>/dev/null \
      | grep -o -E "0x[A-Fa-f0-9]*" )" )
#  balance+=(-n {} -s "0xffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff" -i balance \
#    -i "${address[i]}")
  balance+=(' "'"${address[i]}"'": { "balance": "0xffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff"}')
done

if [[ ! -f "./genesis.json" ]]
then
  EXTRA_DATA="0x3132333400000000000000000000000000000000000000000000000000000000${address[0]#0x}0000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000"
  JSON_VAL='{
  "config": {
    "chainId": '"$CHAINID"',
    "homesteadBlock": 0,
    "eip150Block": 0,
    "eip155Block": 0,
    "eip158Block": 0,
    "byzantiumBlock": 0,
    "constantinopleBlock": 0,
    "petersburgBlock": 0,
    "istanbulBlock": 0,
    "clique": {
    "period": '"$PERIOD"',
    "epoch": 3000
    }
  },
  "difficulty": "0x1",
  "gaslimit": "0xffffffffffff",
  "extraData": "'"$EXTRA_DATA"'",
  "alloc": {'"$balance"'}
  }'
  echo $JSON_VAL | jq . > $chaindir/config/genesis.json

  geth 2>/dev/null --datadir "$chaindir" init "$chaindir/config/genesis.json"
else
  echo "Using local genesis file"
  geth 2>/dev/null --datadir "$chaindir" init "./genesis.json"
fi

export ETH_RPC_URL=http://$RPC_ADDRESS:$RPC_PORT

port=$((RPC_PORT + 30000))

geth version
echo >&2 "testnet:  RPC URL: $ETH_RPC_URL"
echo >&2 "testnet:  DB ADDRESS: $DB_HOST"
echo >&2 "testnet:  TCP port: $port"
echo >&2 "testnet:  Chain ID: $CHAINID"
echo >&2 "testnet:  Database: $chaindir"
echo >&2 "testnet:  Geth log: $chaindir/geth.log"

printf "%s\n" "${address[@]}" > "$chaindir/config/account"
echo "$ETH_RPC_URL"           > "$chaindir/config/rpc-url"
echo "$port"                  > "$chaindir/config/node-port"

set +m
# Uncomment below once waitforsync has been merged
# geth \
#   2> >(tee "$chaindir/geth.log" | grep --line-buffered Success | sed 's/^/geth: /' >&2) \
#   --datadir "$chaindir" --networkid "$CHAINID" --port="$port" \
#   --mine --miner.threads=1 --allow-insecure-unlock \
#   --http --http.api "web3,eth,net,debug,personal,statediff" --http.corsdomain '*' --http.vhosts '*' --nodiscover \
#   --http.addr="$RPC_ADDRESS" --http.port="$RPC_PORT" --syncmode=full --gcmode=archive \
#   --statediff --statediff.db.host="$DB_HOST" --statediff.db.port="$DB_PORT" --statediff.db.user="$DB_USER" \
#   --statediff.db.password="$DB_PASSWORD" --statediff.db.name="$DB_NAME" \
#   --statediff.db.nodeid 1 --statediff.db.clientname test1 --statediff.writing="$DB_WRITE" \
#   --statediff.db.type="$DB_TYPE" --statediff.db.driver="$DB_DRIVER" --statediff.waitforsync="$DB_WAIT_FOR_SYNC" \
#   --ws --ws.addr="0.0.0.0" --unlock="$(IFS=,; echo "${address[*]}")" --password=<(exit) &

echo "Starting Geth with following flags"
geth \
  2> >(tee "$chaindir/geth.log" | grep --line-buffered Success | sed 's/^/geth: /' >&2) \
  --datadir "$chaindir" --networkid "$CHAINID" --port="$port" \
  --mine --miner.threads=1 --allow-insecure-unlock \
  --http --http.api "admin,debug,eth,miner,net,personal,txpool,web3,statediff" --http.corsdomain '*' --http.vhosts '*' --nodiscover \
  --http.addr="$RPC_ADDRESS" --http.port="$RPC_PORT" --syncmode=full --gcmode=archive \
  --statediff --statediff.db.host="$DB_HOST" --statediff.db.port="$DB_PORT" --statediff.db.user="$DB_USER" \
  --statediff.db.password="$DB_PASSWORD" --statediff.db.name="$DB_NAME" \
  --statediff.db.nodeid 1 --statediff.db.clientname test1 --statediff.writing="$DB_WRITE" \
  --statediff.db.type="$DB_TYPE" --statediff.db.driver="$DB_DRIVER" \
  --ws --ws.addr="0.0.0.0" --ws.origins '*' --ws.api=admin,debug,eth,miner,net,personal,txpool,web3 \
  --nat=none --miner.gasprice 16000000000 --nat=none \
  --unlock="$(IFS=,; echo "${address[*]}")" --password=<(exit) &

gethpid=$!
echo "Geth started"
echo "Geth PID: $gethpid"

clean() {
  ( set -x; kill -INT $gethpid; wait )
  if [[ $SAVE ]]; then
    echo >&2 "testnet: saving $gethdir/snapshots/$SAVE"
    mkdir -p "$gethdir/snapshots/$SAVE"
    cp -r "$chaindir/keystore" "$gethdir/snapshots/$SAVE"
    cp -r "$chaindir/config" "$gethdir/snapshots/$SAVE"
    geth >/dev/null 2>&1 --datadir "$chaindir" \
       export "$gethdir/snapshots/$SAVE/backup"
  fi
  ( set -x; rm -rf "$chaindir" )
}
trap clean EXIT

echo "Curling: $ETH_RPC_URL"
until curl -s "$ETH_RPC_URL"; do sleep 1; done

echo "Curling: $ETH_RPC_URL complete"
# UPDATE
#ETH_FROM=$(seth --rpc-url="$ETH_RPC_URL" rpc eth_coinbase)
#export ETH_FROM
export ETH_KEYSTORE=$chaindir/keystore
export ETH_PASSWORD=/dev/null
printf 'testnet:  Account: %s (default)\n' "${address[0]}" >&2

[[ "${#address[@]}" -gt 1 ]] && printf 'testnet:   Account: %s\n' "${address[@]:1}" >&2

echo "Geth Start up completed!"
while true; do sleep 3600; done
