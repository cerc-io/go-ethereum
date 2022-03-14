# Foundry README

# Overview

This document will go through the steps needed to test using Foundry. Currently, we use Foundry in the following capacity.

1. Create a private network with our internal version of Geth.
2. Deploy a smart contract to the private network.
3. Test the smart contract on the private network.
4. Create a transaction on the private network.

# Steps

The steps to create a new project are as follows.

## 1. Creating New Project

1. `cd foundry/projects`.
2. Create a directory that captures your project: `mkdir local-private-network; cd local-private-network`.
3. Create a [new foundry project](https://onbjerg.github.io/foundry-book/forge/creating-a-new-project.html): `forge init stateful`.
4. Follow the foundry [documentation](https://onbjerg.github.io/foundry-book/forge/tests.html) for writing smart contract tests.

## 2. Deployments

You can choose to have custom deployments for your workflow. However, it is recommended to utilize Docker.

# Existing Projects

Below, you can find existing projects and their descriptions.

## `local-private-network`

The purpose of this project is as follows:

1. Compile the geth from the local source.
2. Build a docker container with `ipld-eth-db` and another container for the `local-private-network`.
3. Run the compiled version of geth.
4. Deploy a smart contract to the private blockchain.
5. Trigger a transaction on the newly deployed smart contract.

## Using This Project

If you want to test your local geth code, do the following:

1. cd `foundry/projects/local-private-network`.
2. `./wrapper.sh` - This script will do all the heavy lifting for you.
3. Keep an eye out for the outputs from the docker container.
4. Enter the docker container and do as you please.
5. If you want to change your geth code, you will have to run `./wrapper.sh` for subsequent runs.
6. If you do not change your geth code, you have to run: `docker-compose up --build`.

### Key Notes:

- The command to [deploy](https://onbjerg.github.io/foundry-book/forge/deploying.html) the smart contract is: `forge create --keystore $ETH_KEYSTORE_FILE --rpc-url [http://127.0.0.1:8545](http://127.0.0.1:8545/) --constructor-args 1 --password "" --legacy /root/stateful/src/Stateful.sol:Stateful`
- The command to interact create a [transaction](https://onbjerg.github.io/foundry-book/reference/cast.html) is: `cast send --keystore $ETH_KEYSTORE_FILE --rpc-url [http://127.0.0.1:8545](http://127.0.0.1:8545/) --password "" --legacy $DEPLOYED_ADDRESS "off()"`
- The `Dockerfile` compiles `cast` and `forge`.
- The `foundry/projects/local-private-network/deploy-local-network.sh` file does most heavy lifting. It spins up geth and triggers various events.
- The `foundry/projects/local-private-network/start-private-network.sh` file triggers `deploy-local-network.sh`. This file runs all the tests.
- The `geth` node will stay running even after the tests are terminated.
