FROM frolvlad/alpine-bash

RUN apk update ; apk add --no-cache --repository http://dl-cdn.alpinelinux.org/alpine/edge/testing/ --allow-untrusted ca-certificates curl bash git jshon jq

#USER guest
WORKDIR /root

# copy all files
ADD ./deploy-local-network.sh .
ADD ./geth-linux-amd64 /bin/geth
ADD ./start-private-network.sh .

RUN curl -L https://foundry.paradigm.xyz | bash; \
    /bin/bash -c 'source $HOME/.bashrc'; \
    /root/.foundry/bin/foundryup

RUN chmod +x /bin/geth

EXPOSE 8545
EXPOSE 8546
ENTRYPOINT ["./start-private-network.sh"]