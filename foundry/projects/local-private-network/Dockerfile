FROM frolvlad/alpine-bash

# copy all files

RUN apk update ; apk add --no-cache --allow-untrusted ca-certificates curl bash git jq

ENV GLIBC_REPO=https://github.com/sgerrand/alpine-pkg-glibc
ENV GLIBC_VERSION=2.35-r0

RUN set -ex && \
    apk --update add libstdc++ curl ca-certificates && \
    for pkg in glibc-${GLIBC_VERSION} glibc-bin-${GLIBC_VERSION}; \
        do curl -sSL ${GLIBC_REPO}/releases/download/${GLIBC_VERSION}/${pkg}.apk -o /tmp/${pkg}.apk; done && \
    apk add --allow-untrusted /tmp/*.apk ; \
    rm -v /tmp/*.apk ;/usr/glibc-compat/sbin/ldconfig /lib /usr/glibc-compat/lib

RUN apk add gcompat; echo "Sorry"
WORKDIR /root

COPY stateful ./stateful
ADD ./start-private-network.sh .
ADD ./deploy-local-network.sh .
ADD ../../geth-linux-amd64 /bin/geth

RUN curl -L https://foundry.paradigm.xyz | bash; \
    /bin/bash -c 'source $HOME/.bashrc'; \
    /root/.foundry/bin/foundryup

ENV PATH "$PATH:/root/.foundry/bin/"
RUN echo "export PATH=${PATH}" >> $HOME/.bashrc;

RUN chmod +x /bin/geth


EXPOSE 8545
EXPOSE 8546
ENTRYPOINT ["./start-private-network.sh"]