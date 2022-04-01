# This Makefile is meant to be used by people that do not usually work
# with Go source code. If you know what GOPATH is then you probably
# don't need to bother with make.

.PHONY: geth android ios evm all test clean

BIN = $(GOPATH)/bin

## Migration tool
GOOSE = $(BIN)/goose
$(BIN)/goose:
	go get -u github.com/pressly/goose/cmd/goose

GOBIN = ./build/bin
GO ?= latest
GORUN = env GO111MODULE=on go run

#Database
HOST_NAME = localhost
PORT = 5432
USER = vdbm
PASSWORD = password

# Set env variable
# `PGPASSWORD` is used by `createdb` and `dropdb`
export PGPASSWORD=$(PASSWORD)

#Test
TEST_DB = vulcanize_public
TEST_CONNECT_STRING = postgresql://$(USER):$(PASSWORD)@$(HOST_NAME):$(PORT)/$(TEST_DB)?sslmode=disable

geth:
	$(GORUN) build/ci.go install ./cmd/geth
	@echo "Done building."
	@echo "Run \"$(GOBIN)/geth\" to launch geth."

all:
	$(GORUN) build/ci.go install

android:
	$(GORUN) build/ci.go aar --local
	@echo "Done building."
	@echo "Import \"$(GOBIN)/geth.aar\" to use the library."
	@echo "Import \"$(GOBIN)/geth-sources.jar\" to add javadocs"
	@echo "For more info see https://stackoverflow.com/questions/20994336/android-studio-how-to-attach-javadoc"

ios:
	$(GORUN) build/ci.go xcode --local
	@echo "Done building."
	@echo "Import \"$(GOBIN)/Geth.framework\" to use the library."


.PHONY: statedifftest
statedifftest: | $(GOOSE)
	go get github.com/stretchr/testify/assert@v1.7.0
	MODE=statediff go test ./statediff/... -v

test: all
	$(GORUN) build/ci.go test

lint: ## Run linters.
	$(GORUN) build/ci.go lint

clean:
	env GO111MODULE=on go clean -cache
	rm -fr build/_workspace/pkg/ $(GOBIN)/*

# The devtools target installs tools required for 'go generate'.
# You need to put $GOBIN (or $GOPATH/bin) in your PATH to use 'go generate'.

devtools:
	env GOBIN= go install golang.org/x/tools/cmd/stringer@latest
	env GOBIN= go install github.com/kevinburke/go-bindata/go-bindata@latest
	env GOBIN= go install github.com/fjl/gencodec@latest
	env GOBIN= go install github.com/golang/protobuf/protoc-gen-go@latest
	env GOBIN= go install ./cmd/abigen
	@type "solc" 2> /dev/null || echo 'Please install solc'
	@type "protoc" 2> /dev/null || echo 'Please install protoc'
