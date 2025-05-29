MAKEFLAGS += --warn-undefined-variables
SHELL := /bin/bash

GO := go
PKGS := $(shell $(GO) list ./... | \
       sed -e "s;github.com/ijsong/pebbletest;.;")

.PHONY: precommit
precommit: mod fmt build mod

.PHONY: build
build:
	${GO} build -o ./bin/pebbletest ${CURDIR}

.PHONY: fmt
fmt:
	@echo goimports
	$(foreach path,$(PKGS),go tool goimports -l -w -local $(shell $(GO) list -m) $(path);)
	@echo gofmt
	$(foreach path,$(PKGS),gofmt -w -s $(path);)

.PHONY: mod
mod:
	${GO} mod tidy
	${GO} mod vendor


