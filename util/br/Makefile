GO      ?= go
PKG := ./pkg/...

BUILDTARGET := bin/br
SUFFIX := $(GOEXE)

build:
	$(GO) build -o $(BUILDTARGET) main.go

test:
	$(GO) test -v $(PKG) -short
