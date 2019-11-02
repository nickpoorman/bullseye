# Copyright 2019 Nick Poorman
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

GO_BUILD=go build
GO_TEST?=go test
GO_MOD=go mod

GO_SOURCES  := $(shell find . -path -prune -o -name '*.go' -not -name '*_test.go')
SOURCES_NO_VENDOR := $(shell find . -path ./vendor -prune -o -name "*.go" -not -name '*_test.go' -print)
GO_TEMPLATES := $(shell find . -path ./vendor -prune -o -name "*.tmpl" -print)
GO_COMPILED_TEMPLATES = $(patsubst %.gen.go.tmpl,%.gen.go,$(GO_TEMPLATES))

default: build

build: go-templates

clean:
	find . -type f -name '*.gen.go' -exec rm {} +
	rm -rf bin/
	rm -rf vendor/

test: $(GO_SOURCES)
	$(GO_TEST) $(GO_TEST_ARGS) ./...

ci: test-debug-assert

test-debug-assert: $(GO_SOURCES)
	$(GO_TEST) $(GO_TEST_ARGS) -tags='debug assert' ./...

bench: $(GO_SOURCES)
	$(GO_TEST) $(GO_TEST_ARGS) -bench=. -run=- ./...

go-templates: bin/tmpl $(GO_COMPILED_TEMPLATES)

%.gen.go: %.gen.go.tmpl types.tmpldata
	bin/tmpl -i -data=types.tmpldata $<

fmt: $(SOURCES_NO_VENDOR)
	goimports -w $^

bin/tmpl: ./_tools/tmpl/main.go
	$(GO_BUILD) -o $@ "./$(<D)"

# vendor:
# 	${GO_MOD} vendor

.PHONY: default build clean test ci test-debug-assert bench go-templates