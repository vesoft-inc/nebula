# Copyright (c) 2018 vesoft inc. All rights reserved.
#
# This source code is licensed under Apache 2.0 License.

.PHONY: fmt check check-and-diff init init-all clean test tck fail up down test-all ldbc ps kill

PYPI_MIRROR = https://mirrors.aliyun.com/pypi/simple/
# PYPI_MIRROR = http://pypi.mirrors.ustc.edu.cn/simple --trusted-host pypi.mirrors.ustc.edu.cn
CURR_DIR = $(shell dirname $(realpath $(firstword $(MAKEFILE_LIST))))
PY_VERSION = $(shell python3 --version | cut -f2 -d' ' | cut -f2 -d'.')

RM_DIR ?= true
TEST_DIR ?= $(CURR_DIR)
BUILD_DIR ?= $(CURR_DIR)/../build
DEBUG ?= true
J ?= 10
ENABLE_FT_INDEX ?= false
ES_ADDRESS ?= 127.0.0.1:9200
ENABLE_SSL ?= false
ENABLE_GRAPH_SSL ?= false
ENABLE_META_SSL ?= false
CA_SIGNED ?= false
CONTAINERIZED ?= false
FAILED_LOGIN_ATTEMPTS ?= 0
PASSWORD_LOCK_TIME_IN_SECS ?= 0
# start up the graph/storage with multi-job mode
QUERY_CONCURRENTLY ?= false

# commands
gherkin_fmt = ~/.local/bin/reformat-gherkin
run_test = PYTHONPATH=$$PYTHONPATH:$(CURR_DIR)/.. $(CURR_DIR)/nebula-test-run.py

ifeq ($(ENABLE_FT_INDEX),false)
	PYTEST_MARKEXPR="not skip and not ft_index"
else
	export NEBULA_TEST_ES_ADDRESS=${ES_ADDRESS}
	PYTEST_MARKEXPR="not skip"
endif

test_without_skip = python3 -m pytest -m $(PYTEST_MARKEXPR) --build_dir=$(BUILD_DIR)
test_without_skip_sa = python3 -m pytest -m "not skip and not distonly" --build_dir=$(BUILD_DIR)
test_j = $(test_without_skip) -n$(J)
test_j_sa = $(test_without_skip_sa) -n$(J)


install-deps:
	pip3 install  -Ur $(CURR_DIR)/requirements.txt -i $(PYPI_MIRROR)

install-nebula-py: install-deps
	git clone --branch release-3.5 https://github.com/vesoft-inc/nebula-python $(CURR_DIR)/nebula-python
	cd $(CURR_DIR)/nebula-python \
		&& pip3 install  . -i $(PYPI_MIRROR) --upgrade
	rm -rf $(CURR_DIR)/nebula-python

gherkin-fmt: install-deps
	@if [ $(PY_VERSION) -lt 7 ]; then echo 'Python version must >= 3.7'; exit 1; fi
	pip3 install  poetry
	git clone --branch master https://github.com/OneContainer/reformat-gherkin $(CURR_DIR)/reformat-gherkin
	cd $(CURR_DIR)/reformat-gherkin && python3 -m poetry build
	pip3 uninstall -y reformat-gherkin
	pip3 install  $(CURR_DIR)/reformat-gherkin/dist/reformat_gherkin*.whl
	rm -rf $(CURR_DIR)/reformat-gherkin

init: clean install-nebula-py

init-all: init gherkin-fmt

fmt:
	@find $(CURR_DIR)/tck/ -type f -iname '*.feature' -print | xargs $(gherkin_fmt)

check:
	@find $(CURR_DIR)/tck/ -type f -iname "*.feature" -print | xargs $(gherkin_fmt) --check

check-and-diff:
	@(find $(CURR_DIR)/tck/ -type f -iname '*.feature' -print | xargs $(gherkin_fmt)) 2>&1 | tee .gherkin_fmt
	@git diff
	@tail -1 .gherkin_fmt | grep -qv ,

up: clean
	@mkdir -p $(CURR_DIR)/.pytest
	$(run_test) --cmd=start \
		--build_dir=$(BUILD_DIR) \
		--debug=$(DEBUG) \
		--multi_graphd=true \
		--enable_ssl=$(ENABLE_SSL) \
		--enable_graph_ssl=$(ENABLE_GRAPH_SSL) \
		--enable_meta_ssl=$(ENABLE_META_SSL) \
		--ca_signed=$(CA_SIGNED) \
		--containerized=$(CONTAINERIZED) \
		--query_concurrently=$(QUERY_CONCURRENTLY)

standalone-up: clean
	@mkdir -p $(CURR_DIR)/.pytest
	$(run_test) --cmd=start_standalone \
		--build_dir=$(BUILD_DIR) \
		--debug=$(DEBUG) \
		--multi_graphd=false \
		--enable_ssl=$(ENABLE_SSL) \
		--enable_graph_ssl=$(ENABLE_GRAPH_SSL) \
		--enable_meta_ssl=$(ENABLE_META_SSL) \
		--ca_signed=$(CA_SIGNED) \
		--containerized=$(CONTAINERIZED) \
		--query_concurrently=$(QUERY_CONCURRENTLY)

down:
	$(run_test) --cmd=stop --rm_dir=$(RM_DIR)

currdir:
	cd $(CURR_DIR)

sess: currdir
	$(test_without_skip) -k "not tck" job/test_session.py

jobs: currdir
	$(test_without_skip) tck/steps/test_jobs.py

test: sess
	$(test_j) --dist=loadfile -k "not tck" $(TEST_DIR)

slow-query: currdir
	$(test_j) tck/steps/test_kill_slow_query_base_test.py && \
	$(test_j) tck/steps/test_kill_slow_query_via_same_service.py && \
	$(test_j) tck/steps/test_kill_slow_query_via_different_service.py && \
	$(test_j) tck/steps/test_kill_permission_via_same_service.py && \
	$(test_j) tck/steps/test_kill_permission_via_different_service.py

standalone-tck: jobs
	$(test_j_sa) tck/steps/test_tck.py

tck: jobs slow-query
	$(test_j) tck/steps/test_tck.py

ldbc: currdir
	$(test_j) tck/steps/test_ldbc.py

test-all: test tck ldbc
test-standalone-all: standalone-tck ldbc

fail: currdir
	python3 -m pytest \
		--last-failed \
		--gherkin-terminal-reporter \
		--gherkin-terminal-reporter-expanded \
		tck/steps/test_tck.py

clean:
	@rm -rf $(CURR_DIR)/nebula-python \
		$(CURR_DIR)/reformat-gherkin \
		$(CURR_DIR)/.pytest/* \
		$(CURR_DIR)/.pytest_cache \
		$(CURR_DIR)/*.lock \
		$(BUILD_DIR)/server_*

kill:
	ps -ef | grep -P '\sbin/nebula-' | grep "$$(whoami)" | sed 's/\s\s*/ /g' | cut -f2 -d' ' | xargs kill -9

ps:
	@ps -ef | grep -P '\sbin/nebula-' | grep "$$(whoami)"
