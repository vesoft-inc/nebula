# Copyright (c) 2018 vesoft inc. All rights reserved.
#
# This source code is licensed under Apache 2.0 License,
# attached with Common Clause Condition 1.0, found in the LICENSES directory.

.PHONY: fmt check init init-all clean test tck fail up down

PYPI_MIRROR = https://mirrors.aliyun.com/pypi/simple/
# PYPI_MIRROR = http://pypi.mirrors.ustc.edu.cn/simple --trusted-host pypi.mirrors.ustc.edu.cn
CURR_DIR = $(shell dirname $(realpath $(firstword $(MAKEFILE_LIST))))
gherkin_fmt = ~/.local/bin/reformat-gherkin
PY_VERSION = $(shell python3 --version | cut -f2 -d' ' | cut -f2 -d'.')

RM_DIR ?= true
TEST_DIR ?= $(CURR_DIR)
BUILD_DIR ?= $(CURR_DIR)/../build
DEBUG ?= true
J ?= 10

install-deps:
	pip3 install --user -U setuptools wheel -i $(PYPI_MIRROR)
	pip3 install --user -r $(CURR_DIR)/requirements.txt -i $(PYPI_MIRROR)

install-nebula-py: install-deps
	git clone --branch master https://github.com/vesoft-inc/nebula-python $(CURR_DIR)/nebula-python
	cd $(CURR_DIR)/nebula-python \
		&& pip3 install --user -r requirements.txt -i $(PYPI_MIRROR) \
		&& python3 setup.py install --user
	rm -rf $(CURR_DIR)/nebula-python

gherkin-fmt: install-deps
	@if [[ $(PY_VERSION) -lt 7 ]]; then echo 'Python version must >= 3.7'; exit 1; fi
	pip3 install --user poetry
	git clone --branch master https://github.com/OneContainer/reformat-gherkin $(CURR_DIR)/reformat-gherkin
	cd $(CURR_DIR)/reformat-gherkin && python3 -m poetry build
	pip3 uninstall -y reformat-gherkin
	pip3 install --user $(CURR_DIR)/reformat-gherkin/dist/reformat_gherkin*.whl
	rm -rf $(CURR_DIR)/reformat-gherkin

init: clean install-nebula-py

init-all: init gherkin-fmt

fmt:
	@find $(CURR_DIR)/tck/ -type f -iname '*.feature' -print | xargs $(gherkin_fmt)

check:
	@find $(CURR_DIR)/tck/ -type f -iname "*.feature" -print | xargs $(gherkin_fmt) --check

up: clean
	@mkdir -p $(CURR_DIR)/.pytest
	PYTHONPATH=$$PYTHONPATH:$(CURR_DIR)/.. $(CURR_DIR)/nebula-test-run.py --cmd=start --rm_dir=$(RM_DIR) --build_dir=$(BUILD_DIR) --debug=$(DEBUG) --multi_graphd=true

down:
	PYTHONPATH=$$PYTHONPATH:$(CURR_DIR)/.. $(CURR_DIR)/nebula-test-run.py --cmd=stop --rm_dir=$(RM_DIR)

currdir:
	cd $(CURR_DIR)

sess: currdir
	python3 -m pytest -m "not skip" -k "not tck" job/test_session.py

jobs: currdir
	python3 -m pytest -m "not skip" -k "not tck" job/test_jobs.py

test: sess jobs
	python3 -m pytest -n$(J) --dist=loadfile -m "not skip" -k "not tck" $(TEST_DIR)

slow-query: currdir
	python3 -m pytest -n$(J) -m "not skip" tck/steps/test_kill_slow_query_via_same_service.py && \
	python3 -m pytest -n$(J) -m "not skip" tck/steps/test_kill_slow_query_via_different_service.py

tck: slow-query
	python3 -m pytest -n$(J) -m "not skip" tck/steps/test_tck.py

fail: currdir
	python3 -m pytest --last-failed --gherkin-terminal-reporter --gherkin-terminal-reporter-expanded tck/steps/test_tck.py

report:
	@mv $(CURR_DIR)/tck-report.json $(CURR_DIR)/tck-report-bak.json
	@jq . $(CURR_DIR)/tck-report-bak.json > tck-report.json
	@rm -rf $(CURR_DIR)/tck-report-bak.json

clean:
	@rm -rf $(CURR_DIR)/nebula-python $(CURR_DIR)/reformat-gherkin $(CURR_DIR)/.pytest/* $(CURR_DIR)/.pytest_cache $(CURR_DIR)/tck-report.json $(CURR_DIR)/*.lock

kill:
	ps -ef | grep -P '\sbin/nebula-' | grep "$$(whoami)" | sed 's/\s\s*/ /g' | cut -f2 -d' ' | xargs kill -9
