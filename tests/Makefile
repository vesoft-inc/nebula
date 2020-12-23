# Copyright (c) 2018 vesoft inc. All rights reserved.
#
# This source code is licensed under Apache 2.0 License,
# attached with Common Clause Condition 1.0, found in the LICENSES directory.

.PHONY: fmt check init init-all clean test tck

PYPI_MIRROR = https://mirrors.aliyun.com/pypi/simple/
CURR_DIR = $(shell dirname $(realpath $(firstword $(MAKEFILE_LIST))))
gherkin_fmt = ~/.local/bin/reformat-gherkin

RM_DIR ?= true
TEST_DIR ?= $(CURR_DIR)

install-deps:
	pip3 install --user -U setuptools wheel -i $(PYPI_MIRROR)
	pip3 install --user -r $(CURR_DIR)/requirements.txt -i $(PYPI_MIRROR)

install-nebula-py: install-deps
	git clone --branch master https://github.com/vesoft-inc/nebula-python $(CURR_DIR)/nebula-python
	cd $(CURR_DIR)/nebula-python && python3 setup.py install --user
	rm -rf $(CURR_DIR)/nebula-python

gherkin-fmt: install-deps
	pip3 install --user poetry
	git clone --branch master https://github.com/OneContainer/reformat-gherkin $(CURR_DIR)/reformat-gherkin
	cd $(CURR_DIR)/reformat-gherkin && python3 -m poetry build
	pip3 install --user $(CURR_DIR)/reformat-gherkin/dist/reformat_gherkin*.whl
	rm -rf $(CURR_DIR)/reformat-gherkin

init: clean install-nebula-py

init-all: init gherkin-fmt

fmt:
	@find $(CURR_DIR)/tck/features -type f -iname '*.feature' -print | xargs $(gherkin_fmt)

check:
	@find $(CURR_DIR)/tck/features -type f -iname "*.feature" -print | xargs $(gherkin_fmt) --check

test:
	cd $(CURR_DIR) && python3 -m pytest -n8 --dist=loadfile --rm_dir=$(RM_DIR) -m "not skip" $(TEST_DIR)

tck:
	cd $(CURR_DIR) && python3 -m pytest --gherkin-terminal-reporter --gherkin-terminal-reporter-expanded --rm_dir=$(RM_DIR) -m "not skip" $(CURR_DIR)/tck/

clean:
	@rm -rf $(CURR_DIR)/nebula-python $(CURR_DIR)/reformat-gherkin $(CURR_DIR)/.pytest $(CURR_DIR)/.pytest_cache
