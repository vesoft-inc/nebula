# Copyright (c) 2020 vesoft inc. All rights reserved.
#
# This source code is licensed under Apache 2.0 License.
#

ifneq ($(wildcard ../build/third-party/install),)
  3PP_PATH := ../build/third-party/install
else ifneq ($(wildcard /opt/vesoft/third-party/3.3),)
  3PP_PATH := /opt/vesoft/third-party/3.3
else ifneq ($(wildcard /opt/vesoft/third-party/3.0),)
  3PP_PATH := /opt/vesoft/third-party/3.0
else
  $(error "Cannot find the third-party installation directory")
endif

CXX := g++
CXX_FLAGS := -c -I ../src/ -I $(3PP_PATH)/include/ -fPIC

UDF ?= standard_deviation

all: standard_deviation.cpp
	$(CXX) $(CXX_FLAGS) $(UDF).cpp -o $(UDF).o
	$(CXX) -shared -o $(UDF).so $(UDF).o

clean:
	rm ./*.o
	rm ./*.so

.PHONY: help
help:
	@echo "Usage: make UDF=<udf_name>"
	@echo "Example: make UDF=standard_deviation"
