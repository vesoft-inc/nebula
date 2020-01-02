#!/usr/bin/env python3
# --coding:utf-8--
#
# Copyright (c) 2019 vesoft inc. All rights reserved.
#
# This source code is licensed under Apache 2.0 License,
# attached with Common Clause Condition 1.0, found in the LICENSES directory.

import itertools
import os
import pytest
import sys
import logging
from _pytest.main import ExitCode
from time import gmtime, strftime

NEBULA_HOME = os.environ['NEBULA_HOME']
TEST_DIR = os.path.join(os.environ['NEBULA_HOME'], 'tests')
sys.path.insert(0, NEBULA_HOME)
RESULT_DIR = os.path.join(os.environ['NEBULA_TEST_LOGS_DIR'], 'results')
LOGGING_ARGS = {
    '--junit-xml': 'TEST-nebula-{0}.xml',
    '--report-log': 'TEST-nebula-{0}.log',
    '--html': 'TEST-nebula-{0}.html'
}

LOG_FORMAT = "-- %(asctime)s %(levelname)-8s %(threadName)s: %(message)s"


def configure_logging():
    logging.basicConfig(level=logging.INFO, format=LOG_FORMAT)


class NebulaTestPlugin(object):
    def __init__(self):
        self.tests_collected = set()
        self.tests_executed = set()

    # pytest hook to handle test collection when xdist is used (parallel tests)
    # https://github.com/pytest-dev/pytest-xdist/pull/35/commits (No official documentation available)
    def pytest_xdist_node_collection_finished(self, node, ids):
        self.tests_collected.update(set(ids))

    # link to pytest_collection_modifyitems
    # https://docs.pytest.org/en/5.3.2/writing_plugins.html#hook-function-validation-and-execution
    @pytest.hookimpl(tryfirst=True)
    def pytest_collection_modifyitems(self, items):
        for item in items:
            self.tests_collected.add(item.nodeid)

    # link to pytest_runtest_logreport
    # https://docs.pytest.org/en/5.3.2/reference.html#_pytest.hookspec.pytest_runtest_logreport
    def pytest_runtest_logreport(self, report):
        if report.passed:
            self.tests_executed.add(report.nodeid)

    def pytest_addoption(self, parser, pluginmanager):
        parser.addoption('--address',
                         dest='address',
                         default="127.0.0.1:3699",
                         help="address of the Nebula")
        parser.addoption('--user',
                         dest='user',
                         default='user',
                         help='the user of Nebula')
        parser.addoption('--password',
                         dest='password',
                         default='password',
                         help='the password of Nebula')

    def pytest_configure(self, config):
        configure_logging()
        pytest.cmdline.address = config.getoption("address")
        pytest.cmdline.user = config.getoption("user")
        pytest.cmdline.password = config.getoption("password")


class TestExecutor(object):
    def __init__(self, exit_on_error=True):
        self._exit_on_error = exit_on_error
        self.tests_failed = False
        self.total_executed = 0

    def run_tests(self, args):
        plugin = NebulaTestPlugin()

        try:
            pytest_exit_code = pytest.main(args, plugins=[plugin])
        except:
            sys.stderr.write(
                "Unexpected exception with pytest {0}".format(args))
            raise

        if '--collect-only' in args:
            for test in plugin.tests_collected:
                print(test)

        self.total_executed += len(plugin.tests_executed)

        if self._exit_on_error:
            sys.exit(pytest_exit_code)


if __name__ == "__main__":
    executor = TestExecutor()
    # If the user is just asking for --help, just print the help test and then exit.
    if '-h' in sys.argv[1:] or '--help' in sys.argv[1:]:
        executor.run_tests(sys.argv[1:])
        sys.exit(0)
    os.chdir(TEST_DIR)
    # Create the test result directory if it doesn't already exist.
    if not os.path.exists(RESULT_DIR):
        os.makedirs(RESULT_DIR)

    commandline_args = itertools.chain(
        *[arg.split('=') for arg in sys.argv[1:]])
    current_time = strftime("%Y-%m-%d-%H:%M:%S", gmtime())
    logging_args = []
    for arg, log in LOGGING_ARGS.items():
        logging_args.extend(
            [arg, os.path.join(RESULT_DIR, log.format(current_time))])

    args = logging_args + list(commandline_args)
    print("Running TestExecutor with args: {} ".format(args))
    executor.run_tests(list(args))
    if executor.total_executed == 0:
        sys.exit(1)
    if executor.tests_failed:
        sys.exit(1)
