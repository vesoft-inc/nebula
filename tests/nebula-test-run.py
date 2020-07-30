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
from time import localtime, strftime
from pathlib import Path
from pathlib import Path

TEST_DIR = os.path.dirname(os.path.abspath(__file__))
NEBULA_HOME = TEST_DIR + '/../'
sys.path.insert(0, NEBULA_HOME)

from tests.common.nebula_manager import NebulaManager

TEST_LOGS_DIR = os.getenv('NEBULA_TEST_LOGS_DIR')
if TEST_LOGS_DIR is None or TEST_LOGS_DIR == "":
    TEST_LOGS_DIR = os.environ['HOME']

NEBULA_BUILD_DIR = os.getenv('NEBULA_BUILD_DIR')
if NEBULA_BUILD_DIR is None:
    NEBULA_BUILD_DIR = str(Path(TEST_DIR).parent)
NEBULA_SOURCE_DIR = os.getenv('NEBULA_SOURCE_DIR')
if NEBULA_SOURCE_DIR is None:
    NEBULA_SOURCE_DIR = str(Path(TEST_DIR).parent)

NEBULA_DATA_DIR = os.getenv('NEBULA_DATA_DIR')
if NEBULA_DATA_DIR is None or NEBULA_DATA_DIR == "":
    NEBULA_DATA_DIR = TEST_DIR + "/data"

RESULT_DIR = os.path.join(TEST_LOGS_DIR, 'results')
LOGGING_ARGS = {'--html': 'TEST-nebula-{0}.html'}

LOG_FORMAT = "-- %(asctime)s %(levelname)-8s %(threadName)s: %(message)s"

DOCKER_GRAPHD_DIGESTS = os.getenv('NEBULA_GRAPHD_DIGESTS')
if DOCKER_GRAPHD_DIGESTS is None:
    DOCKER_GRAPHD_DIGESTS = '0'
DOCKER_METAD_DIGESTS = os.getenv('NEBULA_METAD_DIGESTS')
if DOCKER_METAD_DIGESTS is None:
    DOCKER_METAD_DIGESTS = '0'
DOCKER_STORAGED_DIGESTS = os.getenv('NEBULA_STORAGED_DIGESTS')
if DOCKER_STORAGED_DIGESTS is None:
    DOCKER_STORAGED_DIGESTS = '0'


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

    # link to pytest_addoption
    # https://docs.pytest.org/en/latest/reference.html#_pytest.hookspec.pytest_addoption
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
        parser.addoption('--partition_num',
                         dest='partition_num',
                         default=10,
                         help='the partition_num of Nebula\'s space')
        parser.addoption('--replica_factor',
                         dest='replica_factor',
                         default=1,
                         help='the replica_factor of Nebula\'s space')
        parser.addoption('--data_dir',
                         dest='data_dir',
                         help='Data Preload Directory for Nebula')

        parser.addoption('--stop_nebula',
                         dest='stop_nebula',
                         default='true',
                         help='Stop the nebula services')

        parser.addoption('--rm_dir',
                         dest='rm_dir',
                         default='true',
                         help='Remove the temp test dir')

    # link to pytest_configure
    # https://docs.pytest.org/en/latest/reference.html#_pytest.hookspec.pytest_configure
    def pytest_configure(self, config):
        configure_logging()
        pytest.cmdline.address = config.getoption("address")
        pytest.cmdline.user = config.getoption("user")
        pytest.cmdline.password = config.getoption("password")
        pytest.cmdline.replica_factor = config.getoption("replica_factor")
        pytest.cmdline.partition_num = config.getoption("partition_num")
        pytest.cmdline.data_dir = NEBULA_DATA_DIR
        pytest.cmdline.stop_nebula = config.getoption("stop_nebula")
        pytest.cmdline.rm_dir = config.getoption("rm_dir")
        config._metadata['graphd digest'] = DOCKER_GRAPHD_DIGESTS
        config._metadata['metad digest'] = DOCKER_METAD_DIGESTS
        config._metadata['storaged digest'] = DOCKER_STORAGED_DIGESTS


class TestExecutor(object):
    def __init__(self, exit_on_error=True):
        self._exit_on_error = exit_on_error
        self.tests_failed = False
        self.total_executed = 0

    def run_tests(self, args):
        plugin = NebulaTestPlugin()

        try:
            pytest.main(args, plugins=[plugin])
        except Exception:
            sys.stderr.write(
                "Unexpected exception with pytest {0}".format(args))
            raise

        if '--collect-only' in args:
            for test in plugin.tests_collected:
                print(test)

        self.total_executed += len(plugin.tests_executed)


if __name__ == "__main__":
    # If the user is just asking for --help, just print the help test and then exit.
    executor = TestExecutor()
    if '-h' in sys.argv[1:] or '--help' in sys.argv[1:]:
        executor.run_tests(sys.argv[1:])
        sys.exit(0)
    nebula_mgr = NebulaManager(NEBULA_BUILD_DIR, NEBULA_SOURCE_DIR)
    stop_nebula = True
    try:
        os.chdir(TEST_DIR)
        # Create the test result directory if it doesn't already exist.
        if not os.path.exists(RESULT_DIR):
            os.makedirs(RESULT_DIR)

        commandline_args = itertools.chain(
            *[arg.split('=') for arg in sys.argv[1:]])
        current_time = strftime("%Y-%m-%d-%H:%M:%S", localtime())
        args = []
        for arg, log in LOGGING_ARGS.items():
            args.extend(
                [arg, os.path.join(RESULT_DIR, log.format(current_time))])

        args.extend(list(commandline_args))

        if '--address' not in args:
            nebula_mgr.install()
            port = nebula_mgr.start()
            args.extend(['--address', '127.0.0.1:' + str(port)])
        else:
            stop_nebula = False
        print("Running TestExecutor with args: {} ".format(args))
        executor.run_tests(args)
    finally:
        if stop_nebula and pytest.cmdline.stop_nebula.lower() == 'true':
            nebula_mgr.stop(pytest.cmdline.rm_dir.lower() == 'true')

    if executor.total_executed == 0:
        sys.exit(1)
    if executor.tests_failed:
        sys.exit(1)
