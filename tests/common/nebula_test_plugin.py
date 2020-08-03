# --coding:utf-8--
#
# Copyright (c) 2020 vesoft inc. All rights reserved.
#
# This source code is licensed under Apache 2.0 License,
# attached with Common Clause Condition 1.0, found in the LICENSES directory.

import pytest
import os
import logging

DOCKER_GRAPHD_DIGESTS = os.getenv('NEBULA_GRAPHD_DIGESTS')
if DOCKER_GRAPHD_DIGESTS is None:
    DOCKER_GRAPHD_DIGESTS = '0'
DOCKER_METAD_DIGESTS = os.getenv('NEBULA_METAD_DIGESTS')
if DOCKER_METAD_DIGESTS is None:
    DOCKER_METAD_DIGESTS = '0'
DOCKER_STORAGED_DIGESTS = os.getenv('NEBULA_STORAGED_DIGESTS')
if DOCKER_STORAGED_DIGESTS is None:
    DOCKER_STORAGED_DIGESTS = '0'

LOG_FORMAT = "-- %(asctime)s %(levelname)-8s %(threadName)s: %(message)s"


def configure_logging():
    logging.basicConfig(level=logging.INFO, format=LOG_FORMAT)


class NebulaTestPlugin(object):
    def __init__(self, work_dir: str = ""):
        self.tests_collected = set()
        self.tests_executed = set()
        self.data_dir = os.getenv('NEBULA_DATA_DIR')
        if self.data_dir is None or self.data_dir == "":
            self.data_dir = work_dir + "/data"

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
        pytest.cmdline.data_dir = self.data_dir
        pytest.cmdline.stop_nebula = config.getoption("stop_nebula")
        pytest.cmdline.rm_dir = config.getoption("rm_dir")
        config._metadata['graphd digest'] = DOCKER_GRAPHD_DIGESTS
        config._metadata['metad digest'] = DOCKER_METAD_DIGESTS
        config._metadata['storaged digest'] = DOCKER_STORAGED_DIGESTS
