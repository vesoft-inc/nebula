# --coding:utf-8--
#
# Copyright (c) 2020 vesoft inc. All rights reserved.
#
# This source code is licensed under Apache 2.0 License,
# attached with Common Clause Condition 1.0, found in the LICENSES directory.

import json
import os
import pytest

from tests.common.configs import all_configs
from tests.common.types import SpaceDesc
from tests.common.utils import get_conn_pool
from tests.common.constants import NB_TMP_PATH, SPACE_TMP_PATH

tests_collected = set()
tests_executed = set()
data_dir = os.getenv('NEBULA_DATA_DIR')

CURR_PATH = os.path.dirname(os.path.abspath(__file__))


# pytest hook to handle test collection when xdist is used (parallel tests)
# https://github.com/pytest-dev/pytest-xdist/pull/35/commits (No official documentation available)
def pytest_xdist_node_collection_finished(node, ids):
    tests_collected.update(set(ids))


# link to pytest_collection_modifyitems
# https://docs.pytest.org/en/5.3.2/writing_plugins.html#hook-function-validation-and-execution
@pytest.hookimpl(tryfirst=True)
def pytest_collection_modifyitems(items):
    for item in items:
        tests_collected.add(item.nodeid)


# link to pytest_runtest_logreport
# https://docs.pytest.org/en/5.3.2/reference.html#_pytest.hookspec.pytest_runtest_logreport
def pytest_runtest_logreport(report):
    if report.passed:
        tests_executed.add(report.nodeid)


def pytest_addoption(parser):
    for config in all_configs:
        parser.addoption(config,
                         dest=all_configs[config][0],
                         default=all_configs[config][1],
                         help=all_configs[config][2])

    parser.addoption("--build_dir",
                     dest="build_dir",
                     default=f"{CURR_PATH}/../build",
                     help="Nebula Graph CMake build directory")


def pytest_configure(config):
    pytest.cmdline.address = config.getoption("address")
    pytest.cmdline.user = config.getoption("user")
    pytest.cmdline.password = config.getoption("password")
    pytest.cmdline.replica_factor = config.getoption("replica_factor")
    pytest.cmdline.partition_num = config.getoption("partition_num")
    if data_dir is None:
        pytest.cmdline.data_dir = config.getoption("data_dir")
    else:
        pytest.cmdline.data_dir = data_dir
    pytest.cmdline.stop_nebula = config.getoption("stop_nebula")
    pytest.cmdline.rm_dir = config.getoption("rm_dir")
    pytest.cmdline.debug_log = config.getoption("debug_log")


def get_port():
    with open(NB_TMP_PATH, "r") as f:
        data = json.loads(f.readline())
        port = data.get("port", None)
        if port is None:
            raise Exception(f"Invalid port: {port}")
        return port[0]

def get_ports():
    with open(NB_TMP_PATH, "r") as f:
        data = json.loads(f.readline())
        port = data.get("port", None)
        if port is None:
            raise Exception(f"Invalid port: {port}")
        return port

@pytest.fixture(scope="session")
def conn_pool_to_first_graph_service(pytestconfig):
    addr = pytestconfig.getoption("address")
    host_addr = addr.split(":") if addr else ["localhost", get_ports()[0]]
    assert len(host_addr) == 2
    pool = get_conn_pool(host_addr[0], host_addr[1])
    yield pool
    pool.close()

@pytest.fixture(scope="session")
def conn_pool_to_second_graph_service(pytestconfig):
    addr = pytestconfig.getoption("address")
    host_addr = ["localhost", get_ports()[1]]
    assert len(host_addr) == 2
    pool = get_conn_pool(host_addr[0], host_addr[1])
    yield pool
    pool.close()

@pytest.fixture(scope="session")
def conn_pool(conn_pool_to_first_graph_service):
    return conn_pool_to_first_graph_service

@pytest.fixture(scope="class")
def session_from_first_conn_pool(conn_pool_to_first_graph_service, pytestconfig):
    user = pytestconfig.getoption("user")
    password = pytestconfig.getoption("password")
    sess = conn_pool_to_first_graph_service.get_session(user, password)
    yield sess
    sess.release()

@pytest.fixture(scope="class")
def session_from_second_conn_pool(conn_pool_to_second_graph_service, pytestconfig):
    user = pytestconfig.getoption("user")
    password = pytestconfig.getoption("password")
    sess = conn_pool_to_second_graph_service.get_session(user, password)
    yield sess
    sess.release()

@pytest.fixture(scope="class")
def session(session_from_first_conn_pool):
    return session_from_first_conn_pool

def load_csv_data_once(space: str):
    with open(SPACE_TMP_PATH, "r") as f:
        for sp in json.loads(f.readline()):
            if sp.get("name", None) == space:
                return SpaceDesc.from_json(sp)
        raise ValueError(f"Invalid space name: {space}")


@pytest.fixture(scope="session")
def load_nba_data():
    yield load_csv_data_once("nba")


@pytest.fixture(scope="session")
def load_nba_int_vid_data():
    yield load_csv_data_once("nba_int_vid")


@pytest.fixture(scope="session")
def load_student_data():
    yield load_csv_data_once("student")


# TODO(yee): Delete this when we migrate all test cases
@pytest.fixture(scope="class")
def workarround_for_class(request, pytestconfig, conn_pool,
                          session, load_nba_data, load_student_data):
    if request.cls is None:
        return

    addr = pytestconfig.getoption("address")
    if addr:
        ss = addr.split(':')
        request.cls.host = ss[0]
        request.cls.port = ss[1]
    else:
        request.cls.host = "localhost"
        request.cls.port = get_port()

    request.cls.data_dir = os.path.dirname(os.path.abspath(__file__))

    request.cls.spaces = []
    request.cls.user = pytestconfig.getoption("user")
    request.cls.password = pytestconfig.getoption("password")
    request.cls.replica_factor = pytestconfig.getoption("replica_factor")
    request.cls.partition_num = pytestconfig.getoption("partition_num")
    request.cls.check_format_str = 'result: {}, expect: {}'
    request.cls.data_loaded = False
    request.cls.client_pool = conn_pool
    request.cls.client = session
    request.cls.set_delay()
    request.cls.prepare()

    yield

    if request.cls.client is not None:
        request.cls.cleanup()
        request.cls.drop_data()
