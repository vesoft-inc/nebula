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
import sys, time
import logging
import random
import shutil
import socket
from contextlib import closing
from _pytest.main import ExitCode
from time import localtime, strftime
from pathlib import Path
from distutils.dir_util import copy_tree
import subprocess
from pathlib import Path
import glob
import signal


TEST_DIR = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, TEST_DIR)

TEST_LOGS_DIR = os.getenv('NEBULA_TEST_LOGS_DIR')
if TEST_LOGS_DIR is None or TEST_LOGS_DIR == "":
    TEST_LOGS_DIR = os.environ['HOME']

NEBULA_BUILD_DIR=os.getenv('NEBULA_BUILD_DIR')
if NEBULA_BUILD_DIR is None:
    NEBULA_BUILD_DIR = str(Path(TEST_DIR).parent)
NEBULA_SOURCE_DIR=os.getenv('NEBULA_SOURCE_DIR')
if NEBULA_SOURCE_DIR is None:
    NEBULA_SOURCE_DIR = str(Path(TEST_DIR).parent)

NEBULA_DATA_DIR = os.getenv('NEBULA_DATA_DIR')
if NEBULA_DATA_DIR is None or NEBULA_DATA_DIR == "":
    NEBULA_DATA_DIR = TEST_DIR + "/data"

NEBULA_START_COMMAND_FORMAT="bin/nebula-{} --flagfile conf/nebula-{}.conf {}"

RESULT_DIR = os.path.join(TEST_LOGS_DIR, 'results')
LOGGING_ARGS = {
    '--html': 'TEST-nebula-{0}.html'
}

LOG_FORMAT = "-- %(asctime)s %(levelname)-8s %(threadName)s: %(message)s"

DOCKER_GRAPHD_DIGESTS = os.getenv('NEBULA_GRAPHD_DIGESTS')
if DOCKER_GRAPHD_DIGESTS is None:
    DOCKER_GRAPHD_DIGESTS = 0
DOCKER_METAD_DIGESTS = os.getenv('NEBULA_METAD_DIGESTS')
if DOCKER_METAD_DIGESTS is None:
    DOCKER_METAD_DIGESTS = 0
DOCKER_STORAGED_DIGESTS = os.getenv('NEBULA_STORAGED_DIGESTS')
if DOCKER_STORAGED_DIGESTS is None:
    DOCKER_STORAGED_DIGESTS = 0

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
            pytest_exit_code = pytest.main(args, plugins=[plugin])
        except:
            sys.stderr.write(
                "Unexpected exception with pytest {0}".format(args))
            raise

        if '--collect-only' in args:
            for test in plugin.tests_collected:
                print(test)

        self.total_executed += len(plugin.tests_executed)

def find_free_port():
    ports = []
    for i in range(0, 3):
        with closing(socket.socket(socket.AF_INET, socket.SOCK_STREAM)) as s:
            s.bind(('', 0))
            s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            ports.append(s.getsockname()[1])
    return ports

def copy_nebula_conf(nebula_test_dir):
    shutil.copy(NEBULA_BUILD_DIR+'/bin/nebula-graphd', nebula_test_dir + '/bin/')
    shutil.copy(NEBULA_SOURCE_DIR+'/modules/storage/src/daemons/_build/nebula-storaged', nebula_test_dir + '/bin/')
    shutil.copy(NEBULA_SOURCE_DIR+'/modules/storage/src/daemons/_build/nebula-metad', nebula_test_dir + '/bin/')
    shutil.copy(NEBULA_SOURCE_DIR+'/conf/nebula-graphd.conf.default', nebula_test_dir+'/conf/nebula-graphd.conf')
    shutil.copy(NEBULA_SOURCE_DIR+'/modules/storage/conf/nebula-metad.conf.default', nebula_test_dir + '/conf/nebula-metad.conf')
    shutil.copy(NEBULA_SOURCE_DIR+'/modules/storage/conf/nebula-storaged.conf.default', nebula_test_dir + '/conf/nebula-storaged.conf')

def installNebula():
    test_dir = "/tmp/nebula-" + str(random.randrange(1000000, 100000000))
    os.mkdir(test_dir)
    print("created directory:" + test_dir)
    os.chdir(test_dir)
    nebula_install_file = ['logs', 'bin', 'conf', 'data', 'pids', 'scripts']
    for f in nebula_install_file:
        os.mkdir(test_dir+'/'+f)
    copy_nebula_conf(test_dir)
    return test_dir

def formatNebulaCommand(name, meta_port, ports):
    param_format = "--meta_server_addrs={} --port={} --ws_http_port={} --ws_h2_port={} -v=4"
    param = param_format.format("127.0.0.1:" + str(meta_port), ports[0], ports[1], ports[2])
    command=NEBULA_START_COMMAND_FORMAT.format(name, name, param)
    return command

def startNebula(nebula_test_dir):
    os.chdir(nebula_test_dir)

    pids = {}
    metad_ports = find_free_port()
    command = ''
    graph_port = 0
    for server_name in ['metad', 'storaged', 'graphd']:
        ports = []
        if server_name != 'metad':
            ports = find_free_port()
        else:
            ports = metad_ports
        command = formatNebulaCommand(server_name, metad_ports[0], ports)
        print("exec: " + command)
        p = subprocess.Popen([command], shell=True, stdout=subprocess.PIPE)
        p.wait()
        if p.returncode != 0:
            print("error: " + p.communicate()[0])
        else:
            graph_port = ports[0]

    #wait nebula start
    time.sleep(8)
    for pf in glob.glob(nebula_test_dir+'/pids/*.pid'):
        with open(pf) as f:
            pid = int(f.readline())
            pids[f.name] = pid

    return graph_port, pids

def stopNebula(pids, test_dir):
    print("try to stop nebula services...")
    for p in pids:
        try:
            os.kill(pids[p], signal.SIGTERM)
        except OSError as err:
            print("nebula stop " + p + " failed: " + str(err))
    time.sleep(3)
    if pytest.cmdline.rm_dir.lower() == 'true':
        shutil.rmtree(test_dir)

if __name__ == "__main__":
    # If the user is just asking for --help, just print the help test and then exit.
    executor = TestExecutor()
    if '-h' in sys.argv[1:] or '--help' in sys.argv[1:]:
        executor.run_tests(sys.argv[1:])
        sys.exit(0)
    stop_nebula = True
    pids = []
    test_dir = ''
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
            args.extend([arg, os.path.join(RESULT_DIR, log.format(current_time))])

        args.extend(list(commandline_args))

        if '--address' not in args:
            test_dir = installNebula()
            port,pids = startNebula(test_dir)
            args.extend(['--address', '127.0.0.1:'+str(port)])
        else:
            stop_nebula = False
        print("Running TestExecutor with args: {} ".format(args))
        executor.run_tests(args)
    finally:
        if stop_nebula and pytest.cmdline.stop_nebula.lower() == 'true':
            stopNebula(pids, test_dir)

    if executor.total_executed == 0:
        sys.exit(1)
    if executor.tests_failed:
        sys.exit(1)
