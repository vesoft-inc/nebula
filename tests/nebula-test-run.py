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
from time import localtime, strftime
from pathlib import Path

TEST_DIR = os.path.dirname(os.path.abspath(__file__))
NEBULA_HOME = TEST_DIR + '/../'
sys.path.insert(0, NEBULA_HOME)
sys.path.insert(0, TEST_DIR)

from tests.common.nebula_service import NebulaService
from tests.common.load_test_data import LoadGlobalData
from tests.common.configs import init_configs

TEST_LOGS_DIR = os.getenv('NEBULA_TEST_LOGS_DIR')
if TEST_LOGS_DIR is None or TEST_LOGS_DIR == "":
    TEST_LOGS_DIR = os.environ['HOME']

NEBULA_BUILD_DIR = os.getenv('NEBULA_BUILD_DIR')
if NEBULA_BUILD_DIR is None:
    NEBULA_BUILD_DIR = str(Path(TEST_DIR).parent)
NEBULA_SOURCE_DIR = os.getenv('NEBULA_SOURCE_DIR')
if NEBULA_SOURCE_DIR is None:
    NEBULA_SOURCE_DIR = str(Path(TEST_DIR).parent)

RESULT_DIR = os.path.join(TEST_LOGS_DIR, 'results')
LOGGING_ARGS = {'--html': 'TEST-nebula-{0}.html'}


class TestExecutor(object):
    def __init__(self, exit_on_error=True):
        self._exit_on_error = exit_on_error
        self.tests_failed = False
        self.total_executed = 0

    def run_tests(self, args):
        # plugin = NebulaTestPlugin(TEST_DIR)

        error_code = 0
        try:
            error_code = pytest.main(args)
        except Exception:
            sys.stderr.write(
                "Unexpected exception with pytest {0}".format(args))
            error_code = 1

        if '--collect-only' in args:
            for test in plugin.tests_collected:
                print(test)

        #self.total_executed += len(plugin.tests_executed)
        return error_code

if __name__ == "__main__":
    # If the user is just asking for --help, just print the help test and then exit.
    executor = TestExecutor()
    if '-h' in sys.argv[1:] or '--help' in sys.argv[1:]:
        executor.run_tests(sys.argv[1:])
        sys.exit(0)
    nebula_svc = NebulaService(NEBULA_BUILD_DIR, NEBULA_SOURCE_DIR)
    stop_nebula = True
    error_code = 0
    configs = None
    try:
        # Init args
        parser = init_configs()
        (configs, opts) = parser.parse_args()

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

        nebula_ip = ''
        nebula_port = 0
        if len(configs.address) == 0:
            nebula_svc.install()
            port = nebula_svc.start(configs.debug_log)
            args.extend(['--address', '127.0.0.1:' + str(port)])
            nebula_ip = '127.0.0.1'
            nebula_port = port
        else:
            stop_nebula = False
            addr = configs.address.split(':')
            nebula_ip = addr[0]
            nebula_port = int(addr[1])

        if len(configs.data_dir) == 0:
            args.extend(['--data_dir', TEST_DIR])
        print("Running TestExecutor with args: {} ".format(args))

        # load nba data
        load_data = LoadGlobalData(TEST_DIR, nebula_ip, nebula_port, configs.user, configs.password)
        load_data.load_all_test_data()

        # Switch to your $src_dir/tests
        os.chdir(TEST_DIR)
        error_code = executor.run_tests(args)
        load_data.drop_data()
    except Exception as x:
        print('\033[31m' + str(x) + '\033[0m')

    finally:
        if stop_nebula and configs.stop_nebula.lower() == 'true':
            nebula_svc.stop(configs.rm_dir.lower() == 'true')

    sys.exit(error_code)
