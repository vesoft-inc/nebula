# --coding:utf-8--
#
# Copyright (c) 2020 vesoft inc. All rights reserved.
#
# This source code is licensed under Apache 2.0 License,
# attached with Common Clause Condition 1.0, found in the LICENSES directory.

import re
import time

from nebula2.common import ttypes
from tests.common.nebula_test_suite import NebulaTestSuite

class TestJobs(NebulaTestSuite):
    def test_failed(self):
        # submit without space
        resp = self.client.execute('SUBMIT JOB COMPACT;')
        self.check_resp_failed(resp, ttypes.ErrorCode.E_SEMANTIC_ERROR)
        # show one not exists
        resp = self.client.execute('SHOW JOB 233;')
        self.check_resp_failed(resp, ttypes.ErrorCode.E_EXECUTION_ERROR)
        # stop one not exists
        resp = self.client.execute('STOP JOB 233;')
        self.check_resp_failed(resp, ttypes.ErrorCode.E_EXECUTION_ERROR)

    def test_succeeded(self):
        resp = self.client.execute('CREATE SPACE space_for_jobs(partition_num=9, replica_factor=1);'
                                   'USE space_for_jobs;')
        self.check_resp_succeeded(resp)

        resp = self.client.execute('SUBMIT JOB COMPACT;')
        self.check_resp_succeeded(resp)
        expect_col_names = ['New Job Id']
        self.check_column_names(resp, expect_col_names)
        expect_values = [[re.compile(r'\d+')]]
        self.check_result(resp, expect_values, is_regex=True)

        resp = self.client.execute('SUBMIT JOB FLUSH;')
        self.check_resp_succeeded(resp)
        expect_col_names = ['New Job Id']
        self.check_column_names(resp, expect_col_names)
        expect_values = [[re.compile(r'\d+')]]
        self.check_result(resp, expect_values, is_regex=True)

        resp = self.client.execute('SUBMIT JOB STATS;')
        self.check_resp_succeeded(resp)
        expect_col_names = ['New Job Id']
        self.check_column_names(resp, expect_col_names)
        expect_values = [[re.compile(r'\d+')]]
        self.check_result(resp, expect_values, is_regex=True)

        time.sleep(3)
        resp = self.client.execute('SHOW JOBS;')
        self.check_resp_succeeded(resp)
        expect_col_names = ['Job Id', 'Command', 'Status', 'Start Time', 'Stop Time']
        self.check_column_names(resp, expect_col_names)
        expect_values = [[re.compile(r'\d+'), re.compile(r'COMPACT'), re.compile(r'\S+'), re.compile(r'\d+'), re.compile(r'\d+')],
                         [re.compile(r'\d+'), re.compile(r'FLUSH'), re.compile(r'\S+'), re.compile(r'\d+'), re.compile(r'\d+')],
                         [re.compile(r'\d+'), re.compile(r'STATS'), re.compile(r'\S+'), re.compile(r'\d+'), re.compile(r'\d+')]]
        self.search_result(resp, expect_values, is_regex=True)

        job_id = resp.row_values(0)[0].as_int()
        resp = self.client.execute('SHOW JOB {};'.format(job_id))
        self.check_resp_succeeded(resp)
        expect_col_names = ['Job Id(TaskId)', 'Command(Dest)', 'Status', 'Start Time', 'Stop Time']
        self.check_column_names(resp, expect_col_names)
        expect_values = [[re.compile(r'\d+'), re.compile(r'COMPACT|FLUSH|STATS'), re.compile(r'\S+'), re.compile(r'\d+'), re.compile(r'\d+')]]
        self.search_result(resp, expect_values, is_regex=True)

        job_id = resp.row_values(0)[0].as_int()
        resp = self.client.execute('STOP JOB {};'.format(job_id))
        self.check_resp_succeeded(resp)
        expect_col_names = ['Result']
        self.check_column_names(resp, expect_col_names)
        expect_values = [['Job stopped']]
        self.check_result(resp, expect_values)

        resp = self.client.execute('SHOW JOBS;')
        self.check_resp_succeeded(resp)
        expect_col_names = ['Job Id', 'Command', 'Status', 'Start Time', 'Stop Time']
        self.check_column_names(resp, expect_col_names)
        expect_values = [[re.compile(r'\d+'), re.compile(r'COMPACT'), re.compile(r'\S+'), re.compile(r'\d+'), re.compile(r'\d+')],
                         [re.compile(r'\d+'), re.compile(r'FLUSH'), re.compile(r'\S+'), re.compile(r'\d+'), re.compile(r'\d+')],
                         [re.compile(r'\d+'), re.compile(r'STATS'), re.compile(r'\S+'), re.compile(r'\d+'), re.compile(r'\d+')]]
        self.search_result(resp, expect_values, is_regex=True)

        resp = self.client.execute('RECOVER JOB;')
        self.check_resp_succeeded(resp)
        expect_col_names = ['Recovered job num']
        self.check_column_names(resp, expect_col_names)
        expect_values = [[0]]
        self.check_result(resp, expect_values)
