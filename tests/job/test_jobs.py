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
        def check_jobs_resp_obj(resp_row, job_name):
            assert resp_row[1].as_string() == job_name
            assert resp_row[2].is_string()
            assert resp_row[3].is_datetime()
            assert resp_row[4].is_datetime()

        resp = self.client.execute('CREATE SPACE IF NOT EXISTS space_for_jobs(partition_num=9, replica_factor=1, vid_type=FIXED_STRING(20));'
                                   'USE space_for_jobs;')
        self.check_resp_succeeded(resp)

        resp = self.client.execute('SUBMIT JOB COMPACT;')
        self.check_resp_succeeded(resp)
        expect_col_names = ['New Job Id']
        self.check_column_names(resp, expect_col_names)
        expect_values = [[re.compile(r'\d+')]]
        self.check_result(resp, expect_values, is_regex=True)
        time.sleep(1)

        resp = self.client.execute('SUBMIT JOB FLUSH;')
        self.check_resp_succeeded(resp)
        expect_col_names = ['New Job Id']
        self.check_column_names(resp, expect_col_names)
        expect_values = [[re.compile(r'\d+')]]
        self.check_result(resp, expect_values, is_regex=True)
        time.sleep(1)

        resp = self.client.execute('SUBMIT JOB STATS;')
        self.check_resp_succeeded(resp)
        expect_col_names = ['New Job Id']
        self.check_column_names(resp, expect_col_names)
        expect_values = [[re.compile(r'\d+')]]
        self.check_result(resp, expect_values, is_regex=True)

        time.sleep(10)
        resp = self.client.execute('SHOW JOBS;')
        self.check_resp_succeeded(resp)
        expect_col_names = ['Job Id', 'Command', 'Status', 'Start Time', 'Stop Time']
        self.check_column_names(resp, expect_col_names)
        check_jobs_resp_obj(resp.row_values(0), 'STATS')
        check_jobs_resp_obj(resp.row_values(1), 'FLUSH')
        check_jobs_resp_obj(resp.row_values(2), 'COMPACT')

        job_id = resp.row_values(0)[0].as_int()
        resp = self.client.execute('SHOW JOB {};'.format(job_id))
        self.check_resp_succeeded(resp)
        expect_col_names = ['Job Id(TaskId)', 'Command(Dest)', 'Status', 'Start Time', 'Stop Time']
        check_jobs_resp_obj(resp.row_values(0), 'STATS')

        job_id = resp.row_values(0)[0].as_int()
        stop_job_resp = self.client.execute('STOP JOB {};'.format(job_id))
        if resp.row_values(0)[2].as_string() == "FINISHED":
            # Executin error if the job is finished
            self.check_resp_failed(stop_job_resp, ttypes.ErrorCode.E_EXECUTION_ERROR)
        else:
            self.check_resp_succeeded(stop_job_resp)

        # This is skipped becuase it is hard to simulate the situation
        # resp = self.client.execute('RECOVER JOB;')
        # self.check_resp_succeeded(resp)
        # expect_col_names = ['Recovered job num']
        # self.check_column_names(resp, expect_col_names)
        # expect_values = [[0]]
        # self.check_result(resp, expect_values)
