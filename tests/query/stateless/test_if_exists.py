# --coding:utf-8--
#
# Copyright (c) 2020 vesoft inc. All rights reserved.
#
# This source code is licensed under Apache 2.0 License,
# attached with Common Clause Condition 1.0, found in the LICENSES directory.

import re
import sys
import pytest
import time

from nebula2.graph import ttypes

from tests.common.nebula_test_suite import NebulaTestSuite

class TestDropSpaceIfExists(NebulaTestSuite):
    @classmethod
    def prepare(self):
        print("Nothing to Prepare")

    # issue 1461
    def test_drop_space(self):
        cmd = 'drop space IF EXISTS shakespaces'
        resp = self.execute(cmd)
        self.check_resp_succeeded(resp)

        resp = self.execute('CREATE SPACE shakespaces(partition_num=1024, vid_type=FIXED_STRING(8))')
        self.check_resp_succeeded(resp)

        resp = self.execute('CREATE SPACE IF NOT EXISTS shakespaces(partition_num=1024, vid_type=FIXED_STRING(8))')
        self.check_resp_succeeded(resp)

        cmd = 'drop space shakespaces'
        resp = self.execute(cmd)
        self.check_resp_succeeded(resp)

        cmd = 'drop space IF EXISTS shakespaces'
        resp = self.execute(cmd)
        self.check_resp_succeeded(resp)

        resp = self.execute('CREATE SPACE IF NOT EXISTS shakespaces(partition_num=1024, vid_type=FIXED_STRING(8))')
        self.check_resp_succeeded(resp)

        time.sleep(self.delay)
        resp = self.execute('use shakespaces')
        self.check_resp_succeeded(resp)

        resp = self.execute('CREATE TAG person(name string, age int, gender string);')
        self.check_resp_succeeded(resp)

        resp = self.execute('CREATE TAG IF NOT EXISTS person(name string, age int, gender string);')
        self.check_resp_succeeded(resp)

        resp = self.execute('CREATE EDGE IF NOT EXISTS is_schoolmate(start_year int, end_year int);')
        self.check_resp_succeeded(resp)

        resp = self.execute('DROP EDGE is_schoolmate')
        self.check_resp_succeeded(resp)

        resp = self.execute('CREATE EDGE is_schoolmate(start_year int, end_year int);')
        self.check_resp_succeeded(resp)

        resp = self.execute('DROP TAG person')
        self.check_resp_succeeded(resp)

        resp = self.execute('DROP TAG IF EXISTS person')
        self.check_resp_succeeded(resp)

        resp = self.execute('DROP EDGE is_schoolmate')
        self.check_resp_succeeded(resp)

        resp = self.execute('DROP EDGE IF EXISTS is_schoolmate')
        self.check_resp_succeeded(resp)

        resp = self.execute('CREATE TAG person(name string, age int, gender string);')
        self.check_resp_succeeded(resp)

        resp = self.execute('CREATE TAG if not exists person(name string, age int, gender string);')
        self.check_resp_succeeded(resp)

        resp = self.execute('CREATE EDGE is_schoolmate(start_year int, end_year int);')
        self.check_resp_succeeded(resp)

        resp = self.execute('CREATE EDGE if not exists is_schoolmate(start_year int, end_year int);')
        self.check_resp_succeeded(resp)

        cmd = 'drop space shakespaces'
        resp = self.execute(cmd)
        self.check_resp_succeeded(resp)

    @classmethod
    def cleanup(self):
        print("Nothing to cleanup")
