# --coding:utf-8--
#
# Copyright (c) 2020 vesoft inc. All rights reserved.
#
# This source code is licensed under Apache 2.0 License,
# attached with Common Clause Condition 1.0, found in the LICENSES directory.

import re
import sys
import time

from graph import ttypes

from nebula_test_common.nebula_test_suite import NebulaTestSuite


class TestFindAndMatch(NebulaTestSuite):
    @classmethod
    def prepare(self):
        pass

    def test_match(self):
        try:
            cmd = "match"
            resp = self.execute_query(cmd)
            self.check_resp_failed(resp)
        except Exception as e:
            if not 'error: Does not support' in str(e):
                assert False

    @classmethod
    def cleanup(self):
        pass
