# --coding:utf-8--
#
# Copyright (c) 2020 vesoft inc. All rights reserved.
#
# This source code is licensed under Apache 2.0 License,
# attached with Common Clause Condition 1.0, found in the LICENSES directory.

import time
import re

from tests.common.nebula_test_suite import NebulaTestSuite


class TestShowHosts(NebulaTestSuite):
    @classmethod
    def prepare(self):
        pass

    @classmethod
    def cleanup(self):
        pass

    def test_show_hosts(self):
        query = "SHOW HOSTS";
        expected_column_names = ['Host',
                                 'Port',
                                 'Status',
                                 'Leader count',
                                 'Leader distribution',
                                 'Partition distribution']
        expected_result_format = [[re.compile(r'\S+'),
                                   re.compile(r'\d+'),
                                   re.compile(r'ONLINE|OFFLINE'),
                                   re.compile(r'\d+'),
                                   re.compile(r'No valid partition|(\S+:\d+, )*\S+:\d+'),
                                   re.compile(r'No valid partition|(\S+:\d+, )*\S+:\d+')]]
        resp = self.execute(query)
        self.check_resp_succeeded(resp)
        self.check_column_names(resp, expected_column_names)
        self.search_result(resp, expected_result_format, True)
