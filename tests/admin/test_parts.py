# --coding:utf-8--
#
# Copyright (c) 2020 vesoft inc. All rights reserved.
#
# This source code is licensed under Apache 2.0 License,
# attached with Common Clause Condition 1.0, found in the LICENSES directory.

import time
import re

from tests.common.nebula_test_suite import NebulaTestSuite


class TestParts(NebulaTestSuite):

    @classmethod
    def prepare(self):
        resp = self.client.execute('CREATE SPACE space_show_parts(partition_num=5); '
                                   'USE space_show_parts;')
        self.check_resp_succeeded(resp)

        # Wait for leader info
        time.sleep(self.delay)

    @classmethod
    def cleanup(self):
        resp = self.client.execute('DROP SPACE space_show_parts;')
        self.check_resp_succeeded(resp)

    def test_part(self):
        # All
        resp = self.client.execute_query('SHOW PARTS')
        self.check_resp_succeeded(resp)
        expected_col_names = ["Partition ID", "Leader", "Peers", "Losts"]
        self.check_column_names(resp, expected_col_names)
        expected_result = [[re.compile(r'1'), re.compile(r'127.0.0.1:.*|^$'), re.compile(r'127.0.0.1:.*'), re.compile(r'')],
                           [re.compile(r'2'), re.compile(r'127.0.0.1:.*|^$'), re.compile(r'127.0.0.1:.*'), re.compile(r'')],
                           [re.compile(r'3'), re.compile(r'127.0.0.1:.*|^$'), re.compile(r'127.0.0.1:.*'), re.compile(r'')],
                           [re.compile(r'4'), re.compile(r'127.0.0.1:.*|^$'), re.compile(r'127.0.0.1:.*'), re.compile(r'')],
                           [re.compile(r'5'), re.compile(r'127.0.0.1:.*|^$'), re.compile(r'127.0.0.1:.*'), re.compile(r'')]]
        self.check_result(resp, expected_result, is_regex = True)

        # Specify the part id
        resp = self.client.execute_query('SHOW PART 3')
        self.check_resp_succeeded(resp)
        expected_col_names = ["Partition ID", "Leader", "Peers", "Losts"]
        self.check_column_names(resp, expected_col_names)
        expected_result = [[re.compile(r'3'), re.compile(r'127.0.0.1:.*|^$'), re.compile(r'127.0.0.1:.*'), re.compile(r'')]]
        self.check_result(resp, expected_result, is_regex = True)

        # Not exist part id
        resp = self.client.execute_query('SHOW PART 10')
        self.check_resp_succeeded(resp)
        expected_col_names = ["Partition ID", "Leader", "Peers", "Losts"]
        self.check_column_names(resp, expected_col_names)
        expected_result = []
        self.check_result(resp, expected_result)
