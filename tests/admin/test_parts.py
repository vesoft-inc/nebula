# --coding:utf-8--
#
# Copyright (c) 2020 vesoft inc. All rights reserved.
#
# This source code is licensed under Apache 2.0 License,
# attached with Common Clause Condition 1.0, found in the LICENSES directory.

import time
import re

from tests.common.nebula_test_suite import NebulaTestSuite

leader_pattern = re.compile(r'127.0.0.1:.*|^$')
peers_pattern = re.compile(r'127.0.0.1:.*')
losts_pattern = re.compile(r'')


class TestParts(NebulaTestSuite):

    @classmethod
    def prepare(self):
        resp = self.client.execute('CREATE SPACE space_show_parts(partition_num=5, vid_type=FIXED_STRING(8));'
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
        resp = self.client.execute('SHOW PARTS')
        self.check_resp_succeeded(resp)
        expected_col_names = ["Partition ID", "Leader", "Peers", "Losts"]
        self.check_column_names(resp, expected_col_names)
        expected_result = [
            [re.compile(r'{}'.format(i)),
             leader_pattern,
             peers_pattern,
             losts_pattern]
            for i in range(1, 6)
        ]
        self.check_result(resp, expected_result, is_regex=True)


        # Specify the part id
        resp = self.client.execute('SHOW PART 3')
        self.check_resp_succeeded(resp)
        expected_col_names = ["Partition ID", "Leader", "Peers", "Losts"]
        self.check_column_names(resp, expected_col_names)
        expected_result = [[re.compile(r'3'),
                            leader_pattern,
                            peers_pattern,
                            losts_pattern]]
        self.check_result(resp, expected_result, is_regex=True)

        # Not exist part id
        resp = self.client.execute('SHOW PART 10')
        self.check_resp_failed(resp)
