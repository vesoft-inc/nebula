# --coding:utf-8--
#
# Copyright (c) 2020 vesoft inc. All rights reserved.
#
# This source code is licensed under Apache 2.0 License,
# attached with Common Clause Condition 1.0, found in the LICENSES directory.

import re
from tests.common.nebula_test_suite import NebulaTestSuite

class TestSnapshot(NebulaTestSuite):
    def test_all(self):
        resp = self.client.execute('CREATE SNAPSHOT;')
        self.check_resp_succeeded(resp)

        # show snapshots
        resp = self.client.execute('SHOW SNAPSHOTS;')
        self.check_resp_succeeded(resp)
        expected_col_names = ["Name", "Status", "Hosts"]
        expected_col_values = [[re.compile(r'SNAPSHOT_.*'),
                                re.compile(r'INVALID|VALID'),
                                re.compile(r'127.0.0.1:.*')]]

        self.check_column_names(resp, expected_col_names)
        self.check_result(resp, expected_col_values, is_regex=True)
        snapshot_name = resp.row_values(0)[0].as_string()

        # drop snapshot
        resp = self.client.execute('DROP SNAPSHOT {}'.format(snapshot_name))
        self.check_resp_succeeded(resp)

        # show snapshots
        resp = self.client.execute('SHOW SNAPSHOTS;')
        self.check_resp_succeeded(resp)
        expected_col_names = ["Name", "Status", "Hosts"]

        self.check_column_names(resp, expected_col_names)
        self.check_empty_result(resp)

        # insert data and get the data
        self.use_nba()
        resp = self.client.execute('INSERT VERTEX player(name, age) VALUES "Tom":("Tom", 10)')
        self.check_resp_succeeded(resp)
        resp = self.client.execute('FETCH PROP ON player "Tom"')
        self.check_resp_succeeded(resp)
        expected_value = [['Tom', 'Tom', 10]]
        self.check_result(resp, expected_value)
