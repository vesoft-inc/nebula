# --coding:utf-8--
#
# Copyright (c) 2020 vesoft inc. All rights reserved.
#
# This source code is licensed under Apache 2.0 License,
# attached with Common Clause Condition 1.0, found in the LICENSES directory.

import time

from tests.common.nebula_test_suite import NebulaTestSuite

class TestListener(NebulaTestSuite):
    def test_listener(self):
        resp = self.client.execute('SHOW HOSTS')
        self.check_resp_succeeded(resp)
        assert not resp.is_empty()
        storage_ip = resp.row_values(0)[0].as_string()
        storage_port = resp.row_values(0)[1].as_int()

        self.use_nba()

        # Add on same as storage host
        resp = self.client.execute('ADD LISTENER ELASTICSEARCH {}:{}'.format(storage_ip, storage_port))
        self.check_resp_failed(resp)

        # Add non-existen host
        resp = self.client.execute('ADD LISTENER ELASTICSEARCH 127.0.0.1:8899')
        self.check_resp_succeeded(resp)

        # Show listener
        resp = self.client.execute('SHOW LISTENER;')
        self.check_resp_succeeded(resp)
        self.search_result(resp, [[1, "ELASTICSEARCH", '"127.0.0.1":8899', "OFFLINE"]])

        # Remove listener
        resp = self.client.execute('REMOVE LISTENER ELASTICSEARCH;')
        self.check_resp_succeeded(resp)

        # CHECK listener
        resp = self.client.execute('SHOW LISTENER;')
        self.check_resp_succeeded(resp)
        self.check_result(resp, [])
