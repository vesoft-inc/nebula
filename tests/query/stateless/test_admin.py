# --coding:utf-8--
#
# Copyright (c) 2020 vesoft inc. All rights reserved.
#
# This source code is licensed under Apache 2.0 License,
# attached with Common Clause Condition 1.0, found in the LICENSES directory.

import time

from tests.common.nebula_test_suite import NebulaTestSuite


class TestAdmin(NebulaTestSuite):
    '''
    @brief Testing suite about administration function
    '''

    @classmethod
    def prepare(self):
        pass

    @classmethod
    def cleanup(self):
        pass

    def test_config(self):
        '''
        @brief Testing about configuration query
        '''
        # List
        resp = self.client.execute('SHOW CONFIGS meta')
        self.check_resp_succeeded(resp)
        resp = self.client.execute('SHOW CONFIGS graph')
        self.check_resp_succeeded(resp)
        resp = self.client.execute('SHOW CONFIGS storage')
        self.check_resp_succeeded(resp)
        resp = self.client.execute('SHOW CONFIGS')
        self.check_resp_succeeded(resp)

        # Backup
        resp = self.client.execute('GET CONFIGS graph:minloglevel')
        self.check_resp_succeeded(resp)
        graph_minloglevel = resp.row_values(0)[4].as_int()
        resp = self.client.execute('GET CONFIGS storage:minloglevel')
        self.check_resp_succeeded(resp)
        storage_minloglevel = resp.row_values(0)[4].as_int()

        # Set
        minloglevel = 3
        resp = self.client.execute('UPDATE CONFIGS meta:minloglevel={}'.format(minloglevel))
        self.check_resp_failed(resp)
        resp = self.client.execute('UPDATE CONFIGS graph:minloglevel={}'.format(minloglevel))
        self.check_resp_succeeded(resp)
        resp = self.client.execute('UPDATE CONFIGS storage:minloglevel={}'.format(minloglevel))
        self.check_resp_succeeded(resp)

        # get
        resp = self.client.execute('GET CONFIGS meta:minloglevel')
        self.check_resp_failed(resp)
        result = [['GRAPH', 'minloglevel', 'int', 'MUTABLE', minloglevel]]
        resp = self.client.execute('GET CONFIGS graph:minloglevel')
        self.check_resp_succeeded(resp)
        self.check_result(resp, result)
        result = [['STORAGE', 'minloglevel', 'int', 'MUTABLE', minloglevel]]
        resp = self.client.execute('GET CONFIGS storage:minloglevel')
        self.check_resp_succeeded(resp)
        self.check_result(resp, result)

        # rollback
        resp = self.client.execute('UPDATE CONFIGS graph:minloglevel={}'.format(int(graph_minloglevel)))
        print('UPDATE CONFIGS graph:minloglevel={}'.format(graph_minloglevel))
        self.check_resp_succeeded(resp)
        resp = self.client.execute('UPDATE CONFIGS storage:minloglevel={}'.format(int(storage_minloglevel)))
        self.check_resp_succeeded(resp)
        time.sleep(self.delay)
