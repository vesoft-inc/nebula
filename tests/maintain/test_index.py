# --coding:utf-8--
#
# Copyright (c) 2020 vesoft inc. All rights reserved.
#
# This source code is licensed under Apache 2.0 License,
# attached with Common Clause Condition 1.0, found in the LICENSES directory.


import time

from tests.common.nebula_test_suite import NebulaTestSuite
from tests.common.nebula_test_suite import T_EMPTY, T_NULL

class TestIndex(NebulaTestSuite):

    @classmethod
    def prepare(self):
        resp = self.client.execute('CREATE SPACE index_space(partition_num=9)')
        self.check_resp_succeeded(resp)

        time.sleep(self.delay)
        resp = self.client.execute('USE index_space')
        self.check_resp_succeeded(resp)

        resp = self.client.execute('CREATE TAG tag_1(col1 string, col2 int, col3 double, col4 timestamp)')
        self.check_resp_succeeded(resp)

        resp = self.client.execute('CREATE EDGE edge_1(col1 string, col2 int, col3 double, col4 timestamp)')
        self.check_resp_succeeded(resp)
        time.sleep(self.delay)
    
    @classmethod
    def cleanup(self):
        resp = self.execute('DROP SPACE index_space;')
        self.check_resp_succeeded(resp)

    def test_tag_index(self):
        # Single Tag Single Field
        resp = self.client.execute('CREATE TAG INDEX single_tag_index ON tag_1(col2)')
        self.check_resp_succeeded(resp)

        # Duplicate Index
        resp = self.client.execute('CREATE TAG INDEX duplicate_tag_index_1 ON tag_1(col2)')
        self.check_resp_failed(resp)

        # Tag not exist
        resp = self.client.execute('CREATE TAG INDEX single_person_index ON student(name)')
        self.check_resp_failed(resp)

        # Property not exist
        resp = self.client.execute('CREATE TAG INDEX single_tag_index ON tag_1(col5)')
        self.check_resp_failed(resp)

        # Property is empty
        resp = self.client.execute('CREATE TAG INDEX single_tag_index ON tag_1()')
        self.check_resp_failed(resp)

        # Single Tag Multi Field
        resp = self.client.execute('CREATE TAG INDEX multi_tag_index ON tag_1(col2, col3)')
        self.check_resp_succeeded(resp)

        # Duplicate Index
        resp = self.client.execute('CREATE TAG INDEX duplicate_person_index ON tag_1(col2, col3)')
        self.check_resp_failed(resp)

        # Duplicate Field
        resp = self.client.execute('CREATE TAG INDEX duplicate_index ON tag_1(col2, col2)')
        self.check_resp_failed(resp)

        resp = self.client.execute('CREATE TAG INDEX disorder_tag_index ON tag_1(col3, col2)')
        self.check_resp_succeeded(resp)

    def test_edge_index(self):
        # Single Tag Single Field
        resp = self.client.execute('CREATE EDGE INDEX single_edge_index ON edge_1(col2)')
        self.check_resp_succeeded(resp)

        # Duplicate Index
        resp = self.client.execute('CREATE EDGE INDEX duplicate_edge_1_index ON edge_1(col2)')
        self.check_resp_failed(resp)

        # Edge not exist
        resp = self.client.execute('CREATE EDGE INDEX single_edge_index ON edge_1_ship(name)')
        self.check_resp_failed(resp)

        # Property not exist
        resp = self.client.execute('CREATE EDGE INDEX single_edge_index ON edge_1(startTime)')
        self.check_resp_failed(resp)

        # Property is empty
        resp = self.client.execute('CREATE EDGE INDEX single_edge_index ON edge_1()')
        self.check_resp_failed(resp)

        # Single Edge Multi Field
        resp = self.client.execute('CREATE EDGE INDEX multi_edge_1_index ON edge_1(col2, col3)')
        self.check_resp_succeeded(resp)

        # Duplicate Index
        resp = self.client.execute('CREATE EDGE INDEX duplicate_edge_1_index ON edge_1(col2, col3)')
        self.check_resp_failed(resp)

        # Duplicate Field
        resp = self.client.execute('CREATE EDGE INDEX duplicate_index ON edge_1(col2, col2)')
        self.check_resp_failed(resp)

        resp = self.client.execute('CREATE EDGE INDEX disorder_edge_1_index ON edge_1(col3, col2)')
        self.check_resp_succeeded(resp)
