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

        # Insert some data
        resp = self.client.execute('INSERT VERTEX tag_1(col1, col2, col3, col4) VALUES \'101\':(\'Tom\', 18, 35.4, \'2010-09-01 08:00:00\')')
        self.check_resp_succeeded(resp)

        resp = self.client.execute('INSERT VERTEX tag_1(col1, col2, col3, col4) VALUES \'102\':(\'Jerry\', 22, 38.4, \'2011-09-01 08:00:00\')')
        self.check_resp_succeeded(resp)

        resp = self.client.execute('INSERT VERTEX tag_1(col1, col2, col3, col4) VALUES \'103\':(\'Bob\', 19, 36.4, \'2010-09-01 12:00:00\')')
        self.check_resp_succeeded(resp)

        # # Rebuild Tag Index
        # resp = self.client.execute_query('REBUILD TAG INDEX single_tag_index')
        # self.check_resp_succeeded(resp)

        # resp = self.client.execute_query('REBUILD TAG INDEX multi_tag_index')
        # self.check_resp_succeeded(resp)

        # resp = self.client.execute_query('REBUILD TAG INDEX disorder_tag_index')
        # self.check_resp_succeeded(resp)

        # resp = self.client.execute_query('REBUILD TAG INDEX non_existent_tag_index')
        # self.check_resp.succeeded(resp) # need to check if index exists in validator in future

        # Show Tag Index Status
        # resp = self.client.execute_query('SHOW TAG INDEX STATUS')
        # self.check_resp_succeeded(resp)
        # self.check_out_of_order_result(resp, [['single_tag_index', 'SUCCEEDED'], ['multi_tag_index', 'SUCCEEDED'], ['disorder_tag_index', 'SUCCEEDED']])

        # Describe Tag Index
        resp = self.client.execute_query('DESC TAG INDEX single_tag_index')
        self.check_resp_succeeded(resp)
        expect = [['col2', 'int64']]
        self.check_result(resp, expect)

        resp = self.client.execute_query('DESC TAG INDEX multi_tag_index')
        self.check_resp_succeeded(resp)
        expect = [['col2', 'int64'],
                  ['col3', 'double']]
        self.check_result(resp, expect)

        resp = self.client.execute_query('DESC TAG INDEX disorder_tag_index')
        self.check_resp_succeeded(resp)
        expect = [['col3', 'double'],
                  ['col2', 'int64']]
        self.check_result(resp, expect)

        resp = self.client.execute_query('DESC TAG INDEX non_existent_tag_index')
        self.check_resp_failed(resp)

        # Show Create Tag Index
        resp = self.client.execute_query('SHOW CREATE TAG INDEX single_tag_index')
        self.check_resp_succeeded(resp)
        expect = [['single_tag_index', 'CREATE TAG INDEX `single_tag_index` ON `tag_1` (\n `col2`\n)']]
        self.check_result(resp, expect)

        resp = self.client.execute_query('SHOW CREATE TAG INDEX multi_tag_index')
        self.check_resp_succeeded(resp)
        expect = [['multi_tag_index', 'CREATE TAG INDEX `multi_tag_index` ON `tag_1` (\n `col2`,\n `col3`\n)']]
        self.check_result(resp, expect)
        # Check if show create tag index works well
        resp = self.client.execute_query('DROP TAG INDEX multi_tag_index')
        self.check_resp_succeeded(resp)
        resp = self.client.execute_query(expect[0][1])
        self.check_resp_succeeded(resp)

        resp = self.client.execute_query('SHOW CREATE TAG INDEX disorder_tag_index')
        self.check_resp_succeeded(resp)
        expect = [['disorder_tag_index', 'CREATE TAG INDEX `disorder_tag_index` ON `tag_1` (\n `col3`,\n `col2`\n)']]
        self.check_result(resp, expect)
        # Check if show create tag index works well
        resp = self.client.execute_query('DROP TAG INDEX disorder_tag_index')
        self.check_resp_succeeded(resp)
        resp = self.client.execute_query(expect[0][1])
        self.check_resp_succeeded(resp)

        resp = self.client.execute_query('SHOW CREATE TAG INDEX non_existent_tag_index')
        self.check_resp_failed(resp)

        # Show Tag Indexes
        resp = self.client.execute_query('SHOW TAG INDEXES')
        self.check_resp_succeeded(resp)
        self.check_out_of_order_result(resp, [['single_tag_index'], ['multi_tag_index'], ['disorder_tag_index']])

        # Drop Tag Index
        resp = self.client.execute_query('DROP TAG INDEX single_tag_index')
        self.check_resp_succeeded(resp)
        # Check if the index is truly dropped
        resp = self.client.execute_query('DESC TAG INDEX single_tag_index')
        self.check_resp_failed(resp)

        resp = self.client.execute_query('DROP TAG INDEX multi_tag_index')
        self.check_resp_succeeded(resp)
        resp = self.client.execute_query('DESC TAG INDEX multi_tag_index')
        self.check_resp_failed(resp)

        resp = self.client.execute_query('DROP TAG INDEX disorder_tag_index')
        self.check_resp_succeeded(resp)
        resp = self.client.execute_query('DESC TAG INDEX disorder_tag_index')
        self.check_resp_failed(resp)

        resp = self.client.execute_query('DROP TAG INDEX non_existent_tag_index')
        self.check_resp_failed(resp)

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

        # Insert some data
        resp = self.client.execute('INSERT EDGE edge_1(col1, col2, col3, col4) VALUES \'101\'->\'102\':(\'Red\', 81, 45.3, \'2010-09-01 08:00:00\')')
        self.check_resp_succeeded(resp)

        resp = self.client.execute('INSERT EDGE edge_1(col1, col2, col3, col4) VALUES \'102\'->\'103\':(\'Yellow\', 22, 423.8, \'2011-09-01 08:00:00\')')
        self.check_resp_succeeded(resp)

        resp = self.client.execute('INSERT EDGE edge_1(col1, col2, col3, col4) VALUES \'103\'->\'101\':(\'Blue\', 91, 43.1, \'2010-09-01 12:00:00\')')
        self.check_resp_succeeded(resp)

        # # Rebuild Edge Index
        # resp = self.client.execute_query('REBUILD EDGE INDEX single_edge_index')
        # self.check_resp_succeeded(resp)

        # resp = self.client.execute_query('REBUILD EDGE INDEX multi_edge_1_index')
        # self.check_resp_succeeded(resp)

        # resp = self.client.execute_query('REBUILD EDGE INDEX disorder_edge_1_index')
        # self.check_resp_succeeded(resp)

        # resp = self.client.execute_query('REBUILD EDGE INDEX non_existent_edge_index')
        # self.check_resp.failed(resp)

        # Show Edge Index Status
        # resp = self.client.execute_query('SHOW EDGE INDEX STATUS')
        # self.check_resp_succeeded(resp)
        # self.check_out_of_order_result(resp, [['single_edge_index', 'SUCCEEDED'], ['multi_edge_1_index', 'SUCCEEDED'], ['disorder_edge_1_index', 'SUCCEEDED']])

        # Describe Edge Index
        resp = self.client.execute_query('DESC EDGE INDEX single_edge_index')
        self.check_resp_succeeded(resp)
        expect = [['col2', 'int64']]
        self.check_result(resp, expect)

        resp = self.client.execute_query('DESC EDGE INDEX multi_edge_1_index')
        self.check_resp_succeeded(resp)
        expect = [['col2', 'int64'],
                  ['col3', 'double']]
        self.check_result(resp, expect)

        resp = self.client.execute_query('DESC EDGE INDEX disorder_edge_1_index')
        self.check_resp_succeeded(resp)
        expect = [['col3', 'double'],
                  ['col2', 'int64']]
        self.check_result(resp, expect)

        resp = self.client.execute_query('DESC EDGE INDEX non_existent_edge_index')
        self.check_resp_failed(resp)

        # Show Create Edge Index
        resp = self.client.execute_query('SHOW CREATE EDGE INDEX single_edge_index')
        self.check_resp_succeeded(resp)
        expect = [['single_edge_index', 'CREATE EDGE INDEX `single_edge_index` ON `edge_1` (\n `col2`\n)']]
        self.check_result(resp, expect)

        resp = self.client.execute_query('SHOW CREATE EDGE INDEX multi_edge_1_index')
        self.check_resp_succeeded(resp)
        expect = [['multi_edge_1_index', 'CREATE EDGE INDEX `multi_edge_1_index` ON `edge_1` (\n `col2`,\n `col3`\n)']]
        self.check_result(resp, expect)
        # Check if show create edge index works well
        resp = self.client.execute_query('DROP EDGE INDEX multi_edge_1_index')
        self.check_resp_succeeded(resp)
        resp = self.client.execute_query(expect[0][1])
        self.check_resp_succeeded(resp)

        resp = self.client.execute_query('SHOW CREATE EDGE INDEX disorder_edge_1_index')
        self.check_resp_succeeded(resp)
        expect = [['disorder_edge_1_index', 'CREATE EDGE INDEX `disorder_edge_1_index` ON `edge_1` (\n `col3`,\n `col2`\n)']]
        self.check_result(resp, expect)
        # Check if show create edge index works well
        resp = self.client.execute_query('DROP EDGE INDEX disorder_edge_1_index')
        self.check_resp_succeeded(resp)
        resp = self.client.execute_query(expect[0][1])
        self.check_resp_succeeded(resp)

        resp = self.client.execute_query('SHOW CREATE EDGE INDEX non_existent_edge_index')
        self.check_resp_failed(resp)

        # Show Edge Indexes
        resp = self.client.execute_query('SHOW EDGE INDEXES')
        self.check_resp_succeeded(resp)
        self.check_out_of_order_result(resp, [['single_edge_index'], ['multi_edge_1_index'], ['disorder_edge_1_index']])

        # Drop Edge Index
        resp = self.client.execute_query('DROP EDGE INDEX single_edge_index')
        self.check_resp_succeeded(resp)
        # Check if the index is truly dropped
        resp = self.client.execute_query('DESC EDGE INDEX single_edge_index')
        self.check_resp_failed(resp)

        resp = self.client.execute_query('DROP EDGE INDEX multi_edge_1_index')
        self.check_resp_succeeded(resp)
        resp = self.client.execute_query('DESC EDGE INDEX multi_edge_1_index')
        self.check_resp_failed(resp)

        resp = self.client.execute_query('DROP EDGE INDEX disorder_edge_1_index')
        self.check_resp_succeeded(resp)
        resp = self.client.execute_query('DESC EDGE INDEX disorder_edge_1_index')
        self.check_resp_failed(resp)

        resp = self.client.execute_query('DROP EDGE INDEX non_existent_edge_index')
        self.check_resp_failed(resp)
