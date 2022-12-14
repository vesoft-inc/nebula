# --coding:utf-8--
#
# Copyright (c) 2020 vesoft inc. All rights reserved.
#
# This source code is licensed under Apache 2.0 License.

import pytest
from tests.common.nebula_test_suite import NebulaTestSuite


class TestConfigs(NebulaTestSuite):

    @classmethod
    def prepare(self):
        pass

    @classmethod
    def cleanup(self):
        pass

    def test_configs(self):
        # set/get without declaration
        resp = self.client.execute('UPDATE CONFIGS storage:notRegistered=123;')
        self.check_resp_failed(resp)

        # update immutable config will fail, read-only
        resp = self.client.execute('UPDATE CONFIGS storage:num_io_threads=10')
        self.check_resp_failed(resp)

        # set and get config after declaration
        v = 3
        resp = self.client.execute('UPDATE CONFIGS meta:v={}'.format(3))
        self.check_resp_failed(resp)
        resp = self.client.execute('UPDATE CONFIGS graph:v={}'.format(3))
        self.check_resp_succeeded(resp)
        resp = self.client.execute('UPDATE CONFIGS storage:v={}'.format(3))
        self.check_resp_succeeded(resp)

        # update flag to an invalid value, expected to fail
        resp = self.client.execute('UPDATE CONFIGS graph:session_idle_timeout_secs={}'.format(0))
        self.check_resp_failed(resp)
        resp = self.client.execute('UPDATE CONFIGS graph:session_idle_timeout_secs={}'.format(999999))
        self.check_resp_failed(resp)

        # get
        resp = self.client.execute('GET CONFIGS meta:v')
        self.check_resp_failed(resp)

        resp = self.client.execute('GET CONFIGS graph:v')
        self.check_resp_succeeded(resp)
        expected_result = [['GRAPH', 'v', 'int', 'MUTABLE', 3]]
        self.check_result(resp, expected_result)

        resp = self.client.execute('GET CONFIGS storage:v')
        self.check_resp_succeeded(resp)
        expected_result = [['STORAGE', 'v', 'int', 'MUTABLE', 3]]
        self.check_result(resp, expected_result)

        # show configs
        resp = self.client.execute('SHOW CONFIGS meta')
        self.check_resp_succeeded(resp)

        resp = self.client.execute('SHOW CONFIGS graph')
        self.check_resp_succeeded(resp)

        expected_result = [
            ['GRAPH', 'v', 'int', 'MUTABLE', v],
            ['GRAPH', 'minloglevel', 'int', 'MUTABLE', 0],
            ['GRAPH', 'heartbeat_interval_secs', 'int', 'MUTABLE', 1],
            ['GRAPH', 'meta_client_retry_times', 'int', 'MUTABLE', 3],
            ['GRAPH', 'accept_partial_success', 'bool', 'MUTABLE', False],
            ['GRAPH', 'system_memory_high_watermark_ratio', 'float', 'MUTABLE', 0.95],
            ['GRAPH', 'num_rows_to_check_memory', 'int', 'MUTABLE', 4],
            ['GRAPH', 'session_idle_timeout_secs', 'int', 'MUTABLE', 28800],
            ['GRAPH', 'session_reclaim_interval_secs', 'int', 'MUTABLE', 2],
            ['GRAPH', 'max_allowed_connections', 'int', 'MUTABLE', 9223372036854775807],
            ['GRAPH', 'disable_octal_escape_char', 'bool', 'MUTABLE', False],
            ['GRAPH', 'max_sessions_per_ip_per_user', 'int', 'MUTABLE', 300],
            ['GRAPH', 'timezone_name', 'string', 'MUTABLE', "UTC+00:00:00"],
        ]
        self.check_out_of_order_result(resp, expected_result)

        resp = self.client.execute('SHOW CONFIGS storage')
        self.check_resp_succeeded(resp)
        expected_result = [
            ['STORAGE', 'v', 'int', 'MUTABLE', 3],
            ['STORAGE', 'wal_ttl', 'int', 'MUTABLE', 14400],
            ['STORAGE', 'minloglevel', 'int', 'MUTABLE', 0],
            ['STORAGE', 'custom_filter_interval_secs', 'int', 'MUTABLE', 86400],
            ['STORAGE', 'heartbeat_interval_secs', 'int', 'MUTABLE', 1],
            ['STORAGE', 'meta_client_retry_times', 'int', 'MUTABLE', 3],
            ['STORAGE', 'rocksdb_db_options', 'map', 'MUTABLE', {}],
            ['STORAGE', 'clean_wal_interval_secs', 'int', 'MUTABLE', 600],
            ['STORAGE', 'rocksdb_column_family_options', 'map', 'MUTABLE', {"write_buffer_size": "67108864", "max_bytes_for_level_base": "268435456", "max_write_buffer_number": "4"}],
            ['STORAGE', 'rocksdb_block_based_table_options', 'map', 'MUTABLE', {"block_size": "8192"}],
            ["STORAGE", "max_edge_returned_per_vertex", "int", "MUTABLE", 2147483647],
            ['STORAGE', 'system_memory_high_watermark_ratio', 'float', 'MUTABLE', 0.8],
            ['STORAGE', 'timezone_name', 'string', 'MUTABLE', "UTC+00:00:00"],
        ]
        print(resp)
        self.check_out_of_order_result(resp, expected_result)

        # update rocksdb
        resp = self.client.execute('''
                                   UPDATE CONFIGS storage:rocksdb_column_family_options={
                                   max_bytes_for_level_base="1024",
                                   write_buffer_size="1024",
                                   max_write_buffer_number="4"}
                                   ''')
        self.check_resp_succeeded(resp)
        
        # get result
        resp = self.client.execute('GET CONFIGS storage:rocksdb_column_family_options')
        self.check_resp_succeeded(resp)
        value = {"max_bytes_for_level_base": "1024", "write_buffer_size": "1024", "max_write_buffer_number": "4"}
        expected_result = [['STORAGE', 'rocksdb_column_family_options', 'map', 'MUTABLE', value]]
        self.check_result(resp, expected_result)

        # restore
        resp = self.client.execute('''
                                   UPDATE CONFIGS storage:rocksdb_column_family_options={
                                   max_bytes_for_level_base="268435456",
                                   write_buffer_size="67108864",
                                   max_write_buffer_number="4"}
                                   ''')
        self.check_resp_succeeded(resp)

    @pytest.mark.skip("The change of minloglevel will influence the whole test.")
    def test_update_configs(self):
        # set and get a config of all module
        resp = self.client.execute('UPDATE CONFIGS minloglevel={}'.format(2))
        self.check_resp_succeeded(resp)

        # check result
        resp = self.client.execute('GET CONFIGS minloglevel')
        self.check_resp_succeeded(resp)
        expected_result = [['GRAPH', 'minloglevel', 'int', 'MUTABLE', 2],
                           ['STORAGE', 'minloglevel', 'int', 'MUTABLE', 2]]
        self.check_out_of_order_result(resp, expected_result)

        # update storage
        resp = self.client.execute('UPDATE CONFIGS storage:minloglevel={}'.format(3))
        self.check_resp_succeeded(resp)

        # get result
        resp = self.client.execute('GET CONFIGS minloglevel')
        self.check_resp_succeeded(resp)
        expected_result = [['GRAPH', 'minloglevel', 'int', 'MUTABLE', 2],
                           ['STORAGE', 'minloglevel', 'int', 'MUTABLE', 3]]
        self.check_result(resp, expected_result)
