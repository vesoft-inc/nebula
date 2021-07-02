# --coding:utf-8--
#
# Copyright (c) 2020 vesoft inc. All rights reserved.
#
# This source code is licensed under Apache 2.0 License,
# attached with Common Clause Condition 1.0, found in the LICENSES directory.

import time

from tests.common.nebula_test_suite import NebulaTestSuite, T_EMPTY


class TestSpace(NebulaTestSuite):

    def test_space(self):
        # not exist
        resp = self.client.execute('USE not_exist_space')
        self.check_resp_failed(resp)

        # with default options
        resp = self.client.execute('CREATE SPACE space_with_default_options (vid_type=FIXED_STRING(8))')
        self.check_resp_succeeded(resp)

        resp = self.client.execute('CREATE SPACE space_on_default_group on default')
        self.check_resp_failed(resp)

        # check result
        resp = self.client.execute('DESC SPACE space_with_default_options')
        expect_result = [['space_with_default_options', 100, 1, 'utf8', 'utf8_bin',
        'FIXED_STRING(8)', False, 'default', T_EMPTY]]
        self.check_result(resp, expect_result, {0})

        # drop space
        resp = self.client.execute('DROP SPACE space_with_default_options')
        self.check_resp_succeeded(resp)

        # create space succeeded
        resp = self.client.execute('CREATE SPACE default_space(partition_num=9, replica_factor=1, vid_type=FIXED_STRING(8))')
        self.check_resp_succeeded(resp)

        # show spaces
        resp = self.client.execute('SHOW SPACES')
        self.check_resp_succeeded(resp)
        self.search_result(resp, [['default_space']])

        # desc space
        resp = self.client.execute('DESC SPACE default_space')
        self.check_resp_succeeded(resp)
        expect_result = [['default_space', 9, 1, 'utf8', 'utf8_bin', 'FIXED_STRING(8)',
        False, 'default', T_EMPTY]]
        self.check_result(resp, expect_result, {0})

        # show create space
        # TODO(shylock) need meta cache to permission checking
        time.sleep(self.delay)
        resp = self.client.execute('SHOW CREATE SPACE default_space')
        self.check_resp_succeeded(resp)

        create_space_str_result = 'CREATE SPACE `default_space` (partition_num = 9, '\
                                  'replica_factor = 1, '\
                                  'charset = utf8, '\
                                  'collate = utf8_bin, '\
                                  'vid_type = FIXED_STRING(8), '\
                                  'atomic_edge = false) '\
                                  'ON default'

        expect_result = [['default_space', create_space_str_result]]
        self.check_result(resp, expect_result)

        # check result from show create
        resp = self.client.execute('DROP SPACE default_space')
        self.check_resp_succeeded(resp)

        create_space_str = 'CREATE SPACE `default_space` (partition_num = 9, '\
                           'replica_factor = 1, '\
                           'charset = utf8, '\
                           'collate = utf8_bin, '\
                           'vid_type = FIXED_STRING(8), '\
                           'atomic_edge = false)'

        resp = self.client.execute(create_space_str)
        self.check_resp_succeeded(resp)

        resp = self.client.execute('SHOW SPACES')
        self.check_resp_succeeded(resp)

        # 2.0 when use space, the validator get from cache
        time.sleep(self.delay)
        resp = self.client.execute("USE default_space")
        self.check_resp_succeeded(resp)

    def test_charset_collate(self):
        resp = self.client.execute('CREATE SPACE space_charset_collate (partition_num=9, '
                                   'replica_factor=1, charset=utf8, collate=utf8_bin, vid_type=FIXED_STRING(8))')
        self.check_resp_succeeded(resp)

        resp = self.client.execute('DESC SPACE space_charset_collate')
        self.check_resp_succeeded(resp)
        expect_result = [['space_charset_collate', 9, 1, 'utf8', 'utf8_bin', 'FIXED_STRING(8)', False, 'default', T_EMPTY]]
        self.check_result(resp, expect_result, {0})

        # drop space
        resp = self.client.execute('DROP SPACE space_charset_collate')
        self.check_resp_succeeded(resp)

        resp = self.client.execute('CREATE SPACE space_charset (partition_num=9, '
                                   'replica_factor=1, charset=utf8, vid_type=FIXED_STRING(8))')
        self.check_resp_succeeded(resp)

        resp = self.client.execute('DESC SPACE space_charset')
        self.check_resp_succeeded(resp)
        expect_result = [['space_charset', 9, 1, 'utf8', 'utf8_bin', 'FIXED_STRING(8)', False, 'default', T_EMPTY]]
        self.check_result(resp, expect_result, {0})

        # drop space
        resp = self.client.execute('DROP SPACE space_charset')
        self.check_resp_succeeded(resp)

        resp = self.client.execute('CREATE SPACE space_collate (partition_num=9, '
                                   'replica_factor=1, collate=utf8_bin, vid_type=FIXED_STRING(8))')
        self.check_resp_succeeded(resp)

        resp = self.client.execute('DESC SPACE space_collate')
        self.check_resp_succeeded(resp)
        expect_result = [['space_collate', 9, 1, 'utf8', 'utf8_bin', 'FIXED_STRING(8)', False, 'default', T_EMPTY]]
        self.check_result(resp, expect_result, {0})

        # drop space
        resp = self.client.execute('DROP SPACE space_collate')
        self.check_resp_succeeded(resp)

        # not supported collate
        resp = self.client.execute('CREATE SPACE space_charset_collate_nomatch (partition_num=9, '
                                   'replica_factor=1, charset = utf8, collate=gbk_bin, vid_type=FIXED_STRING(8))')
        self.check_resp_failed(resp)

        # not supported charset
        resp = self.client.execute('CREATE SPACE space_charset_collate_nomatch (partition_num=9, '
                                   'replica_factor=1, charset = gbk, collate=utf8_bin, vid_type=FIXED_STRING(8))')
        self.check_resp_failed(resp)

        # not supported charset
        resp = self.client.execute('CREATE SPACE space_illegal_charset (partition_num=9, '
                                   'replica_factor=1, charset = gbk, vid_type=FIXED_STRING(8))')
        self.check_resp_failed(resp)

        # not supported collate
        resp = self.client.execute('CREATE SPACE space_illegal_collate (partition_num=9, '
                                   'replica_factor=1, collate = gbk_bin, vid_type=FIXED_STRING(8))')
        self.check_resp_failed(resp)

        resp = self.client.execute('CREATE SPACE space_illegal_collate (partition_num=9, '
                                   'replica_factor=1, collate = gbk_bin, vid_type=FIXED_STRING(8))')
        self.check_resp_failed(resp)

        resp = self.client.execute('CREATE SPACE space_capital (partition_num=9, '
                                   'replica_factor=1, charset=UTF8, collate=UTF8_bin, vid_type=FIXED_STRING(8))')
        self.check_resp_succeeded(resp)

        resp = self.client.execute('DESC SPACE space_capital')
        self.check_resp_succeeded(resp)
        expect_result = [['space_capital', 9, 1, 'utf8', 'utf8_bin', 'FIXED_STRING(8)',
        False, 'default', T_EMPTY]]
        self.check_result(resp, expect_result, {0})

        # drop space
        resp = self.client.execute('DROP SPACE space_capital')
        self.check_resp_succeeded(resp)

    def test_if_not_exists_and_if_exist(self):
        # exist then failed
        resp = self.client.execute('CREATE SPACE default_space')
        self.check_resp_failed(resp)

        # exist but success
        resp = self.client.execute('CREATE SPACE IF NOT EXISTS default_space')
        self.check_resp_failed(resp)

        # not exist but success
        resp = self.client.execute('DROP SPACE IF EXISTS not_exist_space')
        self.check_resp_succeeded(resp)

        # not exist then failed
        resp = self.client.execute('DROP SPACE not_exist_space')
        self.check_resp_failed(resp)

        resp = self.client.execute('CREATE SPACE exist_space')
        self.check_resp_failed(resp)

        resp = self.client.execute('DROP SPACE IF EXISTS exist_space')
        self.check_resp_succeeded(resp)

    def test_drop_space(self):
        resp = self.client.execute('SHOW SPACES')
        self.check_resp_succeeded(resp)
        expect_result = [['default_space']]
        self.search_result(resp, expect_result)

        resp = self.client.execute('DROP SPACE default_space')
        self.check_resp_succeeded(resp)

        resp = self.client.execute('SHOW SPACES')
        self.check_resp_succeeded(resp)
        expect_result = [['default_space']]
        self.search_not_exist(resp, expect_result)

    def test_create_space_with_string_vid(self):
        resp = self.client.execute('CREATE SPACE space_string_vid (partition_num=9, '
                                   'replica_factor=1, charset=utf8, collate=utf8_bin, '
                                   'vid_type = fixed_string(30))')
        self.check_resp_succeeded(resp)

        resp = self.client.execute('DESC SPACE space_string_vid')
        self.check_resp_succeeded(resp)
        expect_result = [['space_string_vid', 9, 1, 'utf8', 'utf8_bin', 'FIXED_STRING(30)', False, 'default', T_EMPTY]]
        self.check_result(resp, expect_result, {0})

        # clean up
        resp = self.client.execute('DROP SPACE space_string_vid')
        self.check_resp_succeeded(resp)

    def test_create_space_with_int_vid(self):
        resp = self.client.execute('CREATE SPACE space_int_vid (partition_num=9, '
                                   'replica_factor=1, charset=utf8, collate=utf8_bin, '
                                   'vid_type = int64)')
        self.check_resp_succeeded(resp)

        resp = self.client.execute('DESC SPACE space_int_vid')
        self.check_resp_succeeded(resp)
        expect_result = [['space_int_vid', 9, 1, 'utf8', 'utf8_bin', 'INT64', False, 'default', T_EMPTY]]
        self.check_result(resp, expect_result, {0})

        # clean up
        resp = self.client.execute('DROP SPACE space_int_vid')
        self.check_resp_succeeded(resp)
