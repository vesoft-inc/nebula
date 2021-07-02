# --coding:utf-8--
#
# Copyright (c) 2020 vesoft inc. All rights reserved.
#
# This source code is licensed under Apache 2.0 License,
# attached with Common Clause Condition 1.0, found in the LICENSES directory.

import time

from tests.common.nebula_test_suite import NebulaTestSuite


class TestUsers(NebulaTestSuite):
    @classmethod
    def prepare(self):
        query = 'CREATE SPACE user_space(partition_num=1, replica_factor=1, vid_type=FIXED_STRING(8))'
        resp = self.execute(query)
        self.check_resp_succeeded(resp)
        time.sleep(self.delay)

    @classmethod
    def cleanup(self):
        query = 'DROP SPACE user_space'
        resp = self.execute(query)
        self.check_resp_succeeded(resp)

        query = 'DROP USER IF EXISTS user1'
        resp = self.execute(query)
        self.check_resp_succeeded(resp)

        query = 'DROP USER IF EXISTS user2'
        resp = self.execute(query)
        self.check_resp_succeeded(resp)

        query = 'DROP USER IF EXISTS user3'
        resp = self.execute(query)
        self.check_resp_succeeded(resp)

    def test_create_users(self):
        query = 'DROP USER IF EXISTS user1'
        resp = self.execute(query)
        self.check_resp_succeeded(resp)

        query = 'CREATE USER user1 WITH PASSWORD "pwd1"'
        resp = self.execute(query)
        self.check_resp_succeeded(resp)

        query = 'DROP USER IF EXISTS user2'
        resp = self.execute(query)
        self.check_resp_succeeded(resp)

        query = 'CREATE USER user2'
        resp = self.execute(query)
        self.check_resp_succeeded(resp)

        # user exists.
        query = 'CREATE USER user1 WITH PASSWORD "pwd1" '
        resp = self.execute(query)
        self.check_resp_failed(resp)

        # user exists. but if not exists is true.
        query = 'CREATE USER IF NOT EXISTS user1 WITH PASSWORD "pwd1" '
        resp = self.execute(query)
        self.check_resp_succeeded(resp)

        query = 'SHOW USERS'
        expected_column_names = ['Account']
        expected_result = [[['root']], [['user1']], [['user2']]]
        resp = self.execute(query)
        self.check_resp_succeeded(resp)
        self.check_column_names(resp, expected_column_names)
        for row in expected_result:
            self.search_result(resp, row)

    def test_alter_users(self):
        # not exist user
        query = 'ALTER USER user WITH PASSWORD "pwd1"'
        resp = self.execute(query)
        self.check_resp_failed(resp)

        query = 'ALTER USER user2 WITH PASSWORD "pwd2"'
        resp = self.execute(query)
        self.check_resp_succeeded(resp)

    def test_drop_users(self):
        query = 'DROP USER user'
        resp = self.execute(query)
        self.check_resp_failed(resp)

        query = 'DROP USER IF EXISTS user'
        resp = self.execute(query)
        self.check_resp_succeeded(resp)

        query = 'CREATE USER user3'
        resp = self.execute(query)
        self.check_resp_succeeded(resp)

        query = 'DROP USER user3'
        resp = self.execute(query)
        self.check_resp_succeeded(resp)

        query = 'SHOW USERS'
        expected_column_names = ['Account']
        expected_result = [[['root']], [['user1']], [['user2']]]
        resp = self.execute(query)
        self.check_resp_succeeded(resp)
        self.check_column_names(resp, expected_column_names)
        for row in expected_result:
            self.search_result(resp, row)

    def test_change_password(self):
        # user is not exists. expect fail.
        query = 'CHANGE PASSWORD user FROM "pwd" TO "pwd"'
        resp = self.execute(query)
        self.check_resp_failed(resp)

        # user login type is LDAP, can not change password. expect fail.
        query = 'CHANGE PASSWORD user1 FROM "pwd" TO "pwd"'
        resp = self.execute(query)
        self.check_resp_failed(resp)

        # old password invalid. expect fail.
        query = 'CHANGE PASSWORD user2 FROM "pwd" TO "newpwd"'
        resp = self.execute(query)
        self.check_resp_failed(resp)

        query = 'CHANGE PASSWORD user2 FROM "pwd2" TO "newpwd"'
        resp = self.execute(query)
        self.check_resp_succeeded(resp)

    def test_grant_revoke(self):
        # must set the space if is not god role. expect fail.
        query = 'GRANT DBA TO user1'
        resp = self.execute(query)
        self.check_resp_failed(resp)

        # user not exist. expect fail.
        query = 'GRANT DBA ON user_space TO user'
        resp = self.execute(query);
        self.check_resp_failed(resp)

        # TODO(shylock) permission
        # query = 'GRANT GOD ON user_space TO user1'
        # resp = self.execute(query)
        # self.check_resp_failed(resp)

        # space not exists. expect fail.
        query = 'GRANT ROLE DBA ON space TO user2'
        resp = self.execute(query)
        self.check_resp_failed(resp)

        query = 'SHOW USERS'
        expected_column_names = ['Account']
        expected_result = [[['root']], [['user1']], [['user2']]]
        resp = self.execute(query)
        self.check_resp_succeeded(resp)
        self.check_column_names(resp, expected_column_names)
        for row in expected_result:
            self.search_result(resp, row)

        query = 'GRANT ROLE DBA ON user_space TO user2'
        resp = self.execute(query)
        self.check_resp_succeeded(resp)

        query = 'CREATE USER user3'
        resp = self.execute(query)
        self.check_resp_succeeded(resp)

        query = 'GRANT ROLE GUEST ON user_space TO user3'
        resp = self.execute(query)
        self.check_resp_succeeded(resp)

        # space not exists. expect fail.
        query = 'SHOW ROLES IN space'
        resp = self.execute(query)
        self.check_resp_failed(resp);

        query = 'SHOW ROLES IN user_space'
        resp = self.execute(query)
        expected_column_names = ['Account', 'Role Type']
        expected_result = [['user2', 'DBA'], ['user3', 'GUEST']]
        self.check_resp_succeeded(resp)
        self.check_column_names(resp, expected_column_names)
        self.check_out_of_order_result(resp, expected_result)

        # space not exist, expect fail.
        query = 'REVOKE ROLE DBA ON space FROM user2'
        resp = self.execute(query)
        self.check_resp_failed(resp)

        # user not exist. expect fail.
        query = 'REVOKE ROLE DBA ON user_space FROM user'
        resp = self.execute(query)
        self.check_resp_failed(resp)

        # role invalid. expect fail.
        query = 'REVOKE ROLE ADMIN ON user_space FROM user2'
        resp = self.execute(query)
        self.check_resp_failed(resp)

        query = 'REVOKE ROLE DBA ON user_space FROM user2'
        resp = self.execute(query)
        self.check_resp_succeeded(resp)

        query = 'SHOW ROLES IN user_space'
        expected_column_names = ['Account', 'Role Type']
        expected_result = [['user3', 'GUEST']]
        resp = self.execute(query)
        self.check_resp_succeeded(resp)
        self.check_column_names(resp, expected_column_names)
        self.check_out_of_order_result(resp, expected_result)
