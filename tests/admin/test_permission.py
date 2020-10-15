# --coding:utf-8--
#
# Copyright (c) 2020 vesoft inc. All rights reserved.
#
# This source code is licensed under Apache 2.0 License,
# attached with Common Clause Condition 1.0, found in the LICENSES directory.

import time

import pytest

from tests.common.nebula_test_suite import NebulaTestSuite

from nebula2.graph import ttypes


class TestPermission(NebulaTestSuite):
    @classmethod
    def spawn_nebula_client_and_auth(self, user, password):
        client = self.spawn_nebula_client()
        resp = client.authenticate(user, password)
        if resp.error_code != ttypes.ErrorCode.SUCCEEDED:
            self.close_nebula_client(client)
            return resp, None
        else:
            return resp, client

    @classmethod
    def prepare(self):
        query = 'CREATE USER test WITH PASSWORD "test"'
        resp = self.execute(query)
        self.check_resp_succeeded(resp)
        time.sleep(self.delay)

    @classmethod
    def cleanup(self):
        query = 'DROP USER test'
        resp = self.execute(query)
        self.check_resp_succeeded(resp)

        query = 'DROP USER admin'
        resp = self.execute(query)
        self.check_resp_succeeded(resp)

        query = 'DROP USER dba'
        resp = self.execute(query)
        self.check_resp_succeeded(resp)

        query = 'DROP USER user'
        resp = self.execute(query)
        self.check_resp_succeeded(resp)

        query = 'DROP USER guest'
        resp = self.execute(query)
        self.check_resp_succeeded(resp)

        query = 'DROP SPACE test_permission_space'
        resp = self.execute(query)
        self.check_resp_succeeded(resp)

        query = 'DROP SPACE space1'
        resp = self.execute(query)
        self.check_resp_succeeded(resp)

        query = 'DROP SPACE space2'
        resp = self.execute(query)
        self.check_resp_succeeded(resp)

        query = 'DROP SPACE space3'
        resp = self.execute(query)
        self.check_resp_succeeded(resp)

        query = 'DROP SPACE space4'
        resp = self.execute(query)
        self.check_resp_succeeded(resp)

    def test_simple(self):
        # incorrect user/password
        resp, _ = self.spawn_nebula_client_and_auth('test', 'pwd')
        self.check_resp_failed(resp)

        resp, _ = self.spawn_nebula_client_and_auth('user', 'test')
        self.check_resp_failed(resp)

        resp, client = self.spawn_nebula_client_and_auth('test', 'test')
        self.check_resp_succeeded(resp)
        self.close_nebula_client(client)

        # test root user password and use space.
        query = 'CREATE SPACE test_permission_space(partition_num=1, replica_factor=1)'
        resp = self.execute(query)
        self.check_resp_succeeded(resp)
        time.sleep(self.delay)

        query = 'USE test_permission_space; CREATE TAG person(name string)'
        resp = self.execute(query)
        self.check_resp_succeeded(resp)

        # change root password, incorrect password.
        query = 'CHANGE PASSWORD test FROM "aa" TO "bb"'
        resp = self.execute(query)
        self.check_resp_failed(resp, ttypes.ErrorCode.E_EXECUTION_ERROR)

        # change root password, correct password.
        query = 'CHANGE PASSWORD test FROM "test" TO "bb"'
        resp = self.execute(query)
        self.check_resp_succeeded(resp)
        time.sleep(self.delay)

        # verify password changed
        resp, _ = self.spawn_nebula_client_and_auth('test', 'test')
        self.check_resp_failed(resp)

        resp, client = self.spawn_nebula_client_and_auth('test', 'bb')
        self.check_resp_succeeded(resp)
        self.close_nebula_client(client)

        query = 'CHANGE PASSWORD test FROM "bb" TO "test"'
        resp = self.execute(query)
        self.check_resp_succeeded(resp)

    def test_user_write(self):
        query = 'CREATE SPACE space1(partition_num=1, replica_factor=1)'
        resp = self.execute(query)
        self.check_resp_succeeded(resp)
        time.sleep(self.delay)

        query = 'CREATE USER admin WITH PASSWORD "admin"'
        resp = self.execute(query)
        self.check_resp_succeeded(resp)
        time.sleep(self.delay)

        resp, self.adminClient = self.spawn_nebula_client_and_auth('admin', 'admin')
        self.check_resp_succeeded(resp)

        query = 'GRANT ROLE ADMIN ON space1 TO admin'
        resp = self.execute(query)
        self.check_resp_succeeded(resp)

        query = 'GRANT ROLE GOD ON space1 TO admin'
        resp = self.execute(query)
        self.check_resp_failed(resp, ttypes.ErrorCode.E_BAD_PERMISSION)

        query = 'SHOW ROLES IN space1'
        resp = self.execute(query)
        self.check_resp_succeeded(resp)
        # TODO(shylock) check result
        time.sleep(self.delay)

        query = 'ALTER USER root WITH PASSWORD "root"'
        resp = self.adminClient.execute(query)
        self.check_resp_failed(resp, ttypes.ErrorCode.E_BAD_PERMISSION)

        query = 'GRANT ROLE ADMIN ON space1 TO admin'
        resp = self.adminClient.execute(query)
        self.check_resp_failed(resp, ttypes.ErrorCode.E_BAD_PERMISSION)

        query = 'GRANT ROLE GOD ON space1 TO admin'
        resp = self.adminClient.execute(query)
        self.check_resp_failed(resp, ttypes.ErrorCode.E_BAD_PERMISSION)

        query = 'GRANT ROLE GOD ON space1 TO admin'
        resp = self.adminClient.execute(query)
        self.check_resp_failed(resp, ttypes.ErrorCode.E_BAD_PERMISSION)

        # Reject the admin user grant or revoke to himself self
        query = 'GRANT ROLE GUEST ON space1 TO admin'
        resp = self.adminClient.execute(query)
        self.check_resp_failed(resp, ttypes.ErrorCode.E_BAD_PERMISSION)

        query = 'DROP USER admin'
        resp = self.adminClient.execute(query)
        self.check_resp_failed(resp, ttypes.ErrorCode.E_BAD_PERMISSION)

        query = 'DROP USER admin'
        resp = self.execute(query)
        self.check_resp_succeeded(resp)

    def test_schema_and_data(self):
        query = 'CREATE SPACE space2(partition_num=1, replica_factor=1)'
        resp = self.execute(query)
        self.check_resp_succeeded(resp)
        time.sleep(self.delay)

        query = 'CREATE USER admin WITH PASSWORD "admin"'
        resp = self.execute(query)
        self.check_resp_succeeded(resp)

        query = 'GRANT ROLE ADMIN ON space2 TO admin'
        resp = self.execute(query)
        self.check_resp_succeeded(resp)

        query = 'CREATE USER dba WITH PASSWORD "dba"'
        resp = self.execute(query)
        self.check_resp_succeeded(resp)

        query = 'GRANT ROLE DBA ON space2 TO dba'
        resp = self.execute(query)
        self.check_resp_succeeded(resp)

        query = 'CREATE USER user WITH PASSWORD "user"'
        resp = self.execute(query)
        self.check_resp_succeeded(resp)

        query = 'GRANT ROLE USER ON space2 TO user'
        resp = self.execute(query)
        self.check_resp_succeeded(resp)

        query = 'CREATE USER guest WITH PASSWORD "guest"'
        resp = self.execute(query)
        self.check_resp_succeeded(resp)

        query = 'GRANT ROLE GUEST ON space2 TO guest'
        resp = self.execute(query)
        self.check_resp_succeeded(resp)

        time.sleep(self.delay)
        resp, self.adminClient = self.spawn_nebula_client_and_auth('admin', 'admin')
        self.check_resp_succeeded(resp)

        resp, self.dbaClient = self.spawn_nebula_client_and_auth('dba', 'dba')
        self.check_resp_succeeded(resp)

        resp, self.userClient = self.spawn_nebula_client_and_auth('user', 'user')
        self.check_resp_succeeded(resp)

        resp, self.guestClient = self.spawn_nebula_client_and_auth('guest', 'guest')
        self.check_resp_succeeded(resp)

        # god write schema test
        query = 'USE space2'
        resp = self.execute(query)
        self.check_resp_succeeded(resp)

        query = 'CREATE TAG t1(t_c int)';
        resp = self.execute(query)
        self.check_resp_succeeded(resp)


        query = 'CREATE EDGE e1(e_c int)';
        resp = self.execute(query)
        self.check_resp_succeeded(resp)

        # TODO(shylock) not supported sentence
        # query = 'CREATE TAG INDEX tid1 ON t1(t_c)';
        # resp = self.execute(query)
        # self.check_resp_succeeded(resp)

        # query = 'CREATE EDGE INDEX eid1 ON e1(e_c)';
        # resp = self.execute(query)
        # self.check_resp_succeeded(resp)

        query = 'DESCRIBE TAG t1';
        resp = self.execute(query)
        self.check_resp_succeeded(resp)

        query = 'DESCRIBE EDGE e1';
        resp = self.execute(query)
        self.check_resp_succeeded(resp)

        # TODO(shylock) not supported sentence
        # query = 'DESCRIBE TAG INDEX tid1';
        # resp = self.execute(query)
        # self.check_resp_succeeded(resp)

        # query = 'DESCRIBE EDGE INDEX eid1';
        # resp = self.execute(query)
        # self.check_resp_succeeded(resp)

        # query = 'DROP TAG INDEX tid1';
        # resp = self.execute(query)
        # self.check_resp_succeeded(resp)

        # query = 'DROP EDGE INDEX eid1';
        # resp = self.execute(query)
        # self.check_resp_succeeded(resp)

        query = 'ALTER TAG t1 DROP (t_c)';
        resp = self.execute(query)
        self.check_resp_succeeded(resp)

        query = 'ALTER EDGE e1 DROP (e_c)';
        resp = self.execute(query)
        self.check_resp_succeeded(resp)

        query = 'DROP TAG t1';
        resp = self.execute(query)
        self.check_resp_succeeded(resp)

        query = 'DROP EDGE e1';
        resp = self.execute(query)
        self.check_resp_succeeded(resp)
        time.sleep(self.delay)

        # admin write schema test
        query = 'USE space2'
        resp = self.adminClient.execute(query)
        self.check_resp_succeeded(resp)

        query = 'CREATE TAG t1(t_c int)';
        resp = self.adminClient.execute(query)
        self.check_resp_succeeded(resp)

        query = "CREATE EDGE e1(e_c int)";
        resp = self.adminClient.execute(query)
        self.check_resp_succeeded(resp)

        # TODO(shylock) not supported sentence
        # query = "CREATE TAG INDEX tid1 ON t1(t_c)";
        # resp = self.adminClient.execute(query)
        # self.check_resp_succeeded(resp)

        # query = "CREATE EDGE INDEX eid1 ON e1(e_c)";
        # resp = self.adminClient.execute(query)
        # self.check_resp_succeeded(resp)

        query = "DESCRIBE TAG t1";
        resp = self.adminClient.execute(query)
        self.check_resp_succeeded(resp)

        query = "DESCRIBE EDGE e1";
        resp = self.adminClient.execute(query)
        self.check_resp_succeeded(resp)

        # TODO(shylock) not supported sentence
        # query = "DESCRIBE TAG INDEX tid1";
        # resp = self.adminClient.execute(query)
        # self.check_resp_succeeded(resp)

        # query = "DESCRIBE EDGE INDEX eid1";
        # resp = self.adminClient.execute(query)
        # self.check_resp_succeeded(resp)

        # query = "DROP TAG INDEX tid1";
        # resp = self.adminClient.execute(query)
        # self.check_resp_succeeded(resp)

        # query = "DROP EDGE INDEX eid1";
        # resp = self.adminClient.execute(query)
        # self.check_resp_succeeded(resp)

        query = "ALTER TAG t1 DROP (t_c)";
        resp = self.adminClient.execute(query)
        self.check_resp_succeeded(resp)

        query = "ALTER EDGE e1 DROP (e_c)";
        resp = self.adminClient.execute(query)
        self.check_resp_succeeded(resp)

        query = "DROP TAG t1";
        resp = self.adminClient.execute(query)
        self.check_resp_succeeded(resp)

        query = "DROP EDGE e1";
        resp = self.adminClient.execute(query)
        self.check_resp_succeeded(resp)
        time.sleep(self.delay)

        # dba write schema test
        query = 'USE space2'
        resp = self.dbaClient.execute(query)
        self.check_resp_succeeded(resp)

        query = "CREATE TAG t1(t_c int)";
        resp = self.dbaClient.execute(query)
        self.check_resp_succeeded(resp)

        query = "CREATE EDGE e1(e_c int)";
        resp = self.dbaClient.execute(query)
        self.check_resp_succeeded(resp)

        # TODO(shylock) not supported index
        # query = "CREATE TAG INDEX tid1 ON t1(t_c)";
        # resp = self.dbaClient.execute(query)
        # self.check_resp_succeeded(resp)

        # query = "CREATE EDGE INDEX eid1 ON e1(e_c)";
        # resp = self.dbaClient.execute(query)
        # self.check_resp_succeeded(resp)

        query = "DESCRIBE TAG t1";
        resp = self.dbaClient.execute(query)
        self.check_resp_succeeded(resp)

        query = "DESCRIBE EDGE e1";
        resp = self.dbaClient.execute(query)
        self.check_resp_succeeded(resp)

        # TODO(shylock) not supported index
        # query = "DESCRIBE TAG INDEX tid1";
        # resp = self.dbaClient.execute(query)
        # self.check_resp_succeeded(resp)

        # query = "DESCRIBE EDGE INDEX eid1";
        # resp = self.dbaClient.execute(query)
        # self.check_resp_succeeded(resp)

        # query = "DROP TAG INDEX tid1";
        # resp = self.dbaClient.execute(query)
        # self.check_resp_succeeded(resp)

        # query = "DROP EDGE INDEX eid1";
        # resp = self.dbaClient.execute(query)
        # self.check_resp_succeeded(resp)

        query = "ALTER TAG t1 DROP (t_c)";
        resp = self.dbaClient.execute(query)
        self.check_resp_succeeded(resp)

        query = "ALTER EDGE e1 DROP (e_c)";
        resp = self.dbaClient.execute(query)
        self.check_resp_succeeded(resp)

        query = "DROP TAG t1";
        resp = self.dbaClient.execute(query)
        self.check_resp_succeeded(resp)

        query = "DROP EDGE e1";
        resp = self.dbaClient.execute(query)
        self.check_resp_succeeded(resp)
        time.sleep(self.delay)

        # user write schema test
        query = 'USE space2'
        resp = self.userClient.execute(query)
        self.check_resp_succeeded(resp)

        query = "CREATE TAG t1(t_c int)";
        resp = self.userClient.execute(query)
        self.check_resp_failed(resp, ttypes.ErrorCode.E_BAD_PERMISSION)

        query = "CREATE EDGE e1(e_c int)";
        resp = self.userClient.execute(query)
        self.check_resp_failed(resp, ttypes.ErrorCode.E_BAD_PERMISSION)

        # TODO(shylock) not supported index
        # query = "CREATE TAG INDEX tid1 ON t1(t_c)";
        # resp = self.userClient.execute(query)
        # self.check_resp_failed(resp, ttypes.ErrorCode.E_BAD_PERMISSION)

        # query = "CREATE EDGE INDEX eid1 ON e1(e_c)";
        # resp = self.userClient.execute(query)
        # self.check_resp_failed(resp, ttypes.ErrorCode.E_BAD_PERMISSION)

        query = "DESCRIBE TAG t1";
        resp = self.userClient.execute(query)
        self.check_resp_failed(resp, ttypes.ErrorCode.E_EXECUTION_ERROR)

        query = "DESCRIBE EDGE e1";
        resp = self.userClient.execute(query)
        self.check_resp_failed(resp, ttypes.ErrorCode.E_EXECUTION_ERROR)

        # TODO(shylock) not supported sentence
        # query = "DESCRIBE TAG INDEX tid1";
        # resp = self.userClient.execute(query)
        # self.check_resp_failed(resp, ttypes.ErrorCode.E_EXECUTION_ERROR)

        # query = "DESCRIBE EDGE INDEX eid1";
        # resp = self.userClient.execute(query)
        # self.check_resp_failed(resp, ttypes.ErrorCode.E_EXECUTION_ERROR)

        # query = "DROP TAG INDEX tid1";
        # resp = self.userClient.execute(query)
        # self.check_resp_failed(resp, ttypes.ErrorCode.E_BAD_PERMISSION)

        # query = "DROP EDGE INDEX eid1";
        # resp = self.userClient.execute(query)
        # self.check_resp_failed(resp, ttypes.ErrorCode.E_BAD_PERMISSION)

        query = "ALTER TAG t1 DROP (t_c)";
        resp = self.userClient.execute(query)
        self.check_resp_failed(resp, ttypes.ErrorCode.E_BAD_PERMISSION)

        query = "ALTER EDGE e1 DROP (e_c)";
        resp = self.userClient.execute(query)
        self.check_resp_failed(resp, ttypes.ErrorCode.E_BAD_PERMISSION)

        query = "DROP TAG t1";
        resp = self.userClient.execute(query)
        self.check_resp_failed(resp, ttypes.ErrorCode.E_BAD_PERMISSION)

        query = "DROP EDGE e1";
        resp = self.userClient.execute(query)
        self.check_resp_failed(resp, ttypes.ErrorCode.E_BAD_PERMISSION)
        time.sleep(self.delay)

        # guest write schema test
        query = 'USE space2'
        resp = self.guestClient.execute(query)
        self.check_resp_succeeded(resp)

        query = "CREATE TAG t1(t_c int)";
        resp = self.guestClient.execute(query)
        self.check_resp_failed(resp, ttypes.ErrorCode.E_BAD_PERMISSION)

        query = "CREATE EDGE e1(e_c int)";
        resp = self.guestClient.execute(query)
        self.check_resp_failed(resp, ttypes.ErrorCode.E_BAD_PERMISSION)

        # TODO(shylock) not supported sentence
        # query = "CREATE TAG INDEX tid1 ON t1(t_c)";
        # resp = self.guestClient.execute(query)
        # self.check_resp_failed(resp, ttypes.ErrorCode.E_BAD_PERMISSION)

        # query = "CREATE EDGE INDEX eid1 ON e1(e_c)";
        # resp = self.guestClient.execute(query)
        # self.check_resp_failed(resp, ttypes.ErrorCode.E_BAD_PERMISSION)

        query = "DESCRIBE TAG t1";
        resp = self.guestClient.execute(query)
        self.check_resp_failed(resp, ttypes.ErrorCode.E_EXECUTION_ERROR)

        query = "DESCRIBE EDGE e1";
        resp = self.guestClient.execute(query)
        self.check_resp_failed(resp, ttypes.ErrorCode.E_EXECUTION_ERROR)

        # TODO(shylock) not supported sentence
        # query = "DESCRIBE TAG INDEX tid1";
        # resp = self.guestClient.execute(query)
        # self.check_resp_failed(resp, ttypes.ErrorCode.E_EXECUTION_ERROR)

        # query = "DESCRIBE EDGE INDEX eid1";
        # resp = self.guestClient.execute(query)
        # self.check_resp_failed(resp, ttypes.ErrorCode.E_EXECUTION_ERROR)

        # query = "DROP TAG INDEX tid1";
        # resp = self.guestClient.execute(query)
        # self.check_resp_failed(resp, ttypes.ErrorCode.E_BAD_PERMISSION)

        # query = "DROP EDGE INDEX eid1";
        # resp = self.guestClient.execute(query)
        # self.check_resp_failed(resp, ttypes.ErrorCode.E_BAD_PERMISSION)

        query = "ALTER TAG t1 DROP (t_c)";
        resp = self.guestClient.execute(query)
        self.check_resp_failed(resp, ttypes.ErrorCode.E_BAD_PERMISSION)

        query = "ALTER EDGE e1 DROP (e_c)";
        resp = self.guestClient.execute(query)
        self.check_resp_failed(resp, ttypes.ErrorCode.E_BAD_PERMISSION)

        query = "DROP TAG t1";
        resp = self.guestClient.execute(query)
        self.check_resp_failed(resp, ttypes.ErrorCode.E_BAD_PERMISSION)

        query = "DROP EDGE e1";
        resp = self.guestClient.execute(query)
        self.check_resp_failed(resp, ttypes.ErrorCode.E_BAD_PERMISSION)

        # god write data test
        query = 'USE space2'
        resp = self.dbaClient.execute(query)
        self.check_resp_succeeded(resp)

        query = 'CREATE TAG t1(t_c int)'
        resp = self.dbaClient.execute(query)
        self.check_resp_succeeded(resp)

        query = "CREATE EDGE e1(e_c int)"
        resp = self.dbaClient.execute(query)
        self.check_resp_succeeded(resp)
        time.sleep(self.delay)

        query = 'INSERT VERTEX t1(t_c) VALUES "1":(1)';
        resp = self.dbaClient.execute(query)
        self.check_resp_succeeded(resp)

        query = 'INSERT EDGE e1(e_c) VALUES "1" -> "2":(95)';
        resp = self.dbaClient.execute(query)
        self.check_resp_succeeded(resp)

        query = 'GO FROM "1" OVER e1';
        resp = self.dbaClient.execute(query)
        self.check_resp_succeeded(resp)

        # admin write data test
        query = 'USE space2'
        resp = self.adminClient.execute(query)
        self.check_resp_succeeded(resp)

        query = 'INSERT VERTEX t1(t_c) VALUES "1":(1)'
        resp = self.adminClient.execute(query)
        self.check_resp_succeeded(resp)

        query = 'INSERT EDGE e1(e_c) VALUES "1" -> "2":(95)';
        resp = self.adminClient.execute(query)
        self.check_resp_succeeded(resp)

        query = 'GO FROM "1" OVER e1';
        resp = self.adminClient.execute(query)
        self.check_resp_succeeded(resp)

        # dba write data test
        query = 'USE space2'
        resp = self.dbaClient.execute(query)
        self.check_resp_succeeded(resp)

        query = 'INSERT VERTEX t1(t_c) VALUES "1":(1)'
        resp = self.dbaClient.execute(query)
        self.check_resp_succeeded(resp)

        query = 'INSERT EDGE e1(e_c) VALUES "1" -> "2":(95)';
        resp = self.dbaClient.execute(query)
        self.check_resp_succeeded(resp)

        query = 'GO FROM "1" OVER e1';
        resp = self.dbaClient.execute(query)
        self.check_resp_succeeded(resp)

        # user write data test
        query = 'USE space2'
        resp = self.userClient.execute(query)
        self.check_resp_succeeded(resp)

        query = 'INSERT VERTEX t1(t_c) VALUES "1":(1)'
        resp = self.userClient.execute(query)
        self.check_resp_succeeded(resp)

        query = 'INSERT EDGE e1(e_c) VALUES "1" -> "2":(95)';
        resp = self.userClient.execute(query)
        self.check_resp_succeeded(resp)

        query = 'GO FROM "1" OVER e1';
        resp = self.userClient.execute(query)
        self.check_resp_succeeded(resp)

        # guest write data test
        query = 'USE space2'
        resp = self.guestClient.execute(query)
        self.check_resp_succeeded(resp)

        query = 'INSERT VERTEX t1(t_c) VALUES "1":(1)'
        resp = self.guestClient.execute(query)
        self.check_resp_failed(resp, ttypes.ErrorCode.E_BAD_PERMISSION)

        query = 'INSERT EDGE e1(e_c) VALUES "1" -> "2":(95)';
        resp = self.guestClient.execute(query)
        self.check_resp_failed(resp, ttypes.ErrorCode.E_BAD_PERMISSION)

        query = 'GO FROM "1" OVER e1';
        resp = self.guestClient.execute(query)
        self.check_resp_succeeded(resp)

        # use space test
        query = "CREATE SPACE space3(partition_num=1, replica_factor=1)";
        resp = self.execute(query)
        self.check_resp_succeeded(resp)
        time.sleep(self.delay)

        query = 'USE space3'
        resp = self.execute(query)
        self.check_resp_succeeded(resp)

        resp = self.adminClient.execute(query)
        self.check_resp_failed(resp, ttypes.ErrorCode.E_BAD_PERMISSION)

        resp = self.dbaClient.execute(query)
        self.check_resp_failed(resp, ttypes.ErrorCode.E_BAD_PERMISSION)

        resp = self.userClient.execute(query)
        self.check_resp_failed(resp, ttypes.ErrorCode.E_BAD_PERMISSION)

        resp = self.guestClient.execute(query)
        self.check_resp_failed(resp, ttypes.ErrorCode.E_BAD_PERMISSION)

    def test_show_test(self):
        resp, self.adminClient = self.spawn_nebula_client_and_auth('admin', 'admin')
        self.check_resp_succeeded(resp)
        resp, self.dbaClient = self.spawn_nebula_client_and_auth('dba', 'dba')
        self.check_resp_succeeded(resp)
        resp, self.userClient = self.spawn_nebula_client_and_auth('user', 'user')
        self.check_resp_succeeded(resp)
        resp, self.guestClient = self.spawn_nebula_client_and_auth('guest', 'guest')
        self.check_resp_succeeded(resp)

        query = 'CREATE SPACE space4(partition_num=1, replica_factor=1)'
        resp = self.execute(query)
        self.check_resp_succeeded(resp)
        time.sleep(self.delay)

        query = 'SHOW SPACES'
        resp = self.execute_query(query)
        self.check_resp_succeeded(resp)
        expected_column_names = ['Name']
        expected_result = [[['test_permission_space']], [['space1']], [['space2']], [['space3']], [['space4']]]
        self.check_column_names(resp, expected_column_names)
        for row in expected_result:
            self.search_result(resp, row)

        resp = self.adminClient.execute_query(query)
        self.check_resp_succeeded(resp)
        expected_result = [['space2']]
        self.check_column_names(resp, expected_column_names)
        self.check_out_of_order_result(resp, expected_result)

        resp = self.dbaClient.execute_query(query)
        self.check_resp_succeeded(resp)
        expected_result = [['space2']]
        self.check_column_names(resp, expected_column_names)
        self.check_out_of_order_result(resp, expected_result)

        resp = self.userClient.execute_query(query)
        self.check_resp_succeeded(resp)
        expected_result = [['space2']]
        self.check_column_names(resp, expected_column_names)
        self.check_out_of_order_result(resp, expected_result)

        resp = self.guestClient.execute_query(query)
        self.check_resp_succeeded(resp)
        expected_result = [['space2']]
        self.check_column_names(resp, expected_column_names)
        self.check_out_of_order_result(resp, expected_result)

        query = 'SHOW ROLES IN space1'
        resp = self.guestClient.execute(query)
        self.check_resp_failed(resp, ttypes.ErrorCode.E_BAD_PERMISSION)

        query = 'SHOW ROLES IN space2'
        resp = self.guestClient.execute(query)
        self.check_resp_succeeded(resp)

        query = 'SHOW ROLES IN space1'
        resp = self.execute(query)
        self.check_resp_succeeded(resp)

        query = 'SHOW CREATE SPACE space1'
        resp = self.guestClient.execute(query)
        self.check_resp_failed(resp, ttypes.ErrorCode.E_BAD_PERMISSION)

        query = 'SHOW CREATE SPACE space1'
        resp = self.execute(query)
        self.check_resp_succeeded(resp)

        query = 'SHOW CREATE SPACE space2'
        resp = self.guestClient.execute(query)
        self.check_resp_succeeded(resp)

    def test_show_roles(self):
        query = 'CREATE SPACE space5(partition_num=1, replica_factor=1)'
        resp = self.execute(query)
        self.check_resp_succeeded(resp)
        time.sleep(self.delay)

        query = 'GRANT ROLE ADMIN ON space5 TO admin'
        resp = self.execute(query)
        self.check_resp_succeeded(resp)

        query = 'GRANT ROLE DBA ON space5 TO dba'
        resp = self.execute(query)
        self.check_resp_succeeded(resp)

        query = 'GRANT ROLE USER ON space5 TO user'
        resp = self.execute(query)
        self.check_resp_succeeded(resp)

        query = 'GRANT ROLE GUEST ON space5 TO guest'
        resp = self.execute(query)
        self.check_resp_succeeded(resp)
        time.sleep(self.delay)

        resp, self.adminClient = self.spawn_nebula_client_and_auth('admin', 'admin')
        self.check_resp_succeeded(resp)
        resp, self.dbaClient = self.spawn_nebula_client_and_auth('dba', 'dba')
        self.check_resp_succeeded(resp)
        resp, self.userClient = self.spawn_nebula_client_and_auth('user', 'user')
        self.check_resp_succeeded(resp)
        resp, self.guestClient = self.spawn_nebula_client_and_auth('guest', 'guest')
        self.check_resp_succeeded(resp)

        query = 'SHOW ROLES IN space5'
        expected_result = [['guest', 'GUEST'],
                           ['user', 'USER'],
                           ['dba', 'DBA'],
                           ['admin', 'ADMIN']]
        resp = self.execute_query(query)
        self.check_resp_succeeded(resp)
        self.check_out_of_order_result(resp, expected_result)

        resp = self.adminClient.execute_query(query)
        self.check_resp_succeeded(resp)
        self.check_out_of_order_result(resp, expected_result)

        expected_result = [['dba', 'DBA']]
        resp = self.dbaClient.execute_query(query)
        self.check_resp_succeeded(resp)
        self.check_out_of_order_result(resp, expected_result)

        expected_result = [['user', 'USER']]
        resp = self.userClient.execute_query(query)
        self.check_resp_succeeded(resp)
        self.check_out_of_order_result(resp, expected_result)

        expected_result = [['guest', 'GUEST']]
        resp = self.guestClient.execute_query(query)
        self.check_resp_succeeded(resp)
        self.check_out_of_order_result(resp, expected_result)
