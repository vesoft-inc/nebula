# --coding:utf-8--
#
# Copyright (c) 2020 vesoft inc. All rights reserved.
#
# This source code is licensed under Apache 2.0 License.

import time

from nebula3.common import ttypes
from tests.common.nebula_test_suite import NebulaTestSuite


class TestPermission(NebulaTestSuite):
    @classmethod
    def spawn_nebula_client_and_auth(self, user, password):
        try:
            session = self.spawn_nebula_client(user, password)
            return True, session
        except Exception as e:
            print('Get Exception: {}'.format(e))
            return False, None

    @classmethod
    def prepare(cls):
        query = 'CREATE USER test WITH PASSWORD "test"'
        resp = cls.execute(query)
        cls.check_resp_succeeded(resp)
        time.sleep(cls.delay * 2)

    @classmethod
    def cleanup(self):
        query = 'DROP USER IF EXISTS test'
        resp = self.execute(query)
        self.check_resp_succeeded(resp)

        query = 'DROP USER IF EXISTS admin'
        resp = self.execute(query)
        self.check_resp_succeeded(resp)

        query = 'DROP USER IF EXISTS dba'
        resp = self.execute(query)
        self.check_resp_succeeded(resp)

        query = 'DROP USER IF EXISTS user'
        resp = self.execute(query)
        self.check_resp_succeeded(resp)

        query = 'DROP USER IF EXISTS guest'
        resp = self.execute(query)
        self.check_resp_succeeded(resp)

        query = 'DROP SPACE IF EXISTS test_permission_space'
        resp = self.execute(query)
        self.check_resp_succeeded(resp)

        query = 'DROP SPACE IF EXISTS space1'
        resp = self.execute(query)
        self.check_resp_succeeded(resp)

        query = 'DROP SPACE IF EXISTS space2'
        resp = self.execute(query)
        self.check_resp_succeeded(resp)

        query = 'DROP SPACE IF EXISTS space3'
        resp = self.execute(query)
        self.check_resp_succeeded(resp)

        query = 'DROP SPACE IF EXISTS space4'
        resp = self.execute(query)
        self.check_resp_succeeded(resp)

    def test_simple(self):
        # incorrect user/password
        ret, _ = self.spawn_nebula_client_and_auth('test', 'pwd')
        assert not ret

        ret, _ = self.spawn_nebula_client_and_auth('user', 'test')
        assert not ret

        ret, client = self.spawn_nebula_client_and_auth('test', 'test')
        assert ret
        self.release_nebula_client(client)

        # test root user password and use space.
        query = 'CREATE SPACE test_permission_space(partition_num=1, replica_factor=1, vid_type=FIXED_STRING(8))'
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
        ret, _ = self.spawn_nebula_client_and_auth('test', 'test')
        assert not ret

        ret, client = self.spawn_nebula_client_and_auth('test', 'bb')
        assert ret
        self.release_nebula_client(client)

        query = 'CHANGE PASSWORD test FROM "bb" TO "test"'
        resp = self.execute(query)
        self.check_resp_succeeded(resp)

    def test_user_write(self):
        query = 'CREATE SPACE space1(partition_num=1, replica_factor=1, vid_type=FIXED_STRING(8))'
        resp = self.execute(query)
        self.check_resp_succeeded(resp)
        time.sleep(self.delay)

        query = 'CREATE USER admin WITH PASSWORD "admin"'
        resp = self.execute(query)
        self.check_resp_succeeded(resp)
        time.sleep(self.delay)

        ret, self.adminClient = self.spawn_nebula_client_and_auth('admin', 'admin')
        assert ret

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

        query = 'DROP USER IF EXISTS admin'
        resp = self.adminClient.execute(query)
        self.check_resp_failed(resp, ttypes.ErrorCode.E_BAD_PERMISSION)

        query = 'DROP USER IF EXISTS admin'
        resp = self.execute(query)
        self.check_resp_succeeded(resp)

    def test_schema_and_data(self):
        query = 'CREATE SPACE space2(partition_num=1, replica_factor=1, vid_type=FIXED_STRING(8))'
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

        # user without role
        query = 'CREATE USER nop WITH PASSWORD "nop"'
        resp = self.execute(query)
        self.check_resp_succeeded(resp)

        time.sleep(self.delay)
        ret, self.adminClient = self.spawn_nebula_client_and_auth('admin', 'admin')
        assert ret

        ret, self.dbaClient = self.spawn_nebula_client_and_auth('dba', 'dba')
        assert ret

        ret, self.userClient = self.spawn_nebula_client_and_auth('user', 'user')
        assert ret

        ret, self.guestClient = self.spawn_nebula_client_and_auth('guest', 'guest')
        assert ret

        ret, self.nopClient = self.spawn_nebula_client_and_auth('nop', 'nop')
        assert ret

        # god write schema test
        query = 'USE space2'
        resp = self.execute(query)
        self.check_resp_succeeded(resp)

        query = 'CREATE TAG t1(t_c int)'
        resp = self.execute(query)
        self.check_resp_succeeded(resp)


        query = 'CREATE EDGE e1(e_c int)'
        resp = self.execute(query)
        self.check_resp_succeeded(resp)

        query = 'CREATE TAG INDEX tid1 ON t1(t_c)'
        resp = self.execute(query)
        self.check_resp_succeeded(resp)

        query = 'CREATE EDGE INDEX eid1 ON e1(e_c)'
        resp = self.execute(query)
        self.check_resp_succeeded(resp)

        query = 'DESCRIBE TAG t1';
        resp = self.execute(query)
        self.check_resp_succeeded(resp)

        query = 'DESCRIBE EDGE e1'
        resp = self.execute(query)
        self.check_resp_succeeded(resp)

        query = 'DESCRIBE TAG INDEX tid1'
        resp = self.execute(query)
        self.check_resp_succeeded(resp)

        query = 'DESCRIBE EDGE INDEX eid1'
        resp = self.execute(query)
        self.check_resp_succeeded(resp)

        query = 'DROP TAG INDEX tid1'
        resp = self.execute(query)
        self.check_resp_succeeded(resp)

        query = 'DROP EDGE INDEX eid1'
        resp = self.execute(query)
        self.check_resp_succeeded(resp)

        query = 'ALTER TAG t1 DROP (t_c)'
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

        query = "CREATE TAG INDEX tid1 ON t1(t_c)";
        resp = self.adminClient.execute(query)
        self.check_resp_succeeded(resp)

        query = "CREATE EDGE INDEX eid1 ON e1(e_c)";
        resp = self.adminClient.execute(query)
        self.check_resp_succeeded(resp)

        query = "DESCRIBE TAG t1";
        resp = self.adminClient.execute(query)
        self.check_resp_succeeded(resp)

        query = "DESCRIBE EDGE e1";
        resp = self.adminClient.execute(query)
        self.check_resp_succeeded(resp)

        query = "DESCRIBE TAG INDEX tid1";
        resp = self.adminClient.execute(query)
        self.check_resp_succeeded(resp)

        query = "DESCRIBE EDGE INDEX eid1";
        resp = self.adminClient.execute(query)
        self.check_resp_succeeded(resp)

        query = "DROP TAG INDEX tid1";
        resp = self.adminClient.execute(query)
        self.check_resp_succeeded(resp)

        query = "DROP EDGE INDEX eid1";
        resp = self.adminClient.execute(query)
        self.check_resp_succeeded(resp)

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

        # dba use the not exist space to create tag
        query = 'USE not_exist_space;CREATE TAG t11(t_c int);'
        resp = self.dbaClient.execute(query)
        self.check_resp_failed(resp)
        resp.space_name() == ''

        # dba write schema test
        query = 'USE space2;CREATE TAG t1(t_c int);'
        resp = self.dbaClient.execute(query)
        self.check_resp_succeeded(resp)

        # dba use the not exist space to create tag
        query = 'USE not_exist_space;CREATE TAG t11(t_c int);'
        resp = self.dbaClient.execute(query)
        self.check_resp_failed(resp)
        resp.space_name() == 'space2'

        query = "CREATE EDGE e1(e_c int)";
        resp = self.dbaClient.execute(query)
        self.check_resp_succeeded(resp)

        query = "CREATE TAG INDEX tid1 ON t1(t_c)";
        resp = self.dbaClient.execute(query)
        self.check_resp_succeeded(resp)

        query = "CREATE EDGE INDEX eid1 ON e1(e_c)";
        resp = self.dbaClient.execute(query)
        self.check_resp_succeeded(resp)

        query = "DESCRIBE TAG t1";
        resp = self.dbaClient.execute(query)
        self.check_resp_succeeded(resp)

        query = "DESCRIBE EDGE e1";
        resp = self.dbaClient.execute(query)
        self.check_resp_succeeded(resp)

        query = "DESCRIBE TAG INDEX tid1";
        resp = self.dbaClient.execute(query)
        self.check_resp_succeeded(resp)

        query = "DESCRIBE EDGE INDEX eid1";
        resp = self.dbaClient.execute(query)
        self.check_resp_succeeded(resp)

        query = "DROP TAG INDEX tid1";
        resp = self.dbaClient.execute(query)
        self.check_resp_succeeded(resp)

        query = "DROP EDGE INDEX eid1";
        resp = self.dbaClient.execute(query)
        self.check_resp_succeeded(resp)

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

        query = "CREATE TAG INDEX tid1 ON t1(t_c)";
        resp = self.userClient.execute(query)
        self.check_resp_failed(resp, ttypes.ErrorCode.E_BAD_PERMISSION)

        query = "CREATE EDGE INDEX eid1 ON e1(e_c)";
        resp = self.userClient.execute(query)
        self.check_resp_failed(resp, ttypes.ErrorCode.E_BAD_PERMISSION)

        query = "DESCRIBE TAG t1";
        resp = self.userClient.execute(query)
        self.check_resp_failed(resp, ttypes.ErrorCode.E_EXECUTION_ERROR)

        query = "DESCRIBE EDGE e1";
        resp = self.userClient.execute(query)
        self.check_resp_failed(resp, ttypes.ErrorCode.E_EXECUTION_ERROR)

        query = "DESCRIBE TAG INDEX tid1";
        resp = self.userClient.execute(query)
        self.check_resp_failed(resp, ttypes.ErrorCode.E_EXECUTION_ERROR)

        query = "DESCRIBE EDGE INDEX eid1";
        resp = self.userClient.execute(query)
        self.check_resp_failed(resp, ttypes.ErrorCode.E_EXECUTION_ERROR)

        query = "DROP TAG INDEX tid1";
        resp = self.userClient.execute(query)
        self.check_resp_failed(resp, ttypes.ErrorCode.E_BAD_PERMISSION)

        query = "DROP EDGE INDEX eid1";
        resp = self.userClient.execute(query)
        self.check_resp_failed(resp, ttypes.ErrorCode.E_BAD_PERMISSION)

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

        query = "CREATE TAG INDEX tid1 ON t1(t_c)";
        resp = self.guestClient.execute(query)
        self.check_resp_failed(resp, ttypes.ErrorCode.E_BAD_PERMISSION)

        query = "CREATE EDGE INDEX eid1 ON e1(e_c)";
        resp = self.guestClient.execute(query)
        self.check_resp_failed(resp, ttypes.ErrorCode.E_BAD_PERMISSION)

        query = "DESCRIBE TAG t1";
        resp = self.guestClient.execute(query)
        self.check_resp_failed(resp, ttypes.ErrorCode.E_EXECUTION_ERROR)

        query = "DESCRIBE EDGE e1";
        resp = self.guestClient.execute(query)
        self.check_resp_failed(resp, ttypes.ErrorCode.E_EXECUTION_ERROR)

        query = "DESCRIBE TAG INDEX tid1";
        resp = self.guestClient.execute(query)
        self.check_resp_failed(resp, ttypes.ErrorCode.E_EXECUTION_ERROR)

        query = "DESCRIBE EDGE INDEX eid1";
        resp = self.guestClient.execute(query)
        self.check_resp_failed(resp, ttypes.ErrorCode.E_EXECUTION_ERROR)

        query = "DROP TAG INDEX tid1";
        resp = self.guestClient.execute(query)
        self.check_resp_failed(resp, ttypes.ErrorCode.E_BAD_PERMISSION)

        query = "DROP EDGE INDEX eid1";
        resp = self.guestClient.execute(query)
        self.check_resp_failed(resp, ttypes.ErrorCode.E_BAD_PERMISSION)

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

        # no role write schema test
        query = 'USE space2'
        resp = self.nopClient.execute(query)
        self.check_resp_failed(resp, ttypes.ErrorCode.E_BAD_PERMISSION)

        # Can't use space
        # query = "CREATE TAG t1(t_c int)";
        # resp = self.nopClient.execute(query)
        # self.check_resp_failed(resp, ttypes.ErrorCode.E_BAD_PERMISSION)

        # query = "CREATE EDGE e1(e_c int)";
        # resp = self.nopClient.execute(query)
        # self.check_resp_failed(resp, ttypes.ErrorCode.E_BAD_PERMISSION)

        # query = "CREATE TAG INDEX tid1 ON t1(t_c)";
        # resp = self.nopClient.execute(query)
        # self.check_resp_failed(resp, ttypes.ErrorCode.E_BAD_PERMISSION)

        # query = "CREATE EDGE INDEX eid1 ON e1(e_c)";
        # resp = self.nopClient.execute(query)
        # self.check_resp_failed(resp, ttypes.ErrorCode.E_BAD_PERMISSION)

        # query = "DESCRIBE TAG t1";
        # resp = self.nopClient.execute(query)
        # self.check_resp_failed(resp, ttypes.ErrorCode.E_EXECUTION_ERROR)

        # query = "DESCRIBE EDGE e1";
        # resp = self.nopClient.execute(query)
        # self.check_resp_failed(resp, ttypes.ErrorCode.E_EXECUTION_ERROR)

        # query = "DESCRIBE TAG INDEX tid1";
        # resp = self.nopClient.execute(query)
        # self.check_resp_failed(resp, ttypes.ErrorCode.E_EXECUTION_ERROR)

        # query = "DESCRIBE EDGE INDEX eid1";
        # resp = self.nopClient.execute(query)
        # self.check_resp_failed(resp, ttypes.ErrorCode.E_EXECUTION_ERROR)

        # query = "DROP TAG INDEX tid1";
        # resp = self.nopClient.execute(query)
        # self.check_resp_failed(resp, ttypes.ErrorCode.E_BAD_PERMISSION)

        # query = "DROP EDGE INDEX eid1";
        # resp = self.nopClient.execute(query)
        # self.check_resp_failed(resp, ttypes.ErrorCode.E_BAD_PERMISSION)

        # query = "ALTER TAG t1 DROP (t_c)";
        # resp = self.nopClient.execute(query)
        # self.check_resp_failed(resp, ttypes.ErrorCode.E_BAD_PERMISSION)

        # query = "ALTER EDGE e1 DROP (e_c)";
        # resp = self.nopClient.execute(query)
        # self.check_resp_failed(resp, ttypes.ErrorCode.E_BAD_PERMISSION)

        # query = "DROP TAG t1";
        # resp = self.nopClient.execute(query)
        # self.check_resp_failed(resp, ttypes.ErrorCode.E_BAD_PERMISSION)

        # query = "DROP EDGE e1";
        # resp = self.nopClient.execute(query)
        # self.check_resp_failed(resp, ttypes.ErrorCode.E_BAD_PERMISSION)

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

        query = 'GO FROM "1" OVER e1 YIELD e1._dst';
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

        query = 'GO FROM "1" OVER e1 YIELD e1._dst';
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

        query = 'GO FROM "1" OVER e1 YIELD e1._dst';
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

        query = 'GO FROM "1" OVER e1 YIELD e1._dst';
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

        query = 'GO FROM "1" OVER e1 YIELD e1._dst';
        resp = self.guestClient.execute(query)
        self.check_resp_succeeded(resp)

        # no role write data test
        query = 'USE space2'
        resp = self.nopClient.execute(query)
        self.check_resp_failed(resp, ttypes.ErrorCode.E_BAD_PERMISSION)

        # can't use space
        # query = 'INSERT VERTEX t1(t_c) VALUES "1":(1)'
        # resp = self.nopClient.execute(query)
        # self.check_resp_failed(resp, ttypes.ErrorCode.E_BAD_PERMISSION)

        # query = 'INSERT EDGE e1(e_c) VALUES "1" -> "2":(95)';
        # resp = self.nopClient.execute(query)
        # self.check_resp_failed(resp, ttypes.ErrorCode.E_BAD_PERMISSION)

        # query = 'GO FROM "1" OVER e1 YIELD e1._dst';
        # resp = self.nopClient.execute(query)
        # self.check_resp_succeeded(resp)

        # use space test
        query = "CREATE SPACE space3(partition_num=1, replica_factor=1, vid_type=FIXED_STRING(8))";
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

        resp = self.nopClient.execute(query)
        self.check_resp_failed(resp, ttypes.ErrorCode.E_BAD_PERMISSION)

        # roll back space
        query = 'USE space2'
        resp = self.execute(query)
        self.check_resp_succeeded(resp)

        # write data
        query = 'INSERT VERTEX t1(t_c) VALUES "1":(1)'
        resp = self.execute(query)
        self.check_resp_succeeded(resp)

        resp = self.adminClient.execute(query)
        self.check_resp_succeeded(resp)

        resp = self.dbaClient.execute(query)
        self.check_resp_succeeded(resp)

        resp = self.userClient.execute(query)
        self.check_resp_succeeded(resp)

        resp = self.guestClient.execute(query)
        self.check_resp_failed(resp, ttypes.ErrorCode.E_BAD_PERMISSION)

        # can't use space
        resp = self.nopClient.execute(query)
        self.check_resp_failed(resp, ttypes.ErrorCode.E_SEMANTIC_ERROR)

        # read data
        query = 'MATCH (v) RETURN v LIMIT 3'
        resp = self.execute(query)
        self.check_resp_succeeded(resp)

        resp = self.adminClient.execute(query)
        self.check_resp_succeeded(resp)

        resp = self.dbaClient.execute(query)
        self.check_resp_succeeded(resp)

        resp = self.userClient.execute(query)
        self.check_resp_succeeded(resp)

        resp = self.guestClient.execute(query)
        self.check_resp_succeeded(resp)

        # can't use space
        resp = self.nopClient.execute(query)
        self.check_resp_failed(resp, ttypes.ErrorCode.E_SEMANTIC_ERROR)

        # write user
        query = 'CREATE USER xxx WITH PASSWORD "xxx"'
        resp = self.execute(query)
        self.check_resp_succeeded(resp)

        query = 'DROP USER xxx'
        resp = self.execute(query)
        self.check_resp_succeeded(resp)

        query = 'CREATE USER xxx WITH PASSWORD "xxx"'
        resp = self.adminClient.execute(query)
        self.check_resp_failed(resp, ttypes.ErrorCode.E_BAD_PERMISSION)

        resp = self.dbaClient.execute(query)
        self.check_resp_failed(resp, ttypes.ErrorCode.E_BAD_PERMISSION)

        resp = self.userClient.execute(query)
        self.check_resp_failed(resp, ttypes.ErrorCode.E_BAD_PERMISSION)

        resp = self.guestClient.execute(query)
        self.check_resp_failed(resp, ttypes.ErrorCode.E_BAD_PERMISSION)

        resp = self.nopClient.execute(query)
        self.check_resp_failed(resp, ttypes.ErrorCode.E_BAD_PERMISSION)

        # write role
        query = 'CREATE USER xxx WITH PASSWORD "xxx"'
        resp = self.execute(query)
        self.check_resp_succeeded(resp)

        query = 'GRANT ROLE GUEST ON space2 TO xxx'
        resp = self.execute(query)
        self.check_resp_succeeded(resp)

        query = 'REVOKE ROLE GUEST ON space2 FROM xxx'
        resp = self.execute(query)
        self.check_resp_succeeded(resp)

        query = 'GRANT ROLE GUEST ON space2 TO xxx'
        resp = self.adminClient.execute(query)
        self.check_resp_succeeded(resp)

        query = 'REVOKE ROLE GUEST ON space2 FROM xxx'
        resp = self.adminClient.execute(query)
        self.check_resp_succeeded(resp)

        query = 'GRANT ROLE GUEST ON space2 TO xxx'
        resp = self.dbaClient.execute(query)
        self.check_resp_failed(resp, ttypes.ErrorCode.E_BAD_PERMISSION)

        resp = self.userClient.execute(query)
        self.check_resp_failed(resp, ttypes.ErrorCode.E_BAD_PERMISSION)

        resp = self.guestClient.execute(query)
        self.check_resp_failed(resp, ttypes.ErrorCode.E_BAD_PERMISSION)

        resp = self.nopClient.execute(query)
        self.check_resp_failed(resp, ttypes.ErrorCode.E_BAD_PERMISSION)

        query = 'DROP USER xxx'
        resp = self.execute(query)
        self.check_resp_succeeded(resp)

        # write space
        query = 'CREATE SPACE write_space_test(partition_num=1, replica_factor=1, vid_type=int)'
        resp = self.execute(query)
        self.check_resp_succeeded(resp)

        query = 'DROP SPACE write_space_test'
        resp = self.execute(query)
        self.check_resp_succeeded(resp)

        query = 'CREATE SPACE write_space_test(partition_num=1, replica_factor=1, vid_type=int)'
        resp = self.adminClient.execute(query)
        self.check_resp_failed(resp, ttypes.ErrorCode.E_BAD_PERMISSION)

        resp = self.dbaClient.execute(query)
        self.check_resp_failed(resp, ttypes.ErrorCode.E_BAD_PERMISSION)

        resp = self.userClient.execute(query)
        self.check_resp_failed(resp, ttypes.ErrorCode.E_BAD_PERMISSION)

        resp = self.guestClient.execute(query)
        self.check_resp_failed(resp, ttypes.ErrorCode.E_BAD_PERMISSION)

        resp = self.nopClient.execute(query)
        self.check_resp_failed(resp, ttypes.ErrorCode.E_BAD_PERMISSION)

    def test_show_test(self):
        ret, self.adminClient = self.spawn_nebula_client_and_auth('admin', 'admin')
        assert ret
        ret, self.dbaClient = self.spawn_nebula_client_and_auth('dba', 'dba')
        assert ret
        ret, self.userClient = self.spawn_nebula_client_and_auth('user', 'user')
        assert ret
        ret, self.guestClient = self.spawn_nebula_client_and_auth('guest', 'guest')
        assert ret
        ret, self.nopClient = self.spawn_nebula_client_and_auth('nop', 'nop')
        assert ret

        query = 'CREATE SPACE space4(partition_num=1, replica_factor=1, vid_type=FIXED_STRING(8))'
        resp = self.execute(query)
        self.check_resp_succeeded(resp)
        time.sleep(self.delay)

        query = 'SHOW SPACES'
        resp = self.execute(query)
        self.check_resp_succeeded(resp)
        expected_column_names = ['Name']
        expected_result = [[['test_permission_space']], [['space1']], [['space2']], [['space3']], [['space4']]]
        self.check_column_names(resp, expected_column_names)
        for row in expected_result:
            self.search_result(resp, row)

        resp = self.adminClient.execute(query)
        self.check_resp_succeeded(resp)
        expected_result = [['space2']]
        self.check_column_names(resp, expected_column_names)
        self.check_out_of_order_result(resp, expected_result)

        resp = self.dbaClient.execute(query)
        self.check_resp_succeeded(resp)
        expected_result = [['space2']]
        self.check_column_names(resp, expected_column_names)
        self.check_out_of_order_result(resp, expected_result)

        resp = self.userClient.execute(query)
        self.check_resp_succeeded(resp)
        expected_result = [['space2']]
        self.check_column_names(resp, expected_column_names)
        self.check_out_of_order_result(resp, expected_result)

        resp = self.guestClient.execute(query)
        self.check_resp_succeeded(resp)
        expected_result = [['space2']]
        self.check_column_names(resp, expected_column_names)
        self.check_out_of_order_result(resp, expected_result)

        resp = self.nopClient.execute(query)
        self.check_resp_succeeded(resp)
        expected_result = []
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
        query = 'CREATE SPACE space5(partition_num=1, replica_factor=1, vid_type=FIXED_STRING(8))'
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

        ret, self.testClient = self.spawn_nebula_client_and_auth('test', 'test')
        assert ret
        ret, self.adminClient = self.spawn_nebula_client_and_auth('admin', 'admin')
        assert ret
        ret, self.dbaClient = self.spawn_nebula_client_and_auth('dba', 'dba')
        assert ret
        ret, self.userClient = self.spawn_nebula_client_and_auth('user', 'user')
        assert ret
        ret, self.guestClient = self.spawn_nebula_client_and_auth('guest', 'guest')
        assert ret
        ret, self.nopClient = self.spawn_nebula_client_and_auth('nop', 'nop')
        assert ret

        query = 'SHOW ROLES IN space5'
        expected_result = []
        resp = self.testClient.execute(query)
        self.check_resp_failed(resp, ttypes.ErrorCode.E_BAD_PERMISSION)

        query = 'SHOW ROLES IN space5'
        expected_result = [['guest', 'GUEST'],
                           ['user', 'USER'],
                           ['dba', 'DBA'],
                           ['admin', 'ADMIN']]
        resp = self.execute(query)
        self.check_resp_succeeded(resp)
        self.check_out_of_order_result(resp, expected_result)

        resp = self.adminClient.execute(query)
        self.check_resp_succeeded(resp)
        self.check_out_of_order_result(resp, expected_result)

        expected_result = [['dba', 'DBA']]
        resp = self.dbaClient.execute(query)
        self.check_resp_succeeded(resp)
        self.check_out_of_order_result(resp, expected_result)

        expected_result = [['user', 'USER']]
        resp = self.userClient.execute(query)
        self.check_resp_succeeded(resp)
        self.check_out_of_order_result(resp, expected_result)

        expected_result = [['guest', 'GUEST']]
        resp = self.guestClient.execute(query)
        self.check_resp_succeeded(resp)
        self.check_out_of_order_result(resp, expected_result)

        expected_result = []
        resp = self.nopClient.execute(query)
        self.check_resp_failed(resp, ttypes.ErrorCode.E_BAD_PERMISSION)

        # clean up
        query = 'DROP SPACE IF EXISTS space5'
        resp = self.execute(query)
        self.check_resp_succeeded(resp)
