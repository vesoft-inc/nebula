# --coding:utf-8--
#
# Copyright (c) 2020 vesoft inc. All rights reserved.
#
# This source code is licensed under Apache 2.0 License.

import sys
import time
import concurrent

from nebula3.gclient.net import Connection
from nebula3.common import ttypes
from nebula3.data.ResultSet import ResultSet
from tests.common.nebula_test_suite import NebulaTestSuite


class TestSession(NebulaTestSuite):
    @classmethod
    def prepare(self):
        resp = self.execute(
            'CREATE USER IF NOT EXISTS session_user WITH PASSWORD "123456"'
        )
        self.check_resp_succeeded(resp)

        query = 'GRANT ROLE ADMIN ON nba TO session_user'
        resp = self.execute(query)
        self.check_resp_succeeded(resp)

        resp = self.execute(
            'CREATE USER IF NOT EXISTS session_user1 WITH PASSWORD "123456"'
        )
        self.check_resp_succeeded(resp)

        query = 'GRANT ROLE ADMIN ON nba TO session_user1'
        resp = self.execute(query)
        self.check_resp_succeeded(resp)

        resp = self.execute(
            'CREATE USER IF NOT EXISTS session_user2 WITH PASSWORD "123456"'
        )
        self.check_resp_succeeded(resp)

        query = 'GRANT ROLE ADMIN ON nba TO session_user2'
        resp = self.execute(query)
        self.check_resp_succeeded(resp)

        resp = self.execute('SHOW HOSTS GRAPH')
        self.check_resp_succeeded(resp)
        assert not resp.is_empty()
        assert resp.row_size() == 2
        self.addr_host1 = resp.row_values(0)[0].as_string()
        self.addr_port1 = resp.row_values(0)[1].as_int()
        self.addr_host2 = resp.row_values(1)[0].as_string()
        self.addr_port2 = resp.row_values(1)[1].as_int()

        resp = self.execute('UPDATE CONFIGS graph:session_reclaim_interval_secs = 1')
        self.check_resp_succeeded(resp)
        time.sleep(3)

    @classmethod
    def cleanup(self):
        resp = self.execute('UPDATE CONFIGS graph:session_reclaim_interval_secs = 2')
        self.check_resp_succeeded(resp)
        time.sleep(3)

        session = self.client_pool.get_session('root', 'nebula')
        resp = session.execute('DROP USER session_user')
        self.check_resp_succeeded(resp)
        resp = session.execute('DROP USER session_user1')
        self.check_resp_succeeded(resp)
        resp = session.execute('DROP USER session_user2')
        self.check_resp_succeeded(resp)

    def test_sessions(self):
        # 1: test add session with right username
        try:
            user_session = self.client_pool.get_session('session_user', '123456')
            assert user_session is not None
            assert True
        except Exception as e:
            assert False, e

        # 2: test add session with not exist username
        try:
            self.client_pool.get_session('session_not_exist', '123456')
            assert False
        except Exception as e:
            assert True

        # 3: test show sessions
        resp = self.execute('SHOW SESSIONS')
        self.check_resp_succeeded(resp)
        expect_col_names = [
            'SessionId',
            'UserName',
            'SpaceName',
            'CreateTime',
            'UpdateTime',
            'GraphAddr',
            'Timezone',
            'ClientIp',
        ]
        self.check_column_names(resp, expect_col_names)

        session_id = 0
        for row in resp.rows():
            if bytes.decode(row.values[1].get_sVal()) == 'session_user':
                session_id = row.values[0].get_iVal()
                assert row.values[2].get_sVal() == b'', f"resp: {resp}"
                assert row.values[3].getType() == ttypes.Value.DTVAL, f"resp: {resp}"
                assert row.values[4].getType() == ttypes.Value.DTVAL, f"resp: {resp}"
                break

        assert session_id != 0

        # 4: test get session info
        resp = user_session.execute('USE nba')
        self.check_resp_succeeded(resp)

        # wait for session sync.
        time.sleep(3)
        resp = self.execute('SHOW SESSION {}'.format(session_id))
        self.check_resp_succeeded(resp)
        expect_col_names = [
            'SessionId',
            'UserName',
            'SpaceName',
            'CreateTime',
            'UpdateTime',
            'GraphAddr',
            'Timezone',
            'ClientIp',
        ]

        self.check_column_names(resp, expect_col_names)

        assert len(resp.rows()) == 1

        row = resp.rows()[0]
        assert row.values[0].get_iVal() == session_id
        assert row.values[1].get_sVal() == b'session_user'
        assert row.values[2].get_sVal() == b'nba'
        assert row.values[3].getType() == ttypes.Value.DTVAL, f"resp: {resp}"
        assert row.values[4].getType() == ttypes.Value.DTVAL, f"resp: {resp}"

        # 5: test expired session
        resp = self.execute('UPDATE CONFIGS graph:session_idle_timeout_secs = 5')
        self.check_resp_succeeded(resp)
        time.sleep(3)
        # to wait for session expires
        for i in range(3):
            resp = self.execute('SHOW SPACES;')
            self.check_resp_succeeded(resp)
            time.sleep(3)
        resp = self.execute('SHOW SESSION {}'.format(session_id))
        self.check_resp_failed(resp, ttypes.ErrorCode.E_EXECUTION_ERROR)
        resp = self.execute('UPDATE CONFIGS graph:session_idle_timeout_secs = 28800')
        self.check_resp_succeeded(resp)
        time.sleep(3)

        # 6: test privilege
        # show sessions with non-root user, only the root user can show all sessions
        try:
            non_root_session = self.client_pool.get_session('session_user', '123456')
            assert non_root_session is not None
            resp = non_root_session.execute('SHOW SESSIONS')
            self.check_resp_failed(resp)
        except Exception as e:
            assert False, e

    def test_the_same_id_to_different_graphd(self):

        conn1 = self.get_connection(self.addr_host1, self.addr_port1)
        conn2 = self.get_connection(self.addr_host2, self.addr_port2)

        resp = conn1.authenticate('root', 'nebula')
        session_id = resp.get_session_id()

        resp = conn1.execute(
            session_id,
            'CREATE SPACE IF NOT EXISTS aSpace(partition_num=1, vid_type=FIXED_STRING(8));USE aSpace;',
        )
        self.check_resp_succeeded(ResultSet(resp, 0))
        # time::WallClock::fastNowInMicroSec() is not synchronous in different process,
        # so we sleep 3 seconds here and charge session
        time.sleep(3)
        resp = conn1.execute(session_id, 'USE aSpace;')
        self.check_resp_succeeded(ResultSet(resp, 0))
        time.sleep(3)
        # We actually not allowed share sessions, this only for testing the scenario of transfer sessions.
        resp = conn1.execute(session_id, 'CREATE TAG IF NOT EXISTS a();')
        self.check_resp_succeeded(ResultSet(resp, 0))
        resp = conn2.execute(session_id, 'CREATE TAG IF NOT EXISTS b();')
        self.check_resp_succeeded(ResultSet(resp, 0))

        def do_test(connection, sid, num):
            result = connection.execute(sid, 'USE aSpace;')
            assert result.error_code == ttypes.ErrorCode.SUCCEEDED
            result = connection.execute(
                sid, 'CREATE TAG IF NOT EXISTS aa{}()'.format(num)
            )
            assert result.error_code == ttypes.ErrorCode.SUCCEEDED, result.error_msg

        # test multi connection with the same session_id
        test_jobs = []
        with concurrent.futures.ThreadPoolExecutor(3) as executor:
            for i in range(0, 3):
                future = executor.submit(
                    do_test,
                    self.get_connection(self.addr_host2, self.addr_port2),
                    session_id,
                    i,
                )
                test_jobs.append(future)

            for future in concurrent.futures.as_completed(test_jobs):
                assert future.exception() is None, future.exception()
        resp = conn2.execute(session_id, 'SHOW TAGS')
        self.check_resp_succeeded(ResultSet(resp, 0))
        expect_result = [['a'], ['b'], ['aa0'], ['aa1'], ['aa2']]
        self.check_out_of_order_result(ResultSet(resp, 0), expect_result)

    def test_out_of_max_connections(self):
        resp = self.execute('SHOW SESSIONS')
        self.check_resp_succeeded(resp)
        sessions = len(resp.rows())

        resp = self.execute(
            'UPDATE CONFIGS graph:max_allowed_connections = {}'.format(sessions)
        )
        self.check_resp_succeeded(resp)
        time.sleep(3)

        # get new session failed
        try:
            self.client_pool.get_session(self.user, self.password)
            assert False
        except Exception as e:
            assert True
            assert e.message == "Auth failed: b'Too many connections in the cluster'"

        resp = self.execute(
            'UPDATE CONFIGS graph:max_allowed_connections = {}'.format(sys.maxsize)
        )
        self.check_resp_succeeded(resp)
        time.sleep(3)

    def test_signout_and_execute(self):
        try:
            conn = self.client_pool.get_connection()
            auth_result = conn.authenticate(self.user, self.password)
            session_id = auth_result.get_session_id()
            conn.signout(session_id)
        except Exception as e:
            assert False, e.message

        time.sleep(2)
        resp = conn.execute(session_id, 'SHOW HOSTS')
        assert resp.error_code == ttypes.ErrorCode.E_SESSION_INVALID, resp.error_msg
        assert resp.error_msg.find(b'Session not existed!') > 0

    def test_out_of_max_sessions_per_ip_per_user(self):
        resp = self.execute('SHOW SESSIONS')
        self.check_resp_succeeded(resp)
        sessions = len(resp.rows())

        i = 0
        session_user1_count = 0
        while i < sessions:
            row = resp.rows()[i]
            if row.values[1].get_sVal() == b'session_user1':
                session_user1_count += 1
            i += 1

        session_limit = max(3, sessions)
        resp = self.execute(
            'UPDATE CONFIGS graph:max_sessions_per_ip_per_user = {}'.format(
                session_limit
            )
        )
        self.check_resp_succeeded(resp)
        time.sleep(3)

        # get new session failed for user session_user1
        try:
            i = 0
            while i < session_limit - session_user1_count:
                self.client_pool.get_session("session_user1", "123456")
                i += 1
            self.client_pool.get_session("session_user1", "123456")
            assert False
        except Exception as e:
            assert True

        # get new session success from session_user2
        try:
            self.client_pool.get_session("session_user2", "123456")
            self.check_resp_succeeded(resp)
            assert True
        except Exception as e:
            assert False, e.message

        resp = self.execute('UPDATE CONFIGS graph:max_sessions_per_ip_per_user = 300')
        self.check_resp_succeeded(resp)
        time.sleep(3)

    def test_kill_session_basic(self):
        # find all sessions
        resp = self.execute('SHOW SESSIONS')
        self.check_resp_succeeded(resp)
        total_session_num = len(resp.rows())
        assert total_session_num > 0

        # kill the newest session
        resp = self.execute(
            'SHOW SESSIONS | YIELD $-.SessionId AS sid, $-.CreateTime as CreateTime | ORDER BY $-.CreateTime DESC | LIMIT  1 | KILL SESSION  $-.sid'
        )
        self.check_resp_succeeded(resp)
        assert not resp.is_empty()
        # check the number of session is killed
        assert resp.rows()[0].values[0].get_iVal() == 1

        # check the total number sessions left
        resp = self.execute('SHOW SESSIONS')
        self.check_resp_succeeded(resp)
        total_session_num_after_kill_session = len(resp.rows())
        assert total_session_num_after_kill_session == total_session_num - 1

        # wrong type of session id
        resp = self.execute('KILL SESSION "123"')
        self.check_resp_failed(resp, ttypes.ErrorCode.E_SEMANTIC_ERROR)

        # execute as non root user
        try:
            non_root_session = self.client_pool.get_session('session_user', '123456')
            assert non_root_session is not None
            resp = non_root_session.execute('KILL SESSION 123')
            self.check_resp_failed(resp, ttypes.ErrorCode.E_BAD_PERMISSION)
            assert resp.error_msg().find('No permission to kill session') > 0
        except Exception as e:
            assert False, e.message

        # kill current session
        try:
            cur_session = self.client_pool.get_session('root', 'nebula')
            assert cur_session is not None

            # kill the newest session which is also the current session
            resp = cur_session.execute(
                'SHOW SESSIONS | YIELD $-.SessionId AS sid, $-.CreateTime as CreateTime | ORDER BY $-.CreateTime DESC | LIMIT  1 | KILL SESSION  $-.sid'
            )
            self.check_resp_succeeded(resp)

            # execute after kill current session
            resp = cur_session.execute('SHOW HOSTS')
            self.check_resp_failed(resp, ttypes.ErrorCode.E_SESSION_INVALID)
        except Exception as e:
            assert False, e.message

        # kill invalid/non-existed session
        try:
            resp = self.execute('KILL SESSION 100')
            self.check_resp_succeeded(resp)
            # the result should indicate 0 session is killed
            assert resp.rows()[0].values[0].get_iVal() == 0
        except Exception as e:
            assert False, e.message

        # kill multiple sessions at once
        resp = self.execute(
            'SHOW SESSIONS | YIELD $-.SessionId as sid WHERE $-.UserName == "session_user1"'
        )
        self.check_resp_succeeded(resp)
        user1_session_num = len(resp.rows())

        # create a total of 100 sessions for user1
        while user1_session_num < 100:
            conn1 = self.get_connection(self.addr_host1, self.addr_port1)
            try:
                # get session from host1
                conn1.authenticate('session_user1', '123456')
            except Exception as e:
                assert False, e.message
            user1_session_num += 1

        resp = self.execute(
            'SHOW SESSIONS | YIELD $-.SessionId as sid WHERE $-.UserName == "session_user1"'
        )
        self.check_resp_succeeded(resp)
        assert len(resp.rows()) == 100

        # kill all sessions of user1
        resp = self.execute(
            'SHOW SESSIONS | YIELD $-.SessionId as sid WHERE $-.UserName == "session_user1" | KILL SESSION $-.sid'
        )
        self.check_resp_succeeded(resp)
        assert resp.rows()[0].values[0].get_iVal() == 100

        # check the sessions left for user1
        resp = self.execute(
            'SHOW SESSIONS | YIELD $-.SessionId as sid WHERE $-.UserName == "session_user1"'
        )
        self.check_resp_succeeded(resp)
        assert len(resp.rows()) == 0

    # kill a session in another host
    def test_kill_session_multi_graph(self):
        conn1 = self.get_connection(self.addr_host1, self.addr_port1)
        conn2 = self.get_connection(self.addr_host2, self.addr_port2)

        try:
            # get session from host1
            auth_resp = conn1.authenticate('root', 'nebula')
            session_id1 = auth_resp.get_session_id()

            # get session from host2
            auth_resp = conn2.authenticate('session_user1', '123456')
            session_id2 = auth_resp.get_session_id()

            # check the number of sessions for user1
            resp = conn1.execute(
                session_id1,
                'SHOW SESSIONS | YIELD $-.SessionId as sid WHERE $-.UserName == "session_user1"',
            )
            self.check_resp_succeeded(ResultSet(resp, 0))
            user1_session_num = len(ResultSet(resp, 0).rows())

            # kill the session created in host2 from host1
            resp = conn1.execute(
                session_id1,
                'KILL SESSION {}'.format(session_id2),
            )
            self.check_resp_succeeded(ResultSet(resp, 0))
            assert ResultSet(resp, 0).rows()[0].values[0].get_iVal() == 1

            # check the number of sessions for user1 remained
            resp = conn1.execute(
                session_id1,
                'SHOW SESSIONS | YIELD $-.SessionId as sid WHERE $-.UserName == "session_user1"',
            )
            self.check_resp_succeeded(ResultSet(resp, 0))
            assert user1_session_num == len(ResultSet(resp, 0).rows()) + 1

            # wait for the session to be synced (in test session_reclaim_interval_secs=2)
            # and execute a query with the killed session
            time.sleep(4)
            resp = conn2.execute(
                session_id2,
                'SHOW HOSTS',
            )
            self.check_resp_failed(
                ResultSet(resp, 0), ttypes.ErrorCode.E_SESSION_INVALID
            )
        except Exception as e:
            assert False, e
