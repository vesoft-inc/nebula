# --coding:utf-8--
#
# Copyright (c) 2019 vesoft inc. All rights reserved.
#
# This source code is licensed under Apache 2.0 License,
# attached with Common Clause Condition 1.0, found in the LICENSES directory.

import re
import sys
import time

from graph import ttypes
from tests.common.nebula_test_suite import NebulaTestSuite


class TestNebula(NebulaTestSuite):
    @classmethod
    def prepare(self):
        resp = self.execute('CREATE SPACE IF NOT EXISTS space1')
        self.check_resp_successed(resp)

    def test_schema(self):
        resp = self.execute('USE space1')
        self.check_resp_successed(resp)
        resp = self.execute(
            'CREATE TAG IF NOT EXISTS person(name string, age int)')
        self.check_resp_successed(resp)

        self.execute('CREATE EDGE IF NOT EXISTS like(likeness double)')
        self.check_resp_successed(resp)

        resp = self.execute_query('SHOW TAGS')
        self.check_resp_successed(resp)
        self.search_result(resp.rows, [[re.compile(r'\d+'), 'person']])

        resp = self.execute_query('SHOW EDGES')
        self.check_resp_successed(resp)
        self.check_result(resp.rows, [[re.compile(r'\d+'), 'like']])

    def test_insert(self):
        resp = self.execute('USE space1')
        self.check_resp_successed(resp)
        time.sleep(3)

        resp = self.execute(
            'INSERT VERTEX person(name, age) VALUES 1:(\'Bob\', 10)')
        self.check_resp_successed(resp)

        resp = self.execute(
            'INSERT VERTEX person(name, age) VALUES 2:(\'Lily\', 9)')
        self.check_resp_successed(resp)

        resp = self.execute(
            'INSERT VERTEX person(name, age) VALUES 3:(\'Tom\', 10)')
        self.check_resp_successed(resp)

        resp = self.execute('INSERT EDGE like(likeness) VALUES 1->2:(80.0)')
        self.check_resp_successed(resp)

        resp = self.execute('INSERT EDGE like(likeness) VALUES 1->3:(90.0)')
        self.check_resp_successed(resp)

        resp = self.execute('INSERT EDGE like(likeness) VALUES 2->3:(100.0)')
        self.check_resp_successed(resp)

    def test_query(self):
        time.sleep(3)
        resp = self.execute('USE space1')
        assert resp.error_code == 0

        resp = self.execute_query('GO FROM 1 OVER like YIELD $$.person.name, '
                                  '$$.person.age, like.likeness')
        assert resp.error_code == 0

        expect_result = [['Lily', 9, 80.0], ['Tom', 10, 90.0]]
        self.check_result(resp.rows, expect_result)
        expect_result_OO = [['Tom', 10, 90.0], ['Lily', 9, 80.0]]
        self.search_result(resp.rows, expect_result_OO)

    def test_path(self):
        time.sleep(3)
        resp = self.execute('USE space1')
        assert resp.error_code == 0

        resp = self.execute_query('FIND SHORTEST PATH FROM 1 to 3 OVER *')
        assert resp.error_code == 0

        path = ttypes.Path()
        pathEntry1 = ttypes.PathEntry()
        vertex1 = ttypes.Vertex()
        vertex1.id = 1
        pathEntry1.set_vertex(vertex1)
        pathEntry2 = ttypes.PathEntry()
        edge = ttypes.Edge()
        edge.type = b"like"
        edge.ranking = 0
        pathEntry2.set_edge(edge)
        vertex2 = ttypes.Vertex()
        vertex2.id = 3
        pathEntry3 = ttypes.PathEntry()
        pathEntry3.set_vertex(vertex2)
        path.entry_list = [pathEntry1, pathEntry2, pathEntry3]

        expect_result = [[path]]
        self.check_result(resp.rows, expect_result)

    @classmethod
    def cleanup(self):
        resp = self.execute('drop space space1')
        self.check_resp_successed(resp)
