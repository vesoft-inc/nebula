# --coding:utf-8--
#
# Copyright (c) 2020 vesoft inc. All rights reserved.
#
# This source code is licensed under Apache 2.0 License,
# attached with Common Clause Condition 1.0, found in the LICENSES directory.

import time
import re

from tests.common.nebula_test_suite import NebulaTestSuite


class TestLabelExpr(NebulaTestSuite):
    @classmethod
    def prepare(self):
        resp = self.execute('CREATE SPACE test_label_expr(partition_num=1);'
                            'USE test_label_expr;')
        self.check_resp_succeeded(resp)

        resp = self.execute('CREATE TAG person(name string, age int); '
                            'CREATE EDGE friend(start int)')
        self.check_resp_succeeded(resp)

        time.sleep(self.delay)

        resp = self.execute('INSERT VERTEX person(name, age) values "a":("a", 2), "b":("b", 3);')
        self.check_resp_succeeded(resp)
        resp = self.execute('INSERT EDGE friend(start) values "a"->"b":(2010);')
        self.check_resp_succeeded(resp)

    @classmethod
    def cleanup(self):
        resp = self.execute('DROP SPACE test_label_expr')
        self.check_resp_succeeded(resp)

    def test_wrong_props(self):
        # fetch vertex with label expr
        resp = self.execute('FETCH PROP ON person "a" YIELD name')
        self.check_error_msg(resp, "SemanticError: Invalid label identifiers: name")

        resp = self.execute('FETCH PROP ON person "a" YIELD name + 1')
        self.check_error_msg(resp, "SemanticError: Invalid label identifiers: name")

        # fetch edge with label expr
        resp = self.execute('FETCH PROP ON friend "a"->"b" YIELD start')
        self.check_error_msg(resp, "SemanticError: Invalid label identifiers: start")

        # go with label expr
        resp = self.execute('GO FROM "a" OVER friend YIELD name')
        self.check_error_msg(resp, "SemanticError: Invalid label identifiers: name")

        # yield sentence with label expr
        resp = self.execute('YIELD name')
        self.check_error_msg(resp, "SemanticError: Invalid label identifiers: name")
