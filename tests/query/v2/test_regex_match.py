# --coding:utf-8--
#
# Copyright (c) 2020 vesoft inc. All rights reserved.
#
# This source code is licensed under Apache 2.0 License,
# attached with Common Clause Condition 1.0, found in the LICENSES directory.

from tests.common.nebula_test_suite import NebulaTestSuite
from tests.common.nebula_test_suite import T_NULL, T_EMPTY, T_NULL_BAD_TYPE
import pytest

class TestRegexMatch(NebulaTestSuite):
    @classmethod
    def prepare(self):
        self.use_nba()

    def test_regex_match_with_yield(self):
        resp = self.execute(r'''
            YIELD "abcd\xA3g1234efgh\x49ijkl" =~ "\\w{4}\xA3g12\\d*e\\w+\x49\\w+"
        ''')
        self.check_resp_succeeded(resp)
        expected_data = [[True]]
        self.check_result(resp, expected_data)

        resp = self.execute(r'YIELD "Tony Parker" =~ "T\\w+\\s\\w+"')
        self.check_resp_succeeded(resp)
        expected_data = [[True]]
        self.check_result(resp, expected_data)

        resp = self.execute(r'YIELD "010-12345" =~ "\\d{3}\\-\\d{3,8}"')
        self.check_resp_succeeded(resp)
        expected_data = [[True]]
        self.check_result(resp, expected_data)

        resp = self.execute(r'''
            YIELD "test_space_128" =~ "[a-zA-Z_][0-9a-zA-Z_]{0,19}"
        ''')
        self.check_resp_succeeded(resp)
        expected_data = [[True]]
        self.check_result(resp, expected_data)

        resp = self.execute(r'''
            YIELD "2001-09-01 08:00:00" =~ "\\d+\\-0\\d?\\-\\d+\\s\\d+:00:\\d+"
        ''')
        self.check_resp_succeeded(resp)
        expected_data = [[True]]
        self.check_result(resp, expected_data)

        resp = self.execute(r'YIELD "2019" =~ "\\d+"')
        self.check_resp_succeeded(resp)
        expected_data = [[True]]
        self.check_result(resp, expected_data)

        resp = self.execute(r'''
            YIELD "jack138tom发abc数据库烫烫烫" =~ "j\\w*\\d+\\w+\u53d1[a-c]+\u6570\u636e\u5e93[\x70EB]+"
        ''')
        self.check_resp_succeeded(resp)
        expected_data = [[True]]
        self.check_result(resp, expected_data)

        resp = self.execute(r'YIELD "a good person" =~ "a\\s\\w+"')
        self.check_resp_succeeded(resp)
        expected_data = [[False]]
        self.check_result(resp, expected_data)

        resp = self.execute(r'YIELD "Trail Blazers" =~ "\\w+"')
        self.check_resp_succeeded(resp)
        expected_data = [[False]]
        self.check_result(resp, expected_data)

        resp = self.execute(r'YIELD "Tony No.1" =~ "\\w+No\\.\\d+"')
        self.check_resp_succeeded(resp)
        expected_data = [[False]]
        self.check_result(resp, expected_data)

    def test_regex_match_with_go(self):
        resp = self.execute(r'''
            GO FROM "Tracy McGrady" OVER like
            WHERE $$.player.name =~ "\\w+\\s?.*" YIELD $$.player.name
        ''')
        self.check_resp_succeeded(resp)
        expected_data = [["Kobe Bryant"], ["Grant Hill"], ["Rudy Gay"]]
        self.check_out_of_order_result(resp, expected_data)

        resp = self.execute(r'''
            GO FROM "Marco Belinelli" OVER serve
            WHERE $$.team.name =~ "\\d+\\w+" YIELD $$.team.name
        ''')
        self.check_resp_succeeded(resp)
        expected_data = [["76ers"]]
        self.check_out_of_order_result(resp, expected_data)

        resp = self.execute(r'''
            GO FROM "Yao Ming" OVER like
            WHERE $$.player.name =~ "\\w+\\s?\\w+'\\w+" YIELD $$.player.name
        ''')
        self.check_resp_succeeded(resp)
        expected_data = [["Shaquile O'Neal"]]
        self.check_out_of_order_result(resp, expected_data)
