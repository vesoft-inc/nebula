# --coding:utf-8--
#
# Copyright (c) 2020 vesoft inc. All rights reserved.
#
# This source code is licensed under Apache 2.0 License,
# attached with Common Clause Condition 1.0, found in the LICENSES directory.

from tests.common.nebula_test_suite import NebulaTestSuite


class TestFindPathQuery(NebulaTestSuite):
    @classmethod
    def prepare(self):
        self.use_student_space()

    def test_shortest(self):
        # FIND SHORTEST PATH OVER b'is_schoolmate'
        cmd = 'FIND SHORTEST PATH FROM 1004 TO 1007 OVER is_schoolmate;'
        resp = self.execute(cmd)

        expect_result = [[1004, (b'is_schoolmate', 0), 1007]]
        print(cmd)
        self.check_resp_succeeded(resp)
        self.check_path_result(resp.rows, expect_result)

    def test_all(self):
        # FIND ALL PATH OVER b'is_schoolmate'
        cmd = 'FIND ALL PATH FROM 1004 TO 1007 OVER is_schoolmate;'
        resp = self.execute(cmd)

        expect_result = [[1004, (b'is_schoolmate', 0), 1007],
            [1004, (b'is_schoolmate', 0), 1005, (b'is_schoolmate', 0), 1007],
            [1004, (b'is_schoolmate', 0), 1006, (b'is_schoolmate', 0), 1007],
            [1004, (b'is_schoolmate', 0), 1006, (b'is_schoolmate', 0), 1004, (b'is_schoolmate', 0), 1007],
            [1004, (b'is_schoolmate', 0), 1005, (b'is_schoolmate', 0), 1004, (b'is_schoolmate', 0), 1007],
            [1004, (b'is_schoolmate', 0), 1006, (b'is_schoolmate', 0), 1004, (b'is_schoolmate', 0), 1005, (b'is_schoolmate', 0), 1007],
            [1004, (b'is_schoolmate', 0), 1006, (b'is_schoolmate', 0), 1004, (b'is_schoolmate', 0), 1006, (b'is_schoolmate', 0), 1007],
            [1004, (b'is_schoolmate', 0), 1005, (b'is_schoolmate', 0), 1004, (b'is_schoolmate', 0), 1005, (b'is_schoolmate', 0), 1007],
            [1004, (b'is_schoolmate', 0), 1005, (b'is_schoolmate', 0), 1004, (b'is_schoolmate', 0), 1006, (b'is_schoolmate', 0), 1007],
            [1004, (b'is_schoolmate', 0), 1006, (b'is_schoolmate', 0), 1004, (b'is_schoolmate', 0), 1005, (b'is_schoolmate', 0), 1004, (b'is_schoolmate', 0), 1007],
            [1004, (b'is_schoolmate', 0), 1005, (b'is_schoolmate', 0), 1004, (b'is_schoolmate', 0), 1005, (b'is_schoolmate', 0), 1004, (b'is_schoolmate', 0), 1007],
            [1004, (b'is_schoolmate', 0), 1006, (b'is_schoolmate', 0), 1004, (b'is_schoolmate', 0), 1006, (b'is_schoolmate', 0), 1004, (b'is_schoolmate', 0), 1007],
            [1004, (b'is_schoolmate', 0), 1005, (b'is_schoolmate', 0), 1004, (b'is_schoolmate', 0), 1006, (b'is_schoolmate', 0), 1004, (b'is_schoolmate', 0), 1007]]
        print(cmd)
        self.check_resp_succeeded(resp)
        self.check_path_result(resp.rows, expect_result)

    def test_all_over(self):
        # FIND ALL PATH OVER is_schoolmate UPTO 3 STEPS
        cmd = 'FIND ALL PATH FROM 1004 TO 1007 OVER is_schoolmate UPTO 3 STEPS'
        resp = self.execute(cmd)

        expect_result = [[1004, (b'is_schoolmate', 0), 1007],
            [1004, (b'is_schoolmate', 0), 1005, (b'is_schoolmate', 0), 1007],
            [1004, (b'is_schoolmate', 0), 1006, (b'is_schoolmate', 0), 1007],
            [1004, (b'is_schoolmate', 0), 1006, (b'is_schoolmate', 0), 1004, (b'is_schoolmate', 0), 1007],
            [1004, (b'is_schoolmate', 0), 1005, (b'is_schoolmate', 0), 1004, (b'is_schoolmate', 0), 1007]]
        print(cmd)
        self.check_resp_succeeded(resp)
        self.check_path_result(resp.rows, expect_result)

    def test_all_multi_edges(self):
        # FIND SHORTEST PATH OVER multi edges
        cmd = 'FIND SHORTEST PATH FROM 1016 TO 1020 OVER is_teacher, is_schoolmate, is_friend'
        resp = self.execute(cmd)

        expect_result = [[1016, (b'is_schoolmate', 0), 1017, (b'is_friend', 0), 1020],
            [1016, (b'is_schoolmate', 0), 1017, (b'is_schoolmate', 0), 1020],
            [1016, (b'is_friend', 0), 1018, (b'is_friend', 0), 1020],
            [1016, (b'is_schoolmate', 0), 1018, (b'is_friend', 0), 1020]]
        print(cmd)
        self.check_resp_succeeded(resp)
        self.check_path_result(resp.rows, expect_result)

    def test_all_multi_dst(self):
        # FIND SHORTEST PATH OVER multi dst
        cmd = 'FIND SHORTEST PATH FROM 1016 TO 1009, 1018 OVER is_friend, is_schoolmate'
        resp = self.execute(cmd)

        expect_result = [[1016, (b'is_friend', 0), 1008, (b'is_schoolmate', 0), 1009],
            [1016, (b'is_friend', 0), 1018],
            [1016, (b'is_schoolmate', 0), 1018]]
        print(cmd)
        self.check_resp_succeeded(resp)
        self.check_path_result(resp.rows, expect_result)

    def test_shortest_over_all(self):
        # FIND SHORTEST PATH OVER *
        cmd = 'FIND SHORTEST PATH FROM 1016 TO 1020 OVER *'
        resp = self.execute(cmd)

        expect_result = [[1016, (b'is_schoolmate', 0), 1017, (b'is_friend', 0), 1020],
            [1016, (b'is_schoolmate', 0), 1017, (b'is_schoolmate', 0), 1020],
            [1016, (b'is_friend', 0), 1018, (b'is_friend', 0), 1020],
            [1016, (b'is_schoolmate', 0), 1018, (b'is_friend', 0), 1020]]
        print(cmd)
        self.check_resp_succeeded(resp)
        self.check_path_result(resp.rows, expect_result)

    def test_all_over_all_multi_dst(self):
        # FIND SHORTEST PATH OVER *
        cmd = 'FIND ALL PATH FROM 1012 TO 1007, 1015 OVER is_friend, is_schoolmate'
        resp = self.execute(cmd)

        expect_result = [[1012,(b'is_schoolmate', 0), 1013, (b'is_friend',0), 1007],
            [1012, (b'is_schoolmate', 0), 1013, (b'is_schoolmate', 0), 1012, (b'is_schoolmate', 0), 1013, (b'is_friend', 0), 1007],
            [1012, (b'is_schoolmate', 0), 1015],
            [1012, (b'is_schoolmate', 0), 1014, (b'is_schoolmate', 0), 1015],
            [1012, (b'is_schoolmate', 0), 1013, (b'is_schoolmate', 0), 1012, (b'is_schoolmate', 0), 1015],
            [1012, (b'is_schoolmate', 0), 1013, (b'is_schoolmate', 0), 1012, (b'is_schoolmate', 0), 1014,(b'is_schoolmate', 0), 1015],
            [1012, (b'is_schoolmate', 0), 1013, (b'is_schoolmate', 0), 1012, (b'is_schoolmate', 0), 1013,(b'is_schoolmate', 0), 1012, (b'is_schoolmate', 0), 1015]]
        print(cmd)
        self.check_resp_succeeded(resp)
        self.check_path_result(resp.rows, expect_result)

    def test_all_over_all_multi_dst(self):
        # FIND ALL PATH OVER * UPTO 2 STEPS
        cmd = 'FIND ALL PATH FROM 1016 TO 1020 OVER * UPTO 2 STEPS'
        resp = self.execute(cmd)

        expect_result = [[1016, (b'is_schoolmate', 0), 1017, (b'is_friend', 0), 1020],
            [1016, (b'is_schoolmate', 0), 1018, (b'is_friend', 0), 1020],
            [1016, (b'is_friend', 0), 1018, (b'is_friend', 0), 1020],
            [1016, (b'is_schoolmate', 0), 1017, (b'is_schoolmate', 0), 1020]]
        print(cmd)
        self.check_resp_succeeded(resp)
        self.check_path_result(resp.rows, expect_result)
