# --coding:utf-8--
#
# Copyright (c) 2020 vesoft inc. All rights reserved.
#
# This source code is licensed under Apache 2.0 License,
# attached with Common Clause Condition 1.0, found in the LICENSES directory.

import pytest

from tests.common.nebula_test_suite import NebulaTestSuite


@pytest.mark.usefixtures('set_vertices_and_edges')
class TestFindPath(NebulaTestSuite):
    @classmethod
    def prepare(self):
        self.use_nba()

    def test_single_pair_constant_input(self):
        stmt = 'FIND SHORTEST PATH FROM "Tim Duncan" TO "Tony Parker" OVER like'
        resp = self.execute_query(stmt)
        self.check_resp_succeeded(resp)
        expected_data = {
            "column_names": ["_path"],
            "rows": [
                [b"Tim Duncan", (b"like", 0, b"Tony Parker")]
            ]
        }
        self.check_column_names(resp, expected_data["column_names"])
        self.check_path_result_without_prop(resp.data.rows, expected_data["rows"])

        stmt = 'FIND SHORTEST PATH FROM "Tim Duncan" TO "LaMarcus Aldridge" OVER like'
        resp = self.execute_query(stmt)
        self.check_resp_succeeded(resp)
        expected_data = {
            "column_names": ["_path"],
            "rows": [
                [b"Tim Duncan", (b"like", 0, b"Tony Parker"), (b"like", 0, b"LaMarcus Aldridge")]
            ]
        }
        self.check_column_names(resp, expected_data["column_names"])
        self.check_path_result_without_prop(resp.data.rows, expected_data["rows"])

        stmt = 'FIND SHORTEST PATH FROM "Tiago Splitter" TO "LaMarcus Aldridge" OVER like'
        resp = self.execute_query(stmt)
        self.check_resp_succeeded(resp)
        expected_data = {
            "column_names": ["_path"],
            "rows": [
                [b"Tiago Splitter", (b"like", 0, b"Tim Duncan"), (b"like", 0, b"Tony Parker"), (b"like", 0, b"LaMarcus Aldridge")]
            ]
        }
        self.check_column_names(resp, expected_data["column_names"])
        self.check_path_result_without_prop(resp.data.rows, expected_data["rows"])

        stmt = 'FIND SHORTEST PATH FROM "Tiago Splitter" TO "LaMarcus Aldridge" OVER like, teammate'
        resp = self.execute_query(stmt)
        self.check_resp_succeeded(resp)
        expected_data = {
            "column_names": ["_path"],
            "rows": [
                [b"Tiago Splitter", (b"like", 0, b"Tim Duncan"), (b"teammate", 0, b"LaMarcus Aldridge")]
            ]
        }
        self.check_column_names(resp, expected_data["column_names"])
        self.check_path_result_without_prop(resp.data.rows, expected_data["rows"])

        stmt = 'FIND SHORTEST PATH FROM "Tiago Splitter" TO "LaMarcus Aldridge" OVER *'
        resp = self.execute_query(stmt)
        self.check_resp_succeeded(resp)
        expected_data = {
            "column_names": ["_path"],
            "rows": [
                [b"Tiago Splitter", (b"like", 0, b"Tim Duncan"), (b"teammate", 0, b"LaMarcus Aldridge")]
            ]
        }
        self.check_column_names(resp, expected_data["column_names"])
        self.check_path_result_without_prop(resp.data.rows, expected_data["rows"])


    def test_all_pairs_all_paths_constant_input(self):
        stmt = 'FIND ALL PATH FROM "Tim Duncan" TO "Tony Parker" OVER like UPTO 3 STEPS'
        resp = self.execute_query(stmt)
        self.check_resp_succeeded(resp)
        expected_data = {
            "column_names": ["_path"],
            "rows": [
                [b"Tim Duncan", (b"like", 0, b"Tony Parker")],
                [b"Tim Duncan", (b"like", 0, b"Tony Parker"), (b"like", 0, b"Tim Duncan"), (b"like", 0, b"Tony Parker")],
                [b"Tim Duncan", (b"like", 0, b"Manu Ginobili"), (b"like", 0, b"Tim Duncan"), (b"like", 0, b"Tony Parker")],
                [b"Tim Duncan", (b"like", 0, b"Tony Parker"), (b"like", 0, b"LaMarcus Aldridge"), (b"like", 0, b"Tony Parker")]
            ]
        }
        self.check_column_names(resp, expected_data["column_names"])
        self.check_path_result_without_prop(resp.data.rows, expected_data["rows"])

        stmt = 'FIND ALL PATH FROM "Tim Duncan" TO "Tony Parker","Manu Ginobili" OVER like UPTO 3 STEPS'
        resp = self.execute_query(stmt)
        self.check_resp_succeeded(resp)
        expected_data = {
            "column_names": ["_path"],
            "rows": [
                [b"Tim Duncan", (b"like", 0, b"Tony Parker")],
                [b"Tim Duncan", (b"like", 0, b"Manu Ginobili")],
                [b"Tim Duncan", (b"like", 0, b"Tony Parker"), (b"like", 0, b"Manu Ginobili")],
                [b"Tim Duncan", (b"like", 0, b"Tony Parker"), (b"like", 0, b"Tim Duncan"), (b"like", 0, b"Tony Parker")],
                [b"Tim Duncan", (b"like", 0, b"Manu Ginobili"), (b"like", 0, b"Tim Duncan"), (b"like", 0, b"Tony Parker")],
                [b"Tim Duncan", (b"like", 0, b"Tony Parker"), (b"like", 0, b"LaMarcus Aldridge"), (b"like", 0, b"Tony Parker")],
                [b"Tim Duncan", (b"like", 0, b"Tony Parker"), (b"like", 0, b"Tim Duncan"), (b"like", 0, b"Manu Ginobili")],
                [b"Tim Duncan", (b"like", 0, b"Manu Ginobili"), (b"like", 0, b"Tim Duncan"), (b"like", 0, b"Manu Ginobili")]
            ]
        }
        self.check_column_names(resp, expected_data["column_names"])
        self.check_path_result_without_prop(resp.data.rows, expected_data["rows"])

        stmt = 'FIND ALL PATH FROM "Tim Duncan" TO "Tony Parker","LaMarcus Aldridge" OVER like UPTO 3 STEPS'
        resp = self.execute_query(stmt)
        self.check_resp_succeeded(resp)
        expected_data = {
            "column_names": ["_path"],
            "rows": [
                [b"Tim Duncan", (b"like", 0, b"Tony Parker")],
                [b"Tim Duncan", (b"like", 0, b"Tony Parker"), (b"like", 0, b"LaMarcus Aldridge")],
                [b"Tim Duncan", (b"like", 0, b"Tony Parker"), (b"like", 0, b"Tim Duncan"), (b"like", 0, b"Tony Parker")],
                [b"Tim Duncan", (b"like", 0, b"Manu Ginobili"), (b"like", 0, b"Tim Duncan"), (b"like", 0, b"Tony Parker")],
                [b"Tim Duncan", (b"like", 0, b"Tony Parker"), (b"like", 0, b"LaMarcus Aldridge"), (b"like", 0, b"Tony Parker")]
            ]
        }
        self.check_column_names(resp, expected_data["column_names"])
        self.check_path_result_without_prop(resp.data.rows, expected_data["rows"])

        stmt = 'FIND ALL PATH FROM "Tim Duncan" TO "Tony Parker","Spurs" OVER like,serve UPTO 3 STEPS'
        resp = self.execute_query(stmt)
        self.check_resp_succeeded(resp)
        expected_data = {
            "column_names": ["_path"],
            "rows": [
                [b"Tim Duncan", (b"like", 0, b"Tony Parker")],
                [b"Tim Duncan", (b"serve", 0, b"Spurs")],
                [b"Tim Duncan", (b"like", 0, b"Manu Ginobili"), (b"serve", 0, b"Spurs")],
                [b"Tim Duncan", (b"like", 0, b"Tony Parker"), (b"serve", 0, b"Spurs")],
                [b"Tim Duncan", (b"like", 0, b"Tony Parker"), (b"like", 0, b"Tim Duncan"), (b"like", 0, b"Tony Parker")],
                [b"Tim Duncan", (b"like", 0, b"Manu Ginobili"), (b"like", 0, b"Tim Duncan"), (b"like", 0, b"Tony Parker")],
                [b"Tim Duncan", (b"like", 0, b"Tony Parker"), (b"like", 0, b"Tim Duncan"), (b"serve", 0, b"Spurs")],
                [b"Tim Duncan", (b"like", 0, b"Manu Ginobili"), (b"like", 0, b"Tim Duncan"), (b"serve", 0, b"Spurs")],
                [b"Tim Duncan", (b"like", 0, b"Tony Parker"), (b"like", 0, b"LaMarcus Aldridge"), (b"like", 0, b"Tony Parker")],
                [b"Tim Duncan", (b"like", 0, b"Tony Parker"), (b"like", 0, b"LaMarcus Aldridge"), (b"serve", 0, b"Spurs")],
                [b"Tim Duncan", (b"like", 0, b"Tony Parker"), (b"like", 0, b"Manu Ginobili"), (b"serve", 0, b"Spurs")]
            ]
        }
        self.check_column_names(resp, expected_data["column_names"])
        self.check_path_result_without_prop(resp.data.rows, expected_data["rows"])

    def test_multi_source_shortest_path(self):
        stmt = 'FIND SHORTEST PATH FROM "Tim Duncan" TO "Tony Parker","Spurs" OVER like,serve UPTO 3 STEPS'
        resp = self.execute_query(stmt)
        self.check_resp_succeeded(resp)
        expected_data = {
            "column_names": ["_path"],
            "rows": [
                [b"Tim Duncan", (b"like", 0, b"Tony Parker")],
                [b"Tim Duncan", (b"serve", 0, b"Spurs")],
            ]
        }
        self.check_column_names(resp, expected_data["column_names"])
        self.check_path_result_without_prop(resp.data.rows, expected_data["rows"])

        stmt = 'FIND SHORTEST PATH FROM "Tim Duncan" TO "Tony Parker","Spurs" OVER * UPTO 5 STEPS'
        resp = self.execute_query(stmt)
        self.check_resp_succeeded(resp)
        expected_data = {
            "column_names": ["_path"],
            "rows": [
                [b"Tim Duncan", (b"like", 0, b"Tony Parker")],
                [b"Tim Duncan", (b"teammate", 0, b"Tony Parker")],
                [b"Tim Duncan", (b"serve", 0, b"Spurs")],
            ]
        }
        self.check_column_names(resp, expected_data["column_names"])
        self.check_path_result_without_prop(resp.data.rows, expected_data["rows"])

        stmt = 'FIND SHORTEST PATH FROM "Tony Parker", "Yao Ming" TO "Manu Ginobili", "Spurs", "Lakers" OVER * UPTO 5 STEPS'
        resp = self.execute_query(stmt)
        self.check_resp_succeeded(resp)
        expected_data = {
            "column_names": ["_path"],
            "rows": [
                [b"Yao Ming", (b"like", 0, b"Shaquile O'Neal"), (b"like", 0, b"Tim Duncan"), (b"like", 0, b"Manu Ginobili")],
                [b"Yao Ming", (b"like", 0, b"Shaquile O'Neal"), (b"like", 0, b"Tim Duncan"), (b"teammate", 0, b"Manu Ginobili")],
                [b"Yao Ming", (b"like", 0, b"Tracy McGrady"), (b"serve", 0, b"Spurs")],
                [b"Yao Ming", (b"like", 0, b"Shaquile O'Neal"), (b"serve", 0, b"Lakers")],
                [b"Tony Parker", (b"like", 0, b"Tim Duncan"), (b"teammate", 0, b"Danny Green"), (b"like", 0, b"LeBron James"), (b"serve", 0, b"Lakers")],
                [b"Tony Parker", (b"teammate", 0, b"Tim Duncan"), (b"teammate", 0, b"Danny Green"), (b"like", 0, b"LeBron James"), (b"serve", 0, b"Lakers")],
                [b"Tony Parker", (b"like", 0, b"Manu Ginobili")],
                [b"Tony Parker", (b"teammate", 0, b"Manu Ginobili")],
                [b"Tony Parker", (b"serve", 0, b"Spurs")]
            ]
        }
        self.check_column_names(resp, expected_data["column_names"])
        self.check_path_result_without_prop(resp.data.rows, expected_data["rows"])

        stmt = 'FIND SHORTEST PATH FROM "Tony Parker", "Yao Ming" TO "Manu Ginobili", "Spurs", "Lakers" OVER * UPTO 3 STEPS'
        resp = self.execute_query(stmt)
        self.check_resp_succeeded(resp)
        expected_data = {
            "column_names": ["_path"],
            "rows": [
                [b"Yao Ming", (b"like", 0, b"Shaquile O'Neal"), (b"like", 0, b"Tim Duncan"), (b"like", 0, b"Manu Ginobili")],
                [b"Yao Ming", (b"like", 0, b"Shaquile O'Neal"), (b"like", 0, b"Tim Duncan"), (b"teammate", 0, b"Manu Ginobili")],
                [b"Yao Ming", (b"like", 0, b"Tracy McGrady"), (b"serve", 0, b"Spurs")],
                [b"Yao Ming", (b"like", 0, b"Shaquile O'Neal"), (b"serve", 0, b"Lakers")],
                [b"Tony Parker", (b"like", 0, b"Manu Ginobili")],
                [b"Tony Parker", (b"teammate", 0, b"Manu Ginobili")],
                [b"Tony Parker", (b"serve", 0, b"Spurs")]
            ]
        }
        self.check_column_names(resp, expected_data["column_names"])
        self.check_path_result_without_prop(resp.data.rows, expected_data["rows"])

        stmt = 'FIND SHORTEST PATH FROM "Marco Belinelli", "Yao Ming" TO "Spurs", "Lakers" OVER * UPTO 3 STEPS'
        resp = self.execute_query(stmt)
        self.check_resp_succeeded(resp)
        expected_data = {
            "column_names": ["_path"],
            "rows": [
                [b"Yao Ming", (b"like", 0, b"Tracy McGrady"), (b"serve", 0, b"Spurs")],
                [b"Yao Ming", (b"like", 0, b"Shaquile O'Neal"), (b"serve", 0, b"Lakers")],
                [b"Marco Belinelli", (b"like", 0, b"Danny Green"), (b"like", 0, b"LeBron James"), (b"serve", 0, b"Lakers")],
                [b"Marco Belinelli", (b"serve", 0, b"Spurs")],
                [b"Marco Belinelli", (b"serve", 1, b"Spurs")],
            ]
        }
        self.check_column_names(resp, expected_data["column_names"])
        self.check_path_result_without_prop(resp.data.rows, expected_data["rows"])

        stmt = 'FIND SHORTEST PATH FROM "Tim Duncan" TO "Tony Parker","LaMarcus Aldridge" OVER like UPTO 3 STEPS'
        resp = self.execute_query(stmt)
        self.check_resp_succeeded(resp)
        expected_data = {
            "column_names": ["_path"],
            "rows": [
                [b"Tim Duncan", (b"like", 0, b"Tony Parker")],
                [b"Tim Duncan", (b"like", 0, b"Tony Parker"), (b"like", 0, b"LaMarcus Aldridge")],
            ]
        }
        self.check_column_names(resp, expected_data["column_names"])
        self.check_path_result_without_prop(resp.data.rows, expected_data["rows"])

        stmt = 'FIND SHORTEST PATH FROM "Tim Duncan", "Tiago Splitter" TO "Tony Parker","Spurs" OVER like,serve UPTO 5 STEPS'
        resp = self.execute_query(stmt)
        self.check_resp_succeeded(resp)
        expected_data = {
            "column_names": ["_path"],
            "rows": [
                [b"Tiago Splitter", (b"like", 0, b"Tim Duncan"), (b"like", 0, b"Tony Parker")],
                [b"Tiago Splitter", (b"serve", 0, b"Spurs")],
                [b"Tim Duncan", (b"serve", 0, b"Spurs")],
                [b"Tim Duncan", (b"like", 0, b"Tony Parker")],
            ]
        }
        self.check_column_names(resp, expected_data["column_names"])
        self.check_path_result_without_prop(resp.data.rows, expected_data["rows"])

        stmt = 'FIND SHORTEST PATH FROM "Yao Ming"  TO "Tony Parker","Tracy McGrady" OVER like,serve UPTO 5 STEPS'
        resp = self.execute_query(stmt)
        self.check_resp_succeeded(resp)
        expected_data = {
            "column_names": ["_path"],
            "rows": [
                [b"Yao Ming", (b"like", 0, b"Shaquile O'Neal"), (b"like", 0, b"Tim Duncan"), (b"like", 0, b"Tony Parker")],
                [b"Yao Ming", (b"like", 0, b"Tracy McGrady")],
            ]
        }
        self.check_column_names(resp, expected_data["column_names"])
        self.check_path_result_without_prop(resp.data.rows, expected_data["rows"])

        stmt = 'FIND SHORTEST PATH FROM "Shaquile O\'Neal" TO "Manu Ginobili", "Spurs", "Lakers" OVER * UPTO 5 STEPS'
        resp = self.execute_query(stmt)
        self.check_resp_succeeded(resp)
        expected_data = {
            "column_names": ["_path"],
            "rows": [
                [b"Shaquile O\'Neal", (b"like", 0, b"Tim Duncan"), (b"serve", 0, b"Spurs")],
                [b"Shaquile O\'Neal", (b"serve", 0, b"Lakers")],
                [b"Shaquile O\'Neal", (b"like", 0, b"Tim Duncan"), (b"like", 0, b"Manu Ginobili")],
                [b"Shaquile O\'Neal", (b"like", 0, b"Tim Duncan"), (b"teammate", 0, b"Manu Ginobili")],
            ]
        }
        self.check_column_names(resp, expected_data["column_names"])
        self.check_path_result_without_prop(resp.data.rows, expected_data["rows"])

        stmt = 'FIND SHORTEST PATH FROM "Shaquile O\'Neal", "Nobody" TO "Manu Ginobili", "Spurs", "Lakers" OVER * UPTO 5 STEPS'
        resp = self.execute_query(stmt)
        self.check_resp_succeeded(resp)
        expected_data = {
            "column_names": ["_path"],
            "rows": [
                [b"Shaquile O\'Neal", (b"like", 0, b"Tim Duncan"), (b"serve", 0, b"Spurs")],
                [b"Shaquile O\'Neal", (b"serve", 0, b"Lakers")],
                [b"Shaquile O\'Neal", (b"like", 0, b"Tim Duncan"), (b"like", 0, b"Manu Ginobili")],
                [b"Shaquile O\'Neal", (b"like", 0, b"Tim Duncan"), (b"teammate", 0, b"Manu Ginobili")],
            ]
        }
        self.check_column_names(resp, expected_data["column_names"])
        self.check_path_result_without_prop(resp.data.rows, expected_data["rows"])

        stmt = 'FIND SHORTEST PATH FROM "Shaquile O\'Neal" TO "Manu Ginobili", "Spurs", "Lakers" OVER like UPTO 5 STEPS'
        resp = self.execute_query(stmt)
        self.check_resp_succeeded(resp)
        expected_data = {
            "column_names": ["_path"],
            "rows": [
                [b"Shaquile O\'Neal", (b"like", 0, b"Tim Duncan"), (b"like", 0, b"Manu Ginobili")],
            ]
        }
        self.check_column_names(resp, expected_data["column_names"])
        self.check_path_result_without_prop(resp.data.rows, expected_data["rows"])

        stmt = 'FIND SHORTEST PATH FROM "Shaquile O\'Neal" TO "Manu Ginobili", "Spurs", "Lakers" OVER * UPTO 5 STEPS'
        resp = self.execute_query(stmt)
        self.check_resp_succeeded(resp)
        expected_data = {
            "column_names": ["_path"],
            "rows": [
                [b"Shaquile O\'Neal", (b"like", 0, b"Tim Duncan"), (b"serve", 0, b"Spurs")],
                [b"Shaquile O\'Neal", (b"serve", 0, b"Lakers")],
                [b"Shaquile O\'Neal", (b"like", 0, b"Tim Duncan"), (b"like", 0, b"Manu Ginobili")],
                [b"Shaquile O\'Neal", (b"like", 0, b"Tim Duncan"), (b"teammate", 0, b"Manu Ginobili")],
            ]
        }
        self.check_column_names(resp, expected_data["column_names"])
        self.check_path_result_without_prop(resp.data.rows, expected_data["rows"])

        stmt = 'FIND SHORTEST PATH FROM "Shaquile O\'Neal", "Nobody" TO "Manu Ginobili", "Spurs", "Lakers" OVER * UPTO 5 STEPS'
        resp = self.execute_query(stmt)
        self.check_resp_succeeded(resp)
        expected_data = {
            "column_names": ["_path"],
            "rows": [
                [b"Shaquile O\'Neal", (b"like", 0, b"Tim Duncan"), (b"serve", 0, b"Spurs")],
                [b"Shaquile O\'Neal", (b"serve", 0, b"Lakers")],
                [b"Shaquile O\'Neal", (b"like", 0, b"Tim Duncan"), (b"like", 0, b"Manu Ginobili")],
                [b"Shaquile O\'Neal", (b"like", 0, b"Tim Duncan"), (b"teammate", 0, b"Manu Ginobili")],
            ]
        }
        self.check_column_names(resp, expected_data["column_names"])
        self.check_path_result_without_prop(resp.data.rows, expected_data["rows"])

        stmt = 'FIND SHORTEST PATH FROM "Shaquile O\'Neal" TO "Manu Ginobili", "Spurs", "Lakers" OVER like UPTO 5 STEPS'
        resp = self.execute_query(stmt)
        self.check_resp_succeeded(resp)
        expected_data = {
            "column_names": ["_path"],
            "rows": [
                [b"Shaquile O\'Neal", (b"like", 0, b"Tim Duncan"), (b"like", 0, b"Manu Ginobili")],
            ]
        }
        self.check_column_names(resp, expected_data["column_names"])
        self.check_path_result_without_prop(resp.data.rows, expected_data["rows"])

        stmt = 'FIND SHORTEST PATH FROM "Marco Belinelli" TO "Spurs", "Lakers" OVER * UPTO 5 STEPS'
        resp = self.execute_query(stmt)
        self.check_resp_succeeded(resp)
        expected_data = {
            "column_names": ["_path"],
            "rows": [
                [b"Marco Belinelli", (b"serve", 0, b"Spurs")],
                [b"Marco Belinelli", (b"serve", 1, b"Spurs")],
                [b"Marco Belinelli", (b"like", 0, b"Danny Green"), (b"like", 0, b"LeBron James"), (b"serve", 0, b"Lakers")],
            ]
        }
        self.check_column_names(resp, expected_data["column_names"])
        self.check_path_result_without_prop(resp.data.rows, expected_data["rows"])

    def test_multi_source_no_path(self):
        stmt = 'FIND SHORTEST PATH FROM "Tim Duncan" TO "Nobody","Spur" OVER like,serve UPTO 3 STEPS'
        resp = self.execute_query(stmt)
        self.check_resp_succeeded(resp)
        expected_data = {
            "column_names": ["_path"],
            "rows": []
        }
        self.check_column_names(resp, expected_data["column_names"])
        self.check_empty_result(resp)
