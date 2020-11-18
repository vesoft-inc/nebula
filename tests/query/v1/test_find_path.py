# --coding:utf-8--
#
# Copyright (c) 2020 vesoft inc. All rights reserved.
#
# This source code is licensed under Apache 2.0 License,
# attached with Common Clause Condition 1.0, found in the LICENSES directory.

from tests.common.nebula_test_suite import NebulaTestSuite


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
                ["Tim Duncan", ("like", 0, "Tony Parker")]
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
                ["Tim Duncan", ("like", 0, "Tony Parker"), ("like", 0, "LaMarcus Aldridge")]
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
                ["Tiago Splitter", ("like", 0, "Tim Duncan"), ("like", 0, "Tony Parker"), ("like", 0, "LaMarcus Aldridge")]
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
                ["Tiago Splitter", ("like", 0, "Tim Duncan"), ("teammate", 0, "LaMarcus Aldridge")]
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
                ["Tiago Splitter", ("like", 0, "Tim Duncan"), ("teammate", 0, "LaMarcus Aldridge")]
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
                ["Tim Duncan", ("like", 0, "Tony Parker")],
                ["Tim Duncan", ("like", 0, "Tony Parker"), ("like", 0, "Tim Duncan"), ("like", 0, "Tony Parker")],
                ["Tim Duncan", ("like", 0, "Manu Ginobili"), ("like", 0, "Tim Duncan"), ("like", 0, "Tony Parker")],
                ["Tim Duncan", ("like", 0, "Tony Parker"), ("like", 0, "LaMarcus Aldridge"), ("like", 0, "Tony Parker")]
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
                ["Tim Duncan", ("like", 0, "Tony Parker")],
                ["Tim Duncan", ("like", 0, "Manu Ginobili")],
                ["Tim Duncan", ("like", 0, "Tony Parker"), ("like", 0, "Manu Ginobili")],
                ["Tim Duncan", ("like", 0, "Tony Parker"), ("like", 0, "Tim Duncan"), ("like", 0, "Tony Parker")],
                ["Tim Duncan", ("like", 0, "Manu Ginobili"), ("like", 0, "Tim Duncan"), ("like", 0, "Tony Parker")],
                ["Tim Duncan", ("like", 0, "Tony Parker"), ("like", 0, "LaMarcus Aldridge"), ("like", 0, "Tony Parker")],
                ["Tim Duncan", ("like", 0, "Tony Parker"), ("like", 0, "Tim Duncan"), ("like", 0, "Manu Ginobili")],
                ["Tim Duncan", ("like", 0, "Manu Ginobili"), ("like", 0, "Tim Duncan"), ("like", 0, "Manu Ginobili")]
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
                ["Tim Duncan", ("like", 0, "Tony Parker")],
                ["Tim Duncan", ("like", 0, "Tony Parker"), ("like", 0, "LaMarcus Aldridge")],
                ["Tim Duncan", ("like", 0, "Tony Parker"), ("like", 0, "Tim Duncan"), ("like", 0, "Tony Parker")],
                ["Tim Duncan", ("like", 0, "Manu Ginobili"), ("like", 0, "Tim Duncan"), ("like", 0, "Tony Parker")],
                ["Tim Duncan", ("like", 0, "Tony Parker"), ("like", 0, "LaMarcus Aldridge"), ("like", 0, "Tony Parker")]
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
                ["Tim Duncan", ("like", 0, "Tony Parker")],
                ["Tim Duncan", ("serve", 0, "Spurs")],
                ["Tim Duncan", ("like", 0, "Manu Ginobili"), ("serve", 0, "Spurs")],
                ["Tim Duncan", ("like", 0, "Tony Parker"), ("serve", 0, "Spurs")],
                ["Tim Duncan", ("like", 0, "Tony Parker"), ("like", 0, "Tim Duncan"), ("like", 0, "Tony Parker")],
                ["Tim Duncan", ("like", 0, "Manu Ginobili"), ("like", 0, "Tim Duncan"), ("like", 0, "Tony Parker")],
                ["Tim Duncan", ("like", 0, "Tony Parker"), ("like", 0, "Tim Duncan"), ("serve", 0, "Spurs")],
                ["Tim Duncan", ("like", 0, "Manu Ginobili"), ("like", 0, "Tim Duncan"), ("serve", 0, "Spurs")],
                ["Tim Duncan", ("like", 0, "Tony Parker"), ("like", 0, "LaMarcus Aldridge"), ("like", 0, "Tony Parker")],
                ["Tim Duncan", ("like", 0, "Tony Parker"), ("like", 0, "LaMarcus Aldridge"), ("serve", 0, "Spurs")],
                ["Tim Duncan", ("like", 0, "Tony Parker"), ("like", 0, "Manu Ginobili"), ("serve", 0, "Spurs")]
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
                ["Tim Duncan", ("like", 0, "Tony Parker")],
                ["Tim Duncan", ("serve", 0, "Spurs")],
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
                ["Tim Duncan", ("like", 0, "Tony Parker")],
                ["Tim Duncan", ("teammate", 0, "Tony Parker")],
                ["Tim Duncan", ("serve", 0, "Spurs")],
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
                ["Yao Ming", ("like", 0, "Shaquile O'Neal"), ("like", 0, "Tim Duncan"), ("like", 0, "Manu Ginobili")],
                ["Yao Ming", ("like", 0, "Shaquile O'Neal"), ("like", 0, "Tim Duncan"), ("teammate", 0, "Manu Ginobili")],
                ["Yao Ming", ("like", 0, "Tracy McGrady"), ("serve", 0, "Spurs")],
                ["Yao Ming", ("like", 0, "Shaquile O'Neal"), ("serve", 0, "Lakers")],
                ["Tony Parker", ("like", 0, "Tim Duncan"), ("teammate", 0, "Danny Green"), ("like", 0, "LeBron James"), ("serve", 0, "Lakers")],
                ["Tony Parker", ("teammate", 0, "Tim Duncan"), ("teammate", 0, "Danny Green"), ("like", 0, "LeBron James"), ("serve", 0, "Lakers")],
                ["Tony Parker", ("like", 0, "Manu Ginobili")],
                ["Tony Parker", ("teammate", 0, "Manu Ginobili")],
                ["Tony Parker", ("serve", 0, "Spurs")]
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
                ["Yao Ming", ("like", 0, "Shaquile O'Neal"), ("like", 0, "Tim Duncan"), ("like", 0, "Manu Ginobili")],
                ["Yao Ming", ("like", 0, "Shaquile O'Neal"), ("like", 0, "Tim Duncan"), ("teammate", 0, "Manu Ginobili")],
                ["Yao Ming", ("like", 0, "Tracy McGrady"), ("serve", 0, "Spurs")],
                ["Yao Ming", ("like", 0, "Shaquile O'Neal"), ("serve", 0, "Lakers")],
                ["Tony Parker", ("like", 0, "Manu Ginobili")],
                ["Tony Parker", ("teammate", 0, "Manu Ginobili")],
                ["Tony Parker", ("serve", 0, "Spurs")]
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
                ["Yao Ming", ("like", 0, "Tracy McGrady"), ("serve", 0, "Spurs")],
                ["Yao Ming", ("like", 0, "Shaquile O'Neal"), ("serve", 0, "Lakers")],
                ["Marco Belinelli", ("like", 0, "Danny Green"), ("like", 0, "LeBron James"), ("serve", 0, "Lakers")],
                ["Marco Belinelli", ("serve", 0, "Spurs")],
                ["Marco Belinelli", ("serve", 1, "Spurs")],
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
                ["Tim Duncan", ("like", 0, "Tony Parker")],
                ["Tim Duncan", ("like", 0, "Tony Parker"), ("like", 0, "LaMarcus Aldridge")],
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
                ["Tiago Splitter", ("like", 0, "Tim Duncan"), ("like", 0, "Tony Parker")],
                ["Tiago Splitter", ("serve", 0, "Spurs")],
                ["Tim Duncan", ("serve", 0, "Spurs")],
                ["Tim Duncan", ("like", 0, "Tony Parker")],
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
                ["Yao Ming", ("like", 0, "Shaquile O'Neal"), ("like", 0, "Tim Duncan"), ("like", 0, "Tony Parker")],
                ["Yao Ming", ("like", 0, "Tracy McGrady")],
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
                ["Shaquile O\'Neal", ("like", 0, "Tim Duncan"), ("serve", 0, "Spurs")],
                ["Shaquile O\'Neal", ("serve", 0, "Lakers")],
                ["Shaquile O\'Neal", ("like", 0, "Tim Duncan"), ("like", 0, "Manu Ginobili")],
                ["Shaquile O\'Neal", ("like", 0, "Tim Duncan"), ("teammate", 0, "Manu Ginobili")],
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
                ["Shaquile O\'Neal", ("like", 0, "Tim Duncan"), ("serve", 0, "Spurs")],
                ["Shaquile O\'Neal", ("serve", 0, "Lakers")],
                ["Shaquile O\'Neal", ("like", 0, "Tim Duncan"), ("like", 0, "Manu Ginobili")],
                ["Shaquile O\'Neal", ("like", 0, "Tim Duncan"), ("teammate", 0, "Manu Ginobili")],
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
                ["Shaquile O\'Neal", ("like", 0, "Tim Duncan"), ("like", 0, "Manu Ginobili")],
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
                ["Shaquile O\'Neal", ("like", 0, "Tim Duncan"), ("serve", 0, "Spurs")],
                ["Shaquile O\'Neal", ("serve", 0, "Lakers")],
                ["Shaquile O\'Neal", ("like", 0, "Tim Duncan"), ("like", 0, "Manu Ginobili")],
                ["Shaquile O\'Neal", ("like", 0, "Tim Duncan"), ("teammate", 0, "Manu Ginobili")],
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
                ["Shaquile O\'Neal", ("like", 0, "Tim Duncan"), ("serve", 0, "Spurs")],
                ["Shaquile O\'Neal", ("serve", 0, "Lakers")],
                ["Shaquile O\'Neal", ("like", 0, "Tim Duncan"), ("like", 0, "Manu Ginobili")],
                ["Shaquile O\'Neal", ("like", 0, "Tim Duncan"), ("teammate", 0, "Manu Ginobili")],
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
                ["Shaquile O\'Neal", ("like", 0, "Tim Duncan"), ("like", 0, "Manu Ginobili")],
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
                ["Marco Belinelli", ("serve", 0, "Spurs")],
                ["Marco Belinelli", ("serve", 1, "Spurs")],
                ["Marco Belinelli", ("like", 0, "Danny Green"), ("like", 0, "LeBron James"), ("serve", 0, "Lakers")],
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

    def test_run_time_input(self):
        stmt = '''YIELD "Yao Ming" AS src, "Tony Parker" AS dst
                | FIND SHORTEST PATH FROM $-.src TO $-.dst OVER like, serve UPTO 5 STEPS'''
        resp = self.execute_query(stmt)
        self.check_resp_succeeded(resp)
        expected_data = {
            "column_names": ["_path"],
            "rows": [
                ["Yao Ming", ("like", 0, "Shaquile O'Neal"), ("like", 0, "Tim Duncan"), ("like", 0, "Tony Parker")],
            ]
        }
        self.check_column_names(resp, expected_data["column_names"])
        self.check_path_result_without_prop(resp.data.rows, expected_data["rows"])

        stmt = '''YIELD "Shaquile O\'Neal" AS src
                | FIND SHORTEST PATH FROM $-.src TO "Manu Ginobili" OVER * UPTO 5 STEPS'''
        resp = self.execute_query(stmt)
        self.check_resp_succeeded(resp)
        expected_data = {
            "column_names": ["_path"],
            "rows": [
                ["Shaquile O\'Neal", ("like", 0, "Tim Duncan"), ("like", 0, "Manu Ginobili")],
                ["Shaquile O\'Neal", ("like", 0, "Tim Duncan"), ("teammate", 0, "Manu Ginobili")],
            ]
        }
        self.check_column_names(resp, expected_data["column_names"])
        self.check_path_result_without_prop(resp.data.rows, expected_data["rows"])

        stmt = '''YIELD "Manu Ginobili" AS dst
                | FIND SHORTEST PATH FROM "Shaquile O\'Neal" TO $-.dst OVER * UPTO 5 STEPS'''
        resp = self.execute_query(stmt)
        self.check_resp_succeeded(resp)
        expected_data = {
            "column_names": ["_path"],
            "rows": [
                ["Shaquile O\'Neal", ("like", 0, "Tim Duncan"), ("like", 0, "Manu Ginobili")],
                ["Shaquile O\'Neal", ("like", 0, "Tim Duncan"), ("teammate", 0, "Manu Ginobili")],
            ]
        }
        self.check_column_names(resp, expected_data["column_names"])
        self.check_path_result_without_prop(resp.data.rows, expected_data["rows"])

        stmt = '''GO FROM "Yao Ming" over like YIELD like._dst AS src
                | FIND SHORTEST PATH FROM $-.src TO "Tony Parker" OVER like, serve UPTO 5 STEPS'''
        resp = self.execute_query(stmt)
        self.check_resp_succeeded(resp)
        expected_data = {
            "column_names": ["_path"],
            "rows": [
                ["Tracy McGrady", ("like", 0, "Rudy Gay"), ("like", 0, "LaMarcus Aldridge"), ("like", 0, "Tony Parker")],
                ["Shaquile O\'Neal", ("like", 0, "Tim Duncan"), ("like", 0, "Tony Parker")],
            ]
        }
        self.check_column_names(resp, expected_data["column_names"])
        self.check_path_result_without_prop(resp.data.rows, expected_data["rows"])

        stmt = '''$a = GO FROM "Yao Ming" over like YIELD like._dst AS src;
                FIND SHORTEST PATH FROM $a.src TO "Tony Parker" OVER like, serve UPTO 5 STEPS'''
        resp = self.execute_query(stmt)
        self.check_resp_succeeded(resp)
        expected_data = {
            "column_names": ["_path"],
            "rows": [
                ["Tracy McGrady", ("like", 0, "Rudy Gay"), ("like", 0, "LaMarcus Aldridge"), ("like", 0, "Tony Parker")],
                ["Shaquile O\'Neal", ("like", 0, "Tim Duncan"), ("like", 0, "Tony Parker")],
            ]
        }
        self.check_column_names(resp, expected_data["column_names"])
        self.check_path_result_without_prop(resp.data.rows, expected_data["rows"])

        stmt = '''GO FROM "Tim Duncan" over * YIELD like._dst AS src, serve._src AS dst
                | FIND SHORTEST PATH FROM $-.src TO $-.dst OVER like UPTO 5 STEPS'''
        resp = self.execute_query(stmt)
        self.check_resp_succeeded(resp)
        expected_data = {
            "column_names": ["_path"],
            "rows": [
                ["Manu Ginobili", ("like", 0, "Tim Duncan")],
                ["Tony Parker", ("like", 0, "Tim Duncan")],
            ]
            }
        self.check_column_names(resp, expected_data["column_names"])
        self.check_path_result_without_prop(resp.data.rows, expected_data["rows"])

        stmt = '''$a = GO FROM "Tim Duncan" over * YIELD like._dst AS src, serve._src AS dst;
                FIND SHORTEST PATH FROM $a.src TO $a.dst OVER like UPTO 5 STEPS'''
        resp = self.execute_query(stmt)
        self.check_resp_succeeded(resp)
        expected_data = {
            "column_names": ["_path"],
            "rows": [
                ["Manu Ginobili", ("like", 0, "Tim Duncan")],
                ["Tony Parker", ("like", 0, "Tim Duncan")],
            ]
            }
        self.check_column_names(resp, expected_data["column_names"])
        self.check_path_result_without_prop(resp.data.rows, expected_data["rows"])

        stmt = '''GO FROM "Tim Duncan" over * YIELD like._dst AS src, serve._src AS dst
                | FIND ALL PATH FROM $-.src TO $-.dst OVER like UPTO 3 STEPS'''
        resp = self.execute_query(stmt)
        self.check_resp_succeeded(resp)
        expected_data = {
            "column_names": ["_path"],
            "rows": [
                ["Manu Ginobili", ("like", 0, "Tim Duncan")],
                ["Tony Parker", ("like", 0, "Tim Duncan")],
                ["Tony Parker", ("like", 0, "Manu Ginobili"), ("like", 0, "Tim Duncan")],
                ["Tony Parker", ("like", 0, "LaMarcus Aldridge"), ("like", 0, "Tim Duncan")],
                ["Manu Ginobili", ("like", 0, "Tim Duncan"), ("like", 0, "Tony Parker"), ("like", 0, "Tim Duncan")],
                ["Tony Parker", ("like", 0, "Tim Duncan"), ("like", 0, "Tony Parker"), ("like", 0, "Tim Duncan")],
                ["Tony Parker", ("like", 0, "LaMarcus Aldridge"), ("like", 0, "Tony Parker"), ("like", 0, "Tim Duncan")],
                ["Manu Ginobili", ("like", 0, "Tim Duncan"), ("like", 0, "Manu Ginobili"), ("like", 0, "Tim Duncan")],
                ["Tony Parker", ("like", 0, "Tim Duncan"), ("like", 0, "Manu Ginobili"), ("like", 0, "Tim Duncan")],
            ]
            }
        self.check_column_names(resp, expected_data["column_names"])
        self.check_path_result_without_prop(resp.data.rows, expected_data["rows"])

        stmt = '''$a = GO FROM "Tim Duncan" over * YIELD like._dst AS src, serve._src AS dst;
                FIND ALL PATH FROM $a.src TO $a.dst OVER like UPTO 3 STEPS'''
        resp = self.execute_query(stmt)
        self.check_resp_succeeded(resp)
        expected_data = {
            "column_names": ["_path"],
            "rows": [
                ["Manu Ginobili", ("like", 0, "Tim Duncan")],
                ["Tony Parker", ("like", 0, "Tim Duncan")],
                ["Tony Parker", ("like", 0, "Manu Ginobili"), ("like", 0, "Tim Duncan")],
                ["Tony Parker", ("like", 0, "LaMarcus Aldridge"), ("like", 0, "Tim Duncan")],
                ["Manu Ginobili", ("like", 0, "Tim Duncan"), ("like", 0, "Tony Parker"), ("like", 0, "Tim Duncan")],
                ["Tony Parker", ("like", 0, "Tim Duncan"), ("like", 0, "Tony Parker"), ("like", 0, "Tim Duncan")],
                ["Tony Parker", ("like", 0, "LaMarcus Aldridge"), ("like", 0, "Tony Parker"), ("like", 0, "Tim Duncan")],
                ["Manu Ginobili", ("like", 0, "Tim Duncan"), ("like", 0, "Manu Ginobili"), ("like", 0, "Tim Duncan")],
                ["Tony Parker", ("like", 0, "Tim Duncan"), ("like", 0, "Manu Ginobili"), ("like", 0, "Tim Duncan")],
            ]
            }
        self.check_column_names(resp, expected_data["column_names"])
        self.check_path_result_without_prop(resp.data.rows, expected_data["rows"])

        stmt = '''$a = GO FROM "Tim Duncan" over like YIELD like._src AS src;
                GO FROM "Tony Parker" OVER like YIELD like._src AS src, like._dst AS dst
                | FIND SHORTEST PATH FROM $a.src TO $-.dst OVER like UPTO 5 STEPS'''
        resp = self.execute_query(stmt)
        self.check_resp_succeeded(resp)
        expected_data = {
            "column_names": ["_path"],
            "rows": [
                ["Tim Duncan", ("like", 0, "Manu Ginobili")],
                ["Tim Duncan", ("like", 0, "Tony Parker"), ("like", 0, "LaMarcus Aldridge")],
            ]
        }
        self.check_column_names(resp, expected_data["column_names"])
        self.check_path_result_without_prop(resp.data.rows, expected_data["rows"])
