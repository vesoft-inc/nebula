# --coding:utf-8--
#
# Copyright (c) 2020 vesoft inc. All rights reserved.
#
# This source code is licensed under Apache 2.0 License,
# attached with Common Clause Condition 1.0, found in the LICENSES directory.

from tests.common.nebula_test_suite import NebulaTestSuite
from tests.common.nebula_test_suite import T_EMPTY, T_NULL
import pytest

class TestGroupBy(NebulaTestSuite):
    @classmethod
    def prepare(self):
        self.load_data()

    def cleanup():
        pass


    def test_syntax_error(self):
        # Use groupby without input
        stmt = '''GO FROM 'Marco Belinelli' OVER serve YIELD $$.team.name AS name
                | GROUP BY 1+1 YIELD COUNT(1), 1+1 '''
        resp = self.execute_query(stmt)
        self.check_resp_failed(resp)

        # use var
        stmt = '''GO FROM 'Marco Belinelli' OVER serve YIELD $$.team.name AS name,
                serve.end_year AS end_year | GROUP BY $-.start_year YIELD COUNT($var) '''
        resp = self.execute_query(stmt)
        self.check_resp_failed(resp)

        # use dst
        stmt = '''GO FROM 'Marco Belinelli' OVER serve YIELD $$.team.name AS name,
                serve.end_year AS end_year | GROUP BY $-.start_year YIELD COUNT($$.team.name) '''
        resp = self.execute_query(stmt)
        self.check_resp_failed(resp)

        # groupby input noexist
        stmt = '''GO FROM 'Marco Belinelli' OVER serve YIELD $$.team.name AS name,
                serve._dst AS id | GROUP BY $-.start_year YIELD COUNT($-.id) '''
        resp = self.execute_query(stmt)
        self.check_resp_failed(resp)

        # group alias noexist
        # stmt = '''GO FROM 'Marco Belinelli' OVER serve YIELD $$.team.name AS name,
        #         serve._dst AS id | GROUP BY team YIELD COUNT($-.id), $-.name AS teamName '''
        # resp = self.execute_query(stmt)
        # self.check_resp_failed(resp)

        # Field nonexistent
        stmt = '''GO FROM 'Marco Belinelli' OVER serve YIELD $$.team.name AS name,
                serve._dst AS id | GROUP BY $-.name YIELD COUNT($-.start_year) '''
        resp = self.execute_query(stmt)
        self.check_resp_failed(resp)

        # use sum(*)
        stmt = '''GO FROM 'Marco Belinelli' OVER serve YIELD $$.team.name AS name,
                serve._dst AS id | GROUP BY $-.name YIELD SUM(*) '''
        resp = self.execute_query(stmt)
        self.check_resp_failed(resp)

        # use agg fun has more than one inputs
        stmt = '''GO FROM 'Marco Belinelli' OVER serve YIELD $$.team.name AS name,
                serve._dst AS id | GROUP BY $-.name YIELD COUNT($-.name, $-.id)'''
        resp = self.execute_query(stmt)
        self.check_resp_failed(resp)

        # group col has agg fun
        stmt = '''GO FROM 'Marco Belinelli' OVER serve YIELD $$.team.name AS name,
                serve._dst AS id | GROUP BY $-.name, SUM($-.id) YIELD $-.name,  SUM($-.id)'''
        resp = self.execute_query(stmt)
        self.check_resp_failed(resp)

        # yield without group by
        stmt = '''GO FROM 'Marco Belinelli' OVER serve YIELD $$.team.name AS name,
                COUNT(serve._dst) AS id'''
        resp = self.execute_query(stmt)
        self.check_resp_failed(resp)

    def test_group_by(self):
        stmt = '''GO FROM 'Aron Baynes', 'Tracy McGrady' OVER serve
                YIELD $$.team.name AS name,
                serve._dst AS id,
                serve.start_year AS start_year,
                serve.end_year AS end_year
                | GROUP BY $-.name, $-.start_year
                YIELD $-.name AS teamName,
                $-.start_year AS start_year,
                MAX($-.start_year),
                MIN($-.end_year),
                AVG($-.end_year) AS avg_end_year,
                STD($-.end_year) AS std_end_year,
                COUNT($-.id)'''
        resp = self.execute_query(stmt)
        self.check_resp_succeeded(resp)
        expected_data = {
            "column_names" : ["teamName", "start_year", "MAX($-.start_year)", "MIN($-.end_year)", "avg_end_year", "std_end_year", "COUNT($-.id)"],
            "rows" : [
                ["Celtics", 2017, 2017, 2019, 2019.0, 0.0, 1],
                ["Magic", 2000, 2000, 2004, 2004.0, 0.0, 1],
                ["Pistons", 2015, 2015, 2017, 2017.0, 0.0, 1],
                ["Raptors", 1997, 1997, 2000, 2000.0, 0.0, 1],
                ["Rockets", 2004, 2004, 2010, 2010.0, 0.0, 1],
                ["Spurs", 2013, 2013, 2013, 2014.0, 1.0, 2]
            ]
        }
        self.check_column_names(resp, expected_data["column_names"])
        self.check_out_of_order_result(resp, expected_data["rows"])

        # group one col
        stmt = '''GO FROM 'Marco Belinelli' OVER serve
                YIELD $$.team.name AS name,
                serve._dst AS id,
                serve.start_year AS start_year,
                serve.end_year AS end_year
                | GROUP BY $-.start_year
                YIELD COUNT($-.id),
                $-.start_year AS start_year,
                AVG($-.end_year) as avg'''
        resp = self.execute_query(stmt)
        self.check_resp_succeeded(resp)
        expected_data = {
            "column_names" : ["COUNT($-.id)", "start_year", "avg"],
            "rows" : [
                [2, 2018, 2018.5],
                [1, 2017, 2018.0],
                [1, 2016, 2017.0],
                [1, 2009, 2010.0],
                [1, 2007, 2009.0],
                [1, 2012, 2013.0],
                [1, 2013, 2015.0],
                [1, 2015, 2016.0],
                [1, 2010, 2012.0]
            ]
        }
        self.check_column_names(resp, expected_data["column_names"])
        self.check_out_of_order_result(resp, expected_data["rows"])

        # group by aliasName   not implement
        # stmt = '''GO FROM 'Aron Baynes', 'Tracy McGrady' OVER serve YIELD $$.team.name AS name,
        #         serve._dst AS id, serve.start_year AS start_year, serve.end_year AS end_year
        #         | GROUP BY teamName, start_year YIELD $-.name AS teamName, $-.start_year AS start_year,
        #         MAX($-.start), MIN($-.end), AVG($-.end) AS avg_end_year, STD($-.end) AS std_end_year,
        #         COUNT($-.id)'''
        # resp = self.execute_query(stmt)
        # self.check_resp_succeeded(resp)
        # expected_data = {
        #     "column_names" : ["teamName", "start_year", "MAX(%-.start)", "MIN($-.end)", "avg_end_year", "std_end_year", "COUNT($-.id)"],
        #     "rows" : [
        #         ["Celtics", 2017, 2017, 2019, 2019.0, 0, 1],
        #         ["Magic", 2000, 2000, 2004, 2004.0, 0, 1],
        #         ["Pistons", 2015, 2015, 2017, 2017.0, 0, 1],
        #         ["Raptors", 1997, 1997, 2000, 2000.0, 0, 1],
        #         ["Rockets", 2004, 2004, 2010, 2010.0, 0, 1],
        #         ["Spurs", 2013, 2013, 2013, 2014.0, 1, 2]
        #     ]
        # }
        # self.check_column_names(resp, expected_data["column_names"])
        # self.check_out_of_order_result(resp, expected_data["rows"])

        # count(distinct) not implement
        # stmt = '''GO FROM 'Carmelo Anthony', 'Dwyane Wade' OVER like
        #         YIELD $$.player.name AS name,
        #         $$.player.age AS dst_age,
        #         $$.player.age AS src_age,
        #         like.likeness AS likeness
        #         | GROUP BY $-.name
        #         YIELD $-.name AS name,
        #         SUM($-.dst_age) AS sum_dst_age,
        #         AVG($-.dst_age) AS avg_dst_age,
        #         MAX($-.src_age) AS max_src_age,
        #         MIN($-.src_age) AS min_src_age,
        #         BIT_AND(1) AS bit_and,
        #         BIT_OR(2) AS bit_or,
        #         BIT_XOR(3) AS bit_xor,
        #         COUNT($-.likeness),
        #         COUNT_DISTINCT($-.likeness)'''
        # resp = self.execute_query(stmt)
        # self.check_resp_succeeded(resp)
        # expected_data = {
        #     "column_names" : ["name", "sum_dst_age", "avg_dst_age", "max_src_age", "min_src_age", "bit_and",
        #                       "bit_or", "bit_xor", "COUNT($-.likeness)", "COUNT_DISTINCT($-.likeness)"],
        #     "rows" : [
        #         ["LeBron James", 68, 34.0, 34, 34, 1, 2, 0, 2, 1],
        #         ["Chris Paul", 66, 33.0, 33, 33, 1, 2, 0, 2, 1],
        #         ["Dwyane Wade", 37, 37.0, 37, 37, 1, 2, 3, 1, 1],
        #         ["Carmelo Anthony", 34, 34.0, 34, 34, 1, 2, 3, 1, 1]
        #     ]
        # }
        # self.check_column_names(resp, expected_data["column_names"])
        # self.check_out_of_order_result(resp, expected_data["rows"])

        # group has all agg fun
        stmt = '''GO FROM 'Carmelo Anthony', 'Dwyane Wade' OVER like
                YIELD $$.player.name AS name,
                $$.player.age AS dst_age,
                $$.player.age AS src_age,
                like.likeness AS likeness
                | GROUP BY $-.name
                YIELD $-.name AS name,
                SUM($-.dst_age) AS sum_dst_age,
                AVG($-.dst_age) AS avg_dst_age,
                MAX($-.src_age) AS max_src_age,
                MIN($-.src_age) AS min_src_age,
                BIT_AND(1) AS bit_and,
                BIT_OR(2) AS bit_or,
                BIT_XOR(3) AS bit_xor,
                COUNT($-.likeness)'''
        resp = self.execute_query(stmt)
        self.check_resp_succeeded(resp)
        expected_data = {
            "column_names" : ["name", "sum_dst_age", "avg_dst_age", "max_src_age", "min_src_age", "bit_and",
                              "bit_or", "bit_xor", "COUNT($-.likeness)"],
            "rows" : [
                ["LeBron James", 68, 34.0, 34, 34, 1, 2, 0, 2],
                ["Chris Paul", 66, 33.0, 33, 33, 1, 2, 0, 2],
                ["Dwyane Wade", 37, 37.0, 37, 37, 1, 2, 3, 1],
                ["Carmelo Anthony", 34, 34.0, 34, 34, 1, 2, 3, 1]
            ]
        }
        self.check_column_names(resp, expected_data["column_names"])
        self.check_out_of_order_result(resp, expected_data["rows"])

        # group has fun col
        stmt = '''GO FROM 'Carmelo Anthony', 'Dwyane Wade' OVER like
                YIELD $$.player.name AS name
                | GROUP BY $-.name, abs(5)
                YIELD $-.name AS name,
                SUM(1.5) AS sum,
                COUNT(*) AS count,
                1+1 AS cal'''
        resp = self.execute_query(stmt)
        self.check_resp_succeeded(resp)
        expected_data = {
            "column_names" : ["name", "sum", "count", "cal"],
            "rows" : [
                ["LeBron James", 3.0, 2, 2],
                ["Chris Paul", 3.0, 2, 2],
                ["Dwyane Wade", 1.5, 1, 2],
                ["Carmelo Anthony", 1.5, 1, 2]
            ]
        }
        self.check_column_names(resp, expected_data["column_names"])
        self.check_out_of_order_result(resp, expected_data["rows"])

        # output next
        stmt = '''GO FROM 'Paul Gasol' OVER like
                YIELD $$.player.age AS age,
                like._dst AS id
                | GROUP BY $-.id
                YIELD $-.id AS id,
                SUM($-.age) AS age
                | GO FROM $-.id OVER serve
                YIELD $$.team.name AS name,
                $-.age AS sumAge'''
        resp = self.execute_query(stmt)
        self.check_resp_succeeded(resp)
        expected_data = {
            "column_names" : ["name", "sumAge"],
            "rows" : [
                ["Grizzlies", 34],
                ["Raptors", 34],
                ["Lakers", 40]
            ]
        }
        self.check_column_names(resp, expected_data["column_names"])
        self.check_out_of_order_result(resp, expected_data["rows"])

    def test_empty_input(self):
        stmt = '''GO FROM 'noexist' OVER like
                YIELD $$.player.name AS name
                | GROUP BY $-.name, abs(5)
                YIELD $-.name AS name,
                SUM(1.5) AS sum,
                COUNT(*) AS count
                | ORDER BY $-.sum | LIMIT 2'''
        resp = self.execute_query(stmt)
        self.check_resp_succeeded(resp)
        self.check_empty_result(resp)

        stmt = '''GO FROM 'noexist' OVER serve
                YIELD $^.player.name as name,
                serve.start_year as start,
                $$.team.name as team
                | YIELD $-.name as name
                WHERE $-.start > 20000
                | GROUP BY $-.name
                YIELD $-.name AS name'''
        resp = self.execute_query(stmt)
        self.check_resp_succeeded(resp)
        self.check_empty_result(resp)

        stmt = '''GO FROM 'noexist' OVER serve
                YIELD $^.player.name as name,
                serve.start_year as start,
                $$.team.name as team
                | YIELD $-.name as name
                WHERE $-.start > 20000
                | Limit 1'''
        resp = self.execute_query(stmt)
        self.check_resp_succeeded(resp)
        self.check_empty_result(resp)

    def test_duplicate_column(self):
        stmt = '''GO FROM 'Marco Belinelli' OVER serve
                YIELD $$.team.name AS name,
                serve._dst AS id,
                serve.start_year AS start_year,
                serve.end_year AS start_year
                | GROUP BY $-.start_year
                YIELD COUNT($-.id),
                $-.start_year AS start_year,
                AVG($-.end_year) as avg'''
        resp = self.execute_query(stmt)
        self.check_resp_failed(resp)

        stmt = '''GO FROM 'noexist' OVER serve
                YIELD $^.player.name as name,
                serve.start_year as start,
                $$.team.name as name
                | GROUP BY $-.name
                YIELD $-.name AS name'''
        resp = self.execute_query(stmt)
        self.check_resp_failed(resp)

    def test_groupby_orderby_limit(self):
        # with orderby
        stmt = '''GO FROM 'Carmelo Anthony', 'Dwyane Wade' OVER like
                YIELD $$.player.name AS name
                | GROUP BY $-.name, abs(5)
                YIELD $-.name AS name,
                SUM(1.5) AS sum,
                COUNT(*) AS count
                | ORDER BY $-.sum, $-.name'''
        resp = self.execute_query(stmt)
        self.check_resp_succeeded(resp)
        expected_data = {
            "column_names" : ["name", "sum", "count"],
            "rows" : [
                ["Carmelo Anthony", 1.5, 1],
                ["Dwyane Wade", 1.5, 1],
                ["Chris Paul", 3.0, 2],
                ["LeBron James", 3.0, 2]
            ]
        }
        self.check_column_names(resp, expected_data["column_names"])
        self.check_out_of_order_result(resp, expected_data["rows"])

        # with limit ()
        stmt = '''GO FROM 'Carmelo Anthony', 'Dwyane Wade' OVER like
                YIELD $$.player.name AS name
                | GROUP BY $-.name, abs(5)
                YIELD $-.name AS name,
                SUM(1.5) AS sum,
                COUNT(*) AS count
                | ORDER BY $-.sum, $-.name  DESC | LIMIT 2'''
        resp = self.execute_query(stmt)
        self.check_resp_succeeded(resp)
        expected_data = {
            "column_names" : ["name", "sum", "count"],
            "rows" : [
                ["Carmelo Anthony", 1.5, 1],
                ["Dwyane Wade", 1.5, 1]
            ]
        }
        self.check_column_names(resp, expected_data["column_names"])
        self.check_out_of_order_result(resp, expected_data["rows"])
