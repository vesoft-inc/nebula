# --coding:utf-8--
#
# Copyright (c) 2020 vesoft inc. All rights reserved.
#
# This source code is licensed under Apache 2.0 License,
# attached with Common Clause Condition 1.0, found in the LICENSES directory.

import pytest

import time

from tests.common.nebula_test_suite import NebulaTestSuite
from nebula2.common import ttypes as CommonTtypes

class TestBasicTypeCrud(NebulaTestSuite):
    @classmethod
    def prepare(self):
        resp = self.execute('CREATE SPACE basic_type_crud')
        self.check_resp_succeeded(resp)
        time.sleep(self.delay)

        resp = self.execute('USE basic_type_crud')
        self.check_resp_succeeded(resp)

    @classmethod
    def cleanup(self):
        resp = self.execute('DROP SPACE basic_type_crud')
        self.check_resp_succeeded(resp)

    # TODO(shylock) add time datetype
    def test_date_crud(self):
        # create schema
        resp = self.execute('CREATE TAG tag_date(f_date DATE, f_time TIME, f_datetime DATETIME)')
        self.check_resp_succeeded(resp)

        resp = self.execute("SHOW CREATE TAG tag_date")
        self.check_resp_succeeded(resp)
        create_str = 'CREATE TAG `tag_date` (\n' \
                     ' `f_date` date NULL,\n' \
                     ' `f_time` time NULL,\n' \
                     ' `f_datetime` datetime NULL\n' \
                     ') ttl_duration = 0, ttl_col = ""'
        result = [['tag_date', create_str]]
        self.check_out_of_order_result(resp, result)

        resp = self.execute('CREATE EDGE edge_date(f_date DATE, f_time TIME, f_datetime DATETIME)')
        self.check_resp_succeeded(resp)

        resp = self.execute("SHOW CREATE EDGE edge_date")
        self.check_resp_succeeded(resp)
        create_str = 'CREATE EDGE `edge_date` (\n' \
                     ' `f_date` date NULL,\n' \
                     ' `f_time` time NULL,\n' \
                     ' `f_datetime` datetime NULL\n' \
                     ') ttl_duration = 0, ttl_col = ""'
        result = [['edge_date', create_str]]
        self.check_out_of_order_result(resp, result)

        time.sleep(self.delay)
        # insert invalid type
        resp = self.execute('''
                            INSERT VERTEX tag_date(f_date, f_time, f_datetime)
                            VALUES "test":("2017-03-04", "23:01:00", 1234)
                            ''')
        self.check_resp_failed(resp)

        resp = self.execute('''
                            INSERT EDGE edge_date(f_date, f_time, f_datetime)
                            VALUES "test_src"->"test_dst":(true, "23:01:00", "2017-03-04T22:30:40")
                            ''')
        self.check_resp_failed(resp)

        # insert date
        # TODO(shylock) add us
        resp = self.execute('''
                            INSERT VERTEX tag_date(f_date, f_time, f_datetime)
                            VALUES "test":(date("2017-03-04"), time("23:01:00"), datetime("2017-03-04T22:30:40"))
                            ''')
        self.check_resp_succeeded(resp)

        resp = self.execute('''
                            INSERT EDGE edge_date(f_date, f_time, f_datetime)
                            VALUES "test_src"->"test_dst":(date("2017-03-04"), time("23:01:00"), datetime("2017-03-04T22:30:40"))
                            ''')
        self.check_resp_succeeded(resp)

        # query
        resp = self.execute('FETCH PROP ON tag_date "test"')
        self.check_resp_succeeded(resp)
        result = [["test", CommonTtypes.Date(2017, 3, 4), CommonTtypes.Time(23, 1, 0, 0), CommonTtypes.DateTime(2017, 3, 4, 22, 30, 40, 0)]]
        self.check_out_of_order_result(resp, result)

        resp = self.execute('FETCH PROP ON edge_date "test_src"->"test_dst"')
        self.check_resp_succeeded(resp)
        result = [["test_src", "test_dst", 0, CommonTtypes.Date(2017, 3, 4), CommonTtypes.Time(23, 1, 0, 0), CommonTtypes.DateTime(2017, 3, 4, 22, 30, 40, 0)]]
        self.check_out_of_order_result(resp, result)

        # update
        resp = self.execute('''
                                  UPDATE VERTEX "test"
                                  SET tag_date.f_date = Date("2018-03-04"), tag_date.f_time = Time("22:01:00"), tag_date.f_datetime = DateTime("2018-03-04T22:30:40")
                                  YIELD f_date, f_time, f_datetime
                                  ''')
        self.check_resp_succeeded(resp)
        result = [[CommonTtypes.Date(2018, 3, 4), CommonTtypes.Time(22, 1, 0, 0), CommonTtypes.DateTime(2018, 3, 4, 22, 30, 40, 0)]]
        self.check_out_of_order_result(resp, result)

        resp = self.execute('''
                                  UPDATE EDGE "test_src"->"test_dst" OF edge_date
                                  SET edge_date.f_date = Date("2018-03-04"), edge_date.f_time = Time("22:01:00"), edge_date.f_datetime = DateTime("2018-03-04T22:30:40")
                                  YIELD f_date, f_time, f_datetime
                                  ''')
        self.check_resp_succeeded(resp)
        result = [[CommonTtypes.Date(2018, 3, 4), CommonTtypes.Time(22, 1, 0, 0), CommonTtypes.DateTime(2018, 3, 4, 22, 30, 40, 0)]]
        self.check_out_of_order_result(resp, result)

        # delete
        resp = self.execute('DELETE VERTEX "test"')
        self.check_resp_succeeded(resp)

        resp = self.execute('DELETE EDGE edge_date "test_src"->"test_dst"')
        self.check_resp_succeeded(resp)

        # check deletion
        resp = self.execute('FETCH PROP ON tag_date "test"')
        self.check_resp_succeeded(resp)
        result = []
        self.check_out_of_order_result(resp, result)

        resp = self.execute('FETCH PROP ON edge_date "test_src"->"test_dst"')
        self.check_resp_succeeded(resp)
        result = []
        self.check_out_of_order_result(resp, result)
