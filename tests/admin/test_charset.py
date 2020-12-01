# --coding:utf-8--
#
# Copyright (c) 2020 vesoft inc. All rights reserved.
#
# This source code is licensed under Apache 2.0 License,
# attached with Common Clause Condition 1.0, found in the LICENSES directory.

import time
import re

from tests.common.nebula_test_suite import NebulaTestSuite


class TestCharset(NebulaTestSuite):

    @classmethod
    def prepare(self):
        pass

    @classmethod
    def cleanup(self):
        pass

    def test_show_charset(self):
        # succeeded
        resp = self.client.execute('SHOW CHARSET')
        self.check_resp_succeeded(resp)

        self.check_column_names(resp, ["Charset", "Description", "Default collation", "Maxlen"])

        self.check_result(resp, [["utf8", "UTF-8 Unicode", "utf8_bin", 4]])

        # syntax error
        resp = self.client.execute('SHOW CHARSETS')
        self.check_resp_failed(resp)

    def test_show_collate(self):
        # succeeded
        resp = self.client.execute('SHOW COLLATION')
        self.check_resp_succeeded(resp)

        self.check_column_names(resp, ["Collation", "Charset"])

        self.check_result(resp, [["utf8_bin", "utf8"]])

        # syntax error
        resp = self.client.execute('SHOW COLLATE')
        self.check_resp_failed(resp)

        resp = self.client.execute('SHOW COLLATIONS')
        self.check_resp_failed(resp)

