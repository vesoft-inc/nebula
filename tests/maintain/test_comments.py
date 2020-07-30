# --coding:utf-8--
#
# Copyright (c) 2020 vesoft inc. All rights reserved.
#
# This source code is licensed under Apache 2.0 License,
# attached with Common Clause Condition 1.0, found in the LICENSES directory.

from tests.common.nebula_test_suite import NebulaTestSuite


class TestComment(NebulaTestSuite):

    @classmethod
    def prepare(self):
        pass

    @classmethod
    def cleanup(self):
        pass

    def test_comment(self):
        # Test command is comment
        resp = self.client.execute('# CREATE TAG TAG1')
        self.check_resp_succeeded(resp)

        # Test command is comment
        resp = self.client.execute('SHOW SPACES # show all spaces')
        self.check_resp_succeeded(resp)
