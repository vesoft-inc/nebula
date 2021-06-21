# --coding:utf-8--
#
# Copyright (c) 2020 vesoft inc. All rights reserved.
#
# This source code is licensed under Apache 2.0 License,
# attached with Common Clause Condition 1.0, found in the LICENSES directory.

import re
import sys
import pytest
import time

from nebula2.graph import ttypes
from tests.common.nebula_test_suite import NebulaTestSuite

class TestReservedKeyword(NebulaTestSuite):
    @classmethod
    def prepare(self):
        pass

    # some reversed keywords are moved to unreversed keywords, and vice versa in #1922
    def test_keywords1(self):
        resp = self.execute(
            'CREATE SPACE IF NOT EXISTS test(partition_num=1024, vid_type=FIXED_STRING(8))')
        self.check_resp_succeeded(resp)
        time.sleep(self.delay)

        # issue 447
        resp = self.execute('use test')
        self.check_resp_succeeded(resp)
        cmd = 'create tag x0 (firstname string)'
        resp = self.execute(cmd)
        self.check_resp_succeeded(resp)

        cmd = 'create tag x1 (space string)'
        resp = self.execute(cmd)
        self.check_resp_succeeded(resp)

        cmd = 'create tag x2 (values string)'
        resp = self.execute(cmd)
        self.check_resp_succeeded(resp)

        cmd = 'create tag x3 (hosts string)'
        resp = self.execute(cmd)
        self.check_resp_succeeded(resp)

        cmd = 'create tag x4 (lastname string)'
        resp = self.execute(cmd)
        self.check_resp_succeeded(resp)

        cmd = 'create tag x5 (email string)'
        resp = self.execute(cmd)
        self.check_resp_succeeded(resp)

        cmd = 'create tag x6 (phone string)'
        resp = self.execute(cmd)
        self.check_resp_succeeded(resp)

        cmd = 'create tag x7 (user string)'
        resp = self.execute(cmd)
        self.check_resp_succeeded(resp)

        cmd = 'create tag x8 (users string)'
        resp = self.execute(cmd)
        self.check_resp_succeeded(resp)

        cmd = 'create tag x9 (password string)'
        resp = self.execute(cmd)
        self.check_resp_succeeded(resp)

        cmd = 'create tag x10 (role string)'
        resp = self.execute(cmd)
        self.check_resp_succeeded(resp)

        cmd = 'create tag x11 (roles string)'
        resp = self.execute(cmd)
        self.check_resp_succeeded(resp)

        cmd = 'create tag x12 (god string)'
        resp = self.execute(cmd)
        self.check_resp_succeeded(resp)

        cmd = 'create tag x13 (admin string)'
        resp = self.execute(cmd)
        self.check_resp_succeeded(resp)
        cmd = 'create tag x14 (guest string)'
        resp = self.execute(cmd)
        self.check_resp_succeeded(resp)
        cmd = 'create tag x15 (group string)'
        resp = self.execute(cmd)
        self.check_resp_succeeded(resp)
        cmd = 'create tag x16 (count string)'
        resp = self.execute(cmd)
        self.check_resp_succeeded(resp)

        cmd = 'create tag x17 (sum string)'
        resp = self.execute(cmd)
        self.check_resp_succeeded(resp)

        cmd = 'create tag x18 (avg string)'
        resp = self.execute(cmd)
        self.check_resp_succeeded(resp)

        cmd = 'create tag x19 (max string)'
        resp = self.execute(cmd)
        self.check_resp_succeeded(resp)

        cmd = 'create tag x20 (min string)'
        resp = self.execute(cmd)
        self.check_resp_succeeded(resp)

        cmd = 'create tag x21 (std string)'
        resp = self.execute(cmd)
        self.check_resp_succeeded(resp)

        cmd = 'create tag x22 (bit_and string)'
        resp = self.execute(cmd)
        self.check_resp_succeeded(resp)

        cmd = 'create tag x2ss2 (bit_or string)'
        resp = self.execute(cmd)
        self.check_resp_succeeded(resp)

        cmd = 'create tag x2ssa2 (bit_xor string)'
        resp = self.execute(cmd)
        self.check_resp_succeeded(resp)

        cmd = 'create tag x23 (path string)'
        resp = self.execute(cmd)
        self.check_resp_succeeded(resp)

        cmd = 'create tag x24 (data string)'
        resp = self.execute(cmd)
        self.check_resp_succeeded(resp)

        cmd = 'create tag x25 (leader string)'
        resp = self.execute(cmd)
        self.check_resp_succeeded(resp)

        cmd = 'create tag x26 (uuid string)'
        resp = self.execute(cmd)
        self.check_resp_succeeded(resp)

        cmd = 'create tag y0 (FIRSTNAME string)'
        resp = self.execute(cmd)
        self.check_resp_succeeded(resp)

        cmd = 'create tag y1 (SPACE string)'
        resp = self.execute(cmd)
        self.check_resp_succeeded(resp)

        # not include
        cmd = 'create tag yss2 (SPACES string)'
        resp = self.execute(cmd)
        self.check_resp_succeeded(resp)

        # not include
        cmd = 'create tag yaq2 (spaces string)'
        resp = self.execute(cmd)
        self.check_resp_succeeded(resp)

        # not include
        cmd = 'create tag x2s11 (partition string)'
        resp = self.execute(cmd)
        self.check_resp_succeeded(resp)
        # not include
        cmd = 'create tag x2aassss11 (PARTITION string)'
        resp = self.execute(cmd)
        self.check_resp_succeeded(resp)
        # not include
        cmd = 'create tag x2lll22 (replica_f string)'
        resp = self.execute(cmd)
        self.check_resp_succeeded(resp)
        # not include
        cmd = 'create tag x2lllaa22 (REPLICA_F string)'
        resp = self.execute(cmd)
        self.check_resp_succeeded(resp)
        # not include
        cmd = 'create tag x28sssss6 (ttl_durat string)'
        resp = self.execute(cmd)
        self.check_resp_succeeded(resp)
        # not include
        cmd = 'create tag x28sssssaa6 (TTL_DURAT string)'
        resp = self.execute(cmd)
        self.check_resp_succeeded(resp)
        # not include
        cmd = 'create tag x29oos4 (variables string)'
        resp = self.execute(cmd)
        self.check_resp_succeeded(resp)
        # not include
        cmd = 'create tag x2wwsdxb94 (VARIABLES string)'
        resp = self.execute(cmd)
        self.check_resp_succeeded(resp)

        cmd = 'create tag y2 (VALUES string)'
        resp = self.execute(cmd)
        self.check_resp_succeeded(resp)

        cmd = 'create tag x33(HOSTS string)'
        resp = self.execute(cmd)
        self.check_resp_succeeded(resp)

        cmd = 'create tag x44 (LASTNAME string)'
        resp = self.execute(cmd)
        self.check_resp_succeeded(resp)

        cmd = 'create tag x55 (EMAIL string)'
        resp = self.execute(cmd)
        self.check_resp_succeeded(resp)

        cmd = 'create tag x66 (PHONE string)'
        resp = self.execute(cmd)
        self.check_resp_succeeded(resp)

        cmd = 'create tag x77 (USER string)'
        resp = self.execute(cmd)
        self.check_resp_succeeded(resp)

        cmd = 'create tag x88 (USERS string)'
        resp = self.execute(cmd)
        self.check_resp_succeeded(resp)

        cmd = 'create tag x99 (PASSWORD string)'
        resp = self.execute(cmd)
        self.check_resp_succeeded(resp)

        cmd = 'create tag x100 (ROLE string)'
        resp = self.execute(cmd)
        self.check_resp_succeeded(resp)

        cmd = 'create tag x111 (ROLES string)'
        resp = self.execute(cmd)
        self.check_resp_succeeded(resp)

        cmd = 'create tag x122 (GOD string)'
        resp = self.execute(cmd)
        self.check_resp_succeeded(resp)

        cmd = 'create tag x133 (ADMIN string)'
        resp = self.execute(cmd)
        self.check_resp_succeeded(resp)
        cmd = 'create tag x144 (GUEST string)'
        resp = self.execute(cmd)
        self.check_resp_succeeded(resp)
        cmd = 'create tag x155 (GROUP string)'
        resp = self.execute(cmd)
        self.check_resp_succeeded(resp)
        cmd = 'create tag x166 (COUNT string)'
        resp = self.execute(cmd)
        self.check_resp_succeeded(resp)

        cmd = 'create tag x177 (SUM string)'
        resp = self.execute(cmd)
        self.check_resp_succeeded(resp)

        cmd = 'create tag x188 (AVG string)'
        resp = self.execute(cmd)
        self.check_resp_succeeded(resp)

        cmd = 'create tag x199 (MAX string)'
        resp = self.execute(cmd)
        self.check_resp_succeeded(resp)

        cmd = 'create tag x200 (MIN string)'
        resp = self.execute(cmd)
        self.check_resp_succeeded(resp)

        cmd = 'create tag x211 (STD string)'
        resp = self.execute(cmd)
        self.check_resp_succeeded(resp)

        cmd = 'create tag x222 (BIT_AND string)'
        resp = self.execute(cmd)
        self.check_resp_succeeded(resp)

        cmd = 'create tag x223 (BIT_OR string)'
        resp = self.execute(cmd)
        self.check_resp_succeeded(resp)

        cmd = 'create tag x20022 (BIT_XOR string)'
        resp = self.execute(cmd)
        self.check_resp_succeeded(resp)

        cmd = 'create tag x233 (PATH string)'
        resp = self.execute(cmd)
        self.check_resp_succeeded(resp)

        cmd = 'create tag x244 (DATA string)'
        resp = self.execute(cmd)
        self.check_resp_succeeded(resp)

        cmd = 'create tag x255 (LEADER string)'
        resp = self.execute(cmd)
        self.check_resp_succeeded(resp)

        cmd = 'create tag x266 (UUID string)'
        resp = self.execute(cmd)
        self.check_resp_succeeded(resp)

        resp = self.execute('drop space test')
        self.check_resp_succeeded(resp)

    def test_keywords2(self):
        resp = self.execute(
            'CREATE SPACE IF NOT EXISTS test(partition_num=1024, vid_type=FIXED_STRING(8))')
        self.check_resp_succeeded(resp)
        time.sleep(self.delay)

        resp = self.execute('use test')
        self.check_resp_succeeded(resp)

        try:
            cmd = 'create tag x1 (go string)'
            resp = self.execute(cmd)
            self.check_resp_failed(resp)
        except Exception as e:
            assert "SyntaxError: syntax error" in str(e)
        try:
            cmd = 'create tag x2 (as string)'
            resp = self.execute(cmd)
            self.check_resp_failed(resp)
        except Exception as e:
            assert "SyntaxError: syntax error" in str(e)

        try:
            cmd = 'create tag x3 (to string)'
            resp = self.execute(cmd)
            self.check_resp_failed(resp)
        except Exception as e:
            assert "SyntaxError: syntax error" in str(e)

        try:
            cmd = 'create tag x4 (or string)'
            resp = self.execute(cmd)
            self.check_resp_failed(resp)
        except Exception as e:
            assert "SyntaxError: syntax error" in str(e)

        try:
            cmd = 'create tag x5 (and string)'
            resp = self.execute(cmd)
            self.check_resp_failed(resp)
        except Exception as e:
            assert "SyntaxError: syntax error" in str(e)

        try:
            cmd = 'create tag x6 (set string)'
            resp = self.execute(cmd)
            self.check_resp_failed(resp)
        except Exception as e:
            assert "SyntaxError: syntax error" in str(e)

        try:
            cmd = 'create tag x7 (from string)'
            resp = self.execute(cmd)
            self.check_resp_failed(resp)
        except Exception as e:
            assert "SyntaxError: syntax error" in str(e)

        try:
            cmd = 'create tag x8 (where string)'
            resp = self.execute(cmd)
            self.check_resp_failed(resp)
        except Exception as e:
            assert "SyntaxError: syntax error" in str(e)

        try:
            cmd = 'create tag x9 (match string)'
            resp = self.execute(cmd)
            self.check_resp_failed(resp)
        except Exception as e:
            assert "SyntaxError: syntax error" in str(e)

        try:
            cmd = 'create tag x10 (insert string)'
            resp = self.execute(cmd)
            self.check_resp_failed(resp)
        except Exception as e:
            assert "SyntaxError: syntax error" in str(e)

        try:
            cmd = 'create tag x12 (yield string)'
            resp = self.execute(cmd)
            self.check_resp_failed(resp)
        except Exception as e:
            assert "SyntaxError: syntax error" in str(e)

        try:
            cmd = 'create tag x13 (return string)'
            resp = self.execute(cmd)
            self.check_resp_failed(resp)
        except Exception as e:
            assert "SyntaxError: syntax error" in str(e)

        try:
            cmd = 'create tag x14 (create string)'
            resp = self.execute(cmd)
            self.check_resp_failed(resp)
        except Exception as e:
            assert "SyntaxError: syntax error" in str(e)

        try:
            cmd = 'create tag x15 (describe string)'
            resp = self.execute(cmd)
            self.check_resp_failed(resp)
        except Exception as e:
            assert "SyntaxError: syntax error" in str(e)

        try:
            cmd = 'create tag x16 (desc string)'
            resp = self.execute(cmd)
            self.check_resp_failed(resp)
        except Exception as e:
            assert "SyntaxError: syntax error" in str(e)

        try:
            cmd = 'create tag x17 (vertex string)'
            resp = self.execute(cmd)
            self.check_resp_failed(resp)
        except Exception as e:
            assert "SyntaxError: syntax error" in str(e)

        try:
            cmd = 'create tag x18 (edge string)'
            resp = self.execute(cmd)
            self.check_resp_failed(resp)
        except Exception as e:
            assert "SyntaxError: syntax error" in str(e)

        try:
            cmd = 'create tag x19 (edges string)'
            resp = self.execute(cmd)
            self.check_resp_failed(resp)
        except Exception as e:
            assert "SyntaxError: syntax error" in str(e)

        try:
            cmd = 'create tag x20 (update string)'
            resp = self.execute(cmd)
            self.check_resp_failed(resp)
        except Exception as e:
            assert "SyntaxError: syntax error" in str(e)

        # try:
        #     cmd = 'create tag x21 (upset string)'
        #     resp = self.execute(cmd)
        #     self.check_resp_failed(resp)
        # except Exception as e:
        #     assert "SyntaxError: syntax error" in str(e)

        try:
            cmd = 'create tag x22 (when string)'
            resp = self.execute(cmd)
            self.check_resp_failed(resp)
        except Exception as e:
            assert "SyntaxError: syntax error" in str(e)

        try:
            cmd = 'create tag x2ss2 (delete string)'
            resp = self.execute(cmd)
            self.check_resp_failed(resp)
        except Exception as e:
            assert "SyntaxError: syntax error" in str(e)

        try:
            cmd = 'create tag xs22 (find string)'
            resp = self.execute(cmd)
            self.check_resp_failed(resp)
        except Exception as e:
            assert "SyntaxError: syntax error" in str(e)

        try:
            cmd = 'create tag x23 (alter string)'
            resp = self.execute(cmd)
            self.check_resp_failed(resp)
        except Exception as e:
            assert "SyntaxError: syntax error" in str(e)

        try:
            cmd = 'create tag x24 (steps string)'
            resp = self.execute(cmd)
            self.check_resp_failed(resp)
        except Exception as e:
            assert "SyntaxError: syntax error" in str(e)

        try:
            cmd = 'create tag x25 (over string)'
            resp = self.execute(cmd)
            self.check_resp_failed(resp)
        except Exception as e:
            assert "SyntaxError: syntax error" in str(e)

        try:
            cmd = 'create tag x26 (upto string)'
            resp = self.execute(cmd)
            self.check_resp_failed(resp)
        except Exception as e:
            assert "SyntaxError: syntax error" in str(e)

        try:
            cmd = 'create tag y0 (reversely string)'
            resp = self.execute(cmd)
            self.check_resp_failed(resp)
        except Exception as e:
            assert "SyntaxError: syntax error" in str(e)

        # try:
        #     cmd = 'create tag y1 (space string)'
        #     resp = self.execute(cmd)
        #     self.check_resp_failed(resp)
        # except Exception as e:
        #     assert "SyntaxError: syntax error" in str(e)

        # try:
        #     cmd = 'create tag y2 (spaces string)'
        #     resp = self.execute(cmd)
        #     self.check_resp_failed(resp)
        # except Exception as e:
        #     assert "SyntaxError: syntax error" in str(e)

        try:
            cmd = 'create tag x33(int string)'
            resp = self.execute(cmd)
            self.check_resp_failed(resp)
        except Exception as e:
            assert "SyntaxError: syntax error" in str(e)

        try:
            cmd = 'create tag x55 (double string)'
            resp = self.execute(cmd)
            self.check_resp_failed(resp)
        except Exception as e:
            assert "SyntaxError: syntax error" in str(e)

        try:
            cmd = 'create tag x66 (string string)'
            resp = self.execute(cmd)
            self.check_resp_failed(resp)
        except Exception as e:
            assert "SyntaxError: syntax error" in str(e)

        try:
            cmd = 'create tag x77 (bool string)'
            resp = self.execute(cmd)
            self.check_resp_failed(resp)
        except Exception as e:
            assert "SyntaxError: syntax error" in str(e)

        try:
            cmd = 'create tag x88 (tag string)'
            resp = self.execute(cmd)
            self.check_resp_failed(resp)
        except Exception as e:
            assert "SyntaxError: syntax error" in str(e)

        try:
            cmd = 'create tag x99 (tags string)'
            resp = self.execute(cmd)
            self.check_resp_failed(resp)
        except Exception as e:
            assert "SyntaxError: syntax error" in str(e)

        try:
            cmd = 'create tag x100 (union string)'
            resp = self.execute(cmd)
            self.check_resp_failed(resp)
        except Exception as e:
            assert "SyntaxError: syntax error" in str(e)

        try:
            cmd = 'create tag x111 (intersect string)'
            resp = self.execute(cmd)
            self.check_resp_failed(resp)
        except Exception as e:
            assert "SyntaxError: syntax error" in str(e)

        try:
            cmd = 'create tag x122 (minus string)'
            resp = self.execute(cmd)
            self.check_resp_failed(resp)
        except Exception as e:
            assert "SyntaxError: syntax error" in str(e)

        try:
            cmd = 'create tag x133 (no string)'
            resp = self.execute(cmd)
            self.check_resp_failed(resp)
        except Exception as e:
            assert "SyntaxError: syntax error" in str(e)

        try:
            cmd = 'create tag x144 (overwrite string)'
            resp = self.execute(cmd)
            self.check_resp_failed(resp)
        except Exception as e:
            assert "SyntaxError: syntax error" in str(e)

        try:
            cmd = 'create tag x155 (true string)'
            resp = self.execute(cmd)
            self.check_resp_failed(resp)
        except Exception as e:
            assert "SyntaxError: syntax error" in str(e)

        try:
            cmd = 'create tag x166 (false string)'
            resp = self.execute(cmd)
            self.check_resp_failed(resp)
        except Exception as e:
            assert "SyntaxError: syntax error" in str(e)

        try:
            cmd = 'create tag x177 (show string)'
            resp = self.execute(cmd)
            self.check_resp_failed(resp)
        except Exception as e:
            assert "SyntaxError: syntax error" in str(e)

        try:
            cmd = 'create tag x188 (add string)'
            resp = self.execute(cmd)
            self.check_resp_failed(resp)
        except Exception as e:
            assert "SyntaxError: syntax error" in str(e)

        # try:
        #     cmd = 'create tag x199 (hosts string)'
        #     resp = self.execute(cmd)
        #     self.check_resp_failed(resp)
        # except Exception as e:
        #     assert "SyntaxError: syntax error" in str(e)

        try:
            cmd = 'create tag x200 (timestamp string)'
            resp = self.execute(cmd)
            self.check_resp_failed(resp)
        except Exception as e:
            assert "SyntaxError: syntax error" in str(e)

        # try:
        #     cmd = 'create tag x211 (partition string)'
        #     resp = self.execute(cmd)
        #     self.check_resp_failed(resp)
        # except Exception as e:
        #     assert "SyntaxError: syntax error" in str(e)

        # try:
        #     cmd = 'create tag x222 (replica_f string)'
        #     resp = self.execute(cmd)
        #     self.check_resp_failed(resp)
        # except Exception as e:
        #     assert "SyntaxError: syntax error" in str(e)

        try:
            cmd = 'create tag x223 (drop string)'
            resp = self.execute(cmd)
            self.check_resp_failed(resp)
        except Exception as e:
            assert "SyntaxError: syntax error" in str(e)

        try:
            cmd = 'create tag x2ss22 (remove string)'
            resp = self.execute(cmd)
            self.check_resp_failed(resp)
        except Exception as e:
            assert "SyntaxError: syntax error" in str(e)

        try:
            cmd = 'create tag x233 (if string)'
            resp = self.execute(cmd)
            self.check_resp_failed(resp)
        except Exception as e:
            assert "SyntaxError: syntax error" in str(e)

        try:
            cmd = 'create tag x244 (not string)'
            resp = self.execute(cmd)
            self.check_resp_failed(resp)
        except Exception as e:
            assert "SyntaxError: syntax error" in str(e)

        try:
            cmd = 'create tag x255 (exists string)'
            resp = self.execute(cmd)
            self.check_resp_failed(resp)
        except Exception as e:
            assert "SyntaxError: syntax error" in str(e)

        try:
            cmd = 'create tag x266 (with string)'
            resp = self.execute(cmd)
            self.check_resp_failed(resp)
        except Exception as e:
            assert "SyntaxError: syntax error" in str(e)

        try:
            cmd = 'create tag x277 (change string)'
            resp = self.execute(cmd)
            self.check_resp_failed(resp)
        except Exception as e:
            assert "SyntaxError: syntax error" in str(e)

        try:
            cmd = 'create tag x280 (grant string)'
            resp = self.execute(cmd)
            self.check_resp_failed(resp)
        except Exception as e:
            assert "SyntaxError: syntax error" in str(e)

        try:
            cmd = 'create tag x281 (revoke string)'
            resp = self.execute(cmd)
            self.check_resp_failed(resp)
        except Exception as e:
            assert "SyntaxError: syntax error" in str(e)

        try:
            cmd = 'create tag x282 (on string)'
            resp = self.execute(cmd)
            self.check_resp_failed(resp)
        except Exception as e:
            assert "SyntaxError: syntax error" in str(e)

        try:
            cmd = 'create tag x284 (by string)'
            resp = self.execute(cmd)
            self.check_resp_failed(resp)
        except Exception as e:
            assert "SyntaxError: syntax error" in str(e)

        try:
            cmd = 'create tag x285 (in string)'
            resp = self.execute(cmd)
            self.check_resp_failed(resp)
        except Exception as e:
            assert "SyntaxError: syntax error" in str(e)

        # try:
        #     cmd = 'create tag x286 (ttl_durat string)'
        #     resp = self.execute(cmd)
        #     self.check_resp_failed(resp)
        # except Exception as e:
        #     assert "SyntaxError: syntax error" in str(e)

        try:
            cmd = 'create tag x287 (ttl_col string)'
            resp = self.execute(cmd)
            self.check_resp_succeeded(resp)
        except Exception as e:
            assert "SyntaxError: syntax error" in str(e)

        try:
            cmd = 'create tag x288 (download string)'
            resp = self.execute(cmd)
            self.check_resp_failed(resp)
        except Exception as e:
            assert "SyntaxError: syntax error" in str(e)

        try:
            cmd = 'create tag x289 (hdfs string)'
            resp = self.execute(cmd)
            self.check_resp_succeeded(resp)
        except Exception as e:
            assert "SyntaxError: syntax error" in str(e)

        try:
            cmd = 'create tag x290 (order string)'
            resp = self.execute(cmd)
            self.check_resp_failed(resp)
        except Exception as e:
            assert "SyntaxError: syntax error" in str(e)

        try:
            cmd = 'create tag x291 (ingest string)'
            resp = self.execute(cmd)
            self.check_resp_failed(resp)
        except Exception as e:
            assert "SyntaxError: syntax error" in str(e)

        try:
            cmd = 'create tag x292 (asc string)'
            resp = self.execute(cmd)
            self.check_resp_failed(resp)
        except Exception as e:
            assert "SyntaxError: syntax error" in str(e)

        try:
            cmd = 'create tag x293 (distinct string)'
            resp = self.execute(cmd)
            self.check_resp_failed(resp)
        except Exception as e:
            assert "SyntaxError: syntax error" in str(e)

        try:
            cmd = 'create tag x294 (variables string)'
            resp = self.execute(cmd)
            self.check_resp_succeeded(resp)
        except Exception as e:
            assert "SyntaxError: syntax error" in str(e)

        try:
            cmd = 'create tag x295 (get string)'
            resp = self.execute(cmd)
            self.check_resp_failed(resp)
        except Exception as e:
            assert "SyntaxError: syntax error" in str(e)

        try:
            cmd = 'create tag x296 (grant string)'
            resp = self.execute(cmd)
            self.check_resp_failed(resp)
        except Exception as e:
            assert "SyntaxError: syntax error" in str(e)

        try:
            cmd = 'create tag x297 (meta string)'
            resp = self.execute(cmd)
            self.check_resp_succeeded(resp)
        except Exception as e:
            assert "SyntaxError: syntax error" in str(e)

        try:
            cmd = 'create tag x298 (storage string)'
            resp = self.execute(cmd)
            self.check_resp_succeeded(resp)
        except Exception as e:
            assert "SyntaxError: syntax error" in str(e)

        try:
            cmd = 'create tag x299 (fetch string)'
            resp = self.execute(cmd)
            self.check_resp_failed(resp)
        except Exception as e:
            assert "SyntaxError: syntax error" in str(e)

        try:
            cmd = 'create tag x300 (prop string)'
            resp = self.execute(cmd)
            self.check_resp_failed(resp)
        except Exception as e:
            assert "SyntaxError: syntax error" in str(e)

        try:
            cmd = 'create tag x301 (all string)'
            resp = self.execute(cmd)
            self.check_resp_succeeded(resp)
        except Exception as e:
            assert "SyntaxError: syntax error" in str(e)

        try:
            cmd = 'create tag x302 (balance string)'
            resp = self.execute(cmd)
            self.check_resp_failed(resp)
        except Exception as e:
            assert "SyntaxError: syntax error" in str(e)

        try:
            cmd = 'create tag x3rclkls03 (of string)'
            resp = self.execute(cmd)
            self.check_resp_failed(resp)
        except Exception as e:
            assert "SyntaxError: syntax error" in str(e)

        try:
            cmd = 'create tag x3009mm3 (OF string)'
            resp = self.execute(cmd)
            self.check_resp_failed(resp)
        except Exception as e:
            assert "SyntaxError: syntax error" in str(e)

        try:
            cmd = 'create tag x3009zcssmm3 (PARTITION_NUM string)'
            resp = self.execute(cmd)
            self.check_resp_succeeded(resp)
        except Exception as e:
            assert "SyntaxError: syntax error" in str(e)

        try:
            cmd = 'create tag x3009mnsmm3 (REPLICA_FACTOR string)'
            resp = self.execute(cmd)
            self.check_resp_succeeded(resp)
        except Exception as e:
            assert "SyntaxError: syntax error" in str(e)

        try:
            cmd = 'create tag x3009m00011m3 (TTL_DURATION string)'
            resp = self.execute(cmd)
            self.check_resp_succeeded(resp)
        except Exception as e:
            assert "SyntaxError: syntax error" in str(e)

        try:
            cmd = 'create tag TTL_DURATION (TTL_DURATION string)'
            resp = self.execute(cmd)
            self.check_resp_succeeded(resp)
        except Exception as e:
            assert "SyntaxError: syntax error" in str(e)


        resp = self.execute('drop space test')
        self.check_resp_succeeded(resp)

    def test_keywords3(self):
        resp = self.execute(
            'CREATE SPACE IF NOT EXISTS test (partition_num=10, vid_type=FIXED_STRING(8))')
        self.check_resp_succeeded(resp)
        time.sleep(self.delay)

        # issue 447
        resp = self.execute('use test')
        self.check_resp_succeeded(resp)

        try:
            cmd = 'create tag `TAG` (`tag` string)'
            resp = self.execute(cmd)
            self.check_resp_succeeded(resp)
        except Exception as e:
            assert "SyntaxError: syntax error" in str(e)

        try:
            cmd = 'create EDGE `EDGE` (`EDGE` string)'
            resp = self.execute(cmd)
            self.check_resp_succeeded(resp)
        except Exception as e:
            assert "SyntaxError: syntax error" in str(e)

        resp = self.execute('drop space test')
        self.check_resp_succeeded(resp)


    @classmethod
    def cleanup(self):
        pass
