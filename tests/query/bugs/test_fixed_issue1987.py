import time

import pytest

from tests.common.nebula_test_suite import NebulaTestSuite


@pytest.mark.parametrize('schema', [('TAG', 'VERTEX', '233'), ('EDGE', 'EDGE', '233->2333')])
class TestBugUpdateFilterOut(NebulaTestSuite):
    space = 'issue1987_update'

    @classmethod
    def prepare(self):
        resp = self.execute('DROP SPACE IF EXISTS {}'.format(
            TestBugUpdateFilterOut.space))
        self.check_resp_succeeded(resp)
        resp = self.execute('CREATE SPACE {}'.format(
            TestBugUpdateFilterOut.space))
        self.check_resp_succeeded(resp)
        time.sleep(self.delay)
        resp = self.execute('USE {}'.format(TestBugUpdateFilterOut.space))
        self.check_resp_succeeded(resp)

    # https://github.com/vesoft-inc/nebula/issues/1987
    def test_bugs_issue1987(self, schema):
        # schema
        resp = self.execute('DROP TAG IF EXISTS t')
        self.check_resp_succeeded(resp)
        resp = self.execute('DROP EDGE IF EXISTS t')
        self.check_resp_succeeded(resp)
        resp = self.execute('CREATE {} t(id int, name string)'.format(schema[0]))
        self.check_resp_succeeded(resp)
        time.sleep(self.delay)

        # ADD default value
        resp = self.execute(
            'ALTER {} t CHANGE (id int DEFAULT 233, name string DEFAULT \'shylock\')'.format(schema[0]))
        self.check_resp_succeeded(resp)
        time.sleep(self.delay)
        resp = self.execute(
            'INSERT {} t() values {}:()'.format(schema[1], schema[2]))
        self.check_resp_succeeded(resp)
        resp = self.execute('FETCH PROP ON t {}'.format(schema[2]))
        self.check_resp_succeeded(resp)
        ignore = {0} if schema[0] == 'TAG' else {0, 1, 2}
        expect = [[233, 'shylock']]
        self.check_result(resp.rows, expect, ignore)

        # Change default value
        resp = self.execute(
            'ALTER {} t CHANGE (id int DEFAULT 444, name string DEFAULT \'hg\')'.format(schema[0]))
        self.check_resp_succeeded(resp)
        time.sleep(self.delay)
        resp = self.execute(
            'INSERT {} t() values {}:()'.format(schema[1], schema[2]))
        self.check_resp_succeeded(resp)
        resp = self.execute('FETCH PROP ON t {}'.format(schema[2]))
        self.check_resp_succeeded(resp)
        ignore = {0} if schema[0] == 'TAG' else {0, 1, 2}
        expect = [[444, 'hg']]
        self.check_result(resp.rows, expect, ignore)

        # Drop default value
        resp = self.execute(
            'ALTER {} t CHANGE (id int, name string)'.format(schema[0]))
        self.check_resp_succeeded(resp)
        time.sleep(self.delay)
        resp = self.execute(
            'INSERT {} t() values {}:()'.format(schema[1], schema[2]))
        self.check_resp_failed(resp)

    @classmethod
    def cleanup(self):
        resp = self.execute('DROP SPACE {}'.format(
            TestBugUpdateFilterOut.space))
        self.check_resp_succeeded(resp)
