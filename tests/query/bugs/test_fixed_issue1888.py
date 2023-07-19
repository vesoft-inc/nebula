import time

import pytest

from tests.common.nebula_test_suite import NebulaTestSuite

@pytest.mark.parametrize('sentence', ['UPDATE', 'UPSERT'])
class TestBugUpdateFilterOut(NebulaTestSuite):
    space      = 'issue1888_update'
    tag        = 'issue1888_update_tag'
    edge_type  = 'issue1888_update_edge'

    vertex = 233

    @classmethod
    def prepare(self):
        resp = self.execute('CREATE SPACE {}'.format(TestBugUpdateFilterOut.space))
        self.check_resp_succeeded(resp)
        time.sleep(self.delay)
        resp = self.execute('USE {}'.format(TestBugUpdateFilterOut.space))
        self.check_resp_succeeded(resp)
        # schema
        resp = self.execute('CREATE TAG {}(id int, name string)'.format(TestBugUpdateFilterOut.tag))
        self.check_resp_succeeded(resp)
        resp = self.execute('CREATE EDGE {}(id int, name string)'.format(TestBugUpdateFilterOut.edge_type))
        self.check_resp_succeeded(resp)
        time.sleep(self.delay)
        # data
        resp = self.execute('INSERT VERTEX {}(id, name) VALUE {}:(0, "shylock")'.format(TestBugUpdateFilterOut.tag, TestBugUpdateFilterOut.vertex))
        self.check_resp_succeeded(resp)
        resp = self.execute('INSERT EDGE {}(id, name) VALUE {}->2333:(0, "shylock")'.format(TestBugUpdateFilterOut.edge_type, TestBugUpdateFilterOut.vertex))
        self.check_resp_succeeded(resp)

    # https://github.com/vesoft-inc/nebula/issues/1888
    def test_bugs_issue1888(self, sentence):
        # update vertex filter out
        resp = self.execute('{} VERTEX {} SET {}.name = "hg" WHEN $^.{}.id > 0'.format(
            sentence, TestBugUpdateFilterOut.vertex, TestBugUpdateFilterOut.tag, TestBugUpdateFilterOut.tag))
        self.check_resp_succeeded(resp)
        resp = self.execute('FETCH PROP ON {} {}'.format(TestBugUpdateFilterOut.tag, TestBugUpdateFilterOut.vertex))
        self.check_resp_succeeded(resp)
        expect = [[TestBugUpdateFilterOut.vertex, 0, 'shylock']]
        self.check_result(resp.rows, expect)

        # update edge filter out
        resp = self.execute('{} EDGE {}->2333 OF {} SET name = "hg" WHEN {}.id > 0'.format(
            sentence, TestBugUpdateFilterOut.vertex, TestBugUpdateFilterOut.edge_type, TestBugUpdateFilterOut.edge_type))
        self.check_resp_succeeded(resp)
        resp = self.execute('FETCH PROP ON {} {}->2333'.format(TestBugUpdateFilterOut.edge_type, TestBugUpdateFilterOut.vertex))
        self.check_resp_succeeded(resp)
        expect = [[TestBugUpdateFilterOut.vertex, 2333, 0, 0, 'shylock']]
        self.check_result(resp.rows, expect)


    @classmethod
    def cleanup(self):
        print('Debug Point Clean Up')
        resp = self.execute('DROP SPACE {}'.format(TestBugUpdateFilterOut.space))
        self.check_resp_succeeded(resp)
