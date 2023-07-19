import time

from tests.common.nebula_test_suite import NebulaTestSuite

class TestBugUpdateFilterOut(NebulaTestSuite):
    space = 'issue2009_default'
    tag = 'issue2009_default_tag'
    edge_type = 'issue2009_default_edge'

    vertex = 233

    @classmethod
    def prepare(self):
        resp = self.execute('CREATE SPACE {}'.format(
            TestBugUpdateFilterOut.space))
        self.check_resp_succeeded(resp)
        time.sleep(self.delay)
        resp = self.execute('USE {}'.format(TestBugUpdateFilterOut.space))
        self.check_resp_succeeded(resp)
        # schema
        resp = self.execute('CREATE TAG {}(id int DEFAULT 0, '
                            'name string DEFAULT \'shylock\', '
                            'age double DEFAULT 99.0, '
                            'male bool DEFAULT true, '
                            'birthday timestamp DEFAULT 0)'.format(TestBugUpdateFilterOut.tag))
        self.check_resp_succeeded(resp)
        resp = self.execute('CREATE EDGE {}(id int DEFAULT 0, '
                            'name string DEFAULT \'shylock\', '
                            'age double DEFAULT 99.0, '
                            'male bool DEFAULT true, '
                            'birthday timestamp DEFAULT 0)'.format(TestBugUpdateFilterOut.edge_type))
        self.check_resp_succeeded(resp)
        time.sleep(self.delay)

    # https://github.com/vesoft-inc/nebula/issues/2009
    def test_bugs_issue2009(self):
        # data
        resp = self.execute('INSERT VERTEX {}() VALUE {}:()'.format(
            TestBugUpdateFilterOut.tag, TestBugUpdateFilterOut.vertex))
        self.check_resp_succeeded(resp)
        resp = self.execute('INSERT EDGE {}() VALUE {}->2333:()'.format(
            TestBugUpdateFilterOut.edge_type, TestBugUpdateFilterOut.vertex))
        self.check_resp_succeeded(resp)
        # fetch
        resp = self.execute('FETCH PROP ON {} {}'.format(
            TestBugUpdateFilterOut.tag, TestBugUpdateFilterOut.vertex))
        self.check_resp_succeeded(resp)
        expect = [
            [TestBugUpdateFilterOut.vertex, 0, 'shylock', 99.0, True, 0]
        ]
        self.check_result(resp.rows, expect)

        resp = self.execute('FETCH PROP ON {} {}->2333'.format(
            TestBugUpdateFilterOut.edge_type, TestBugUpdateFilterOut.vertex))
        self.check_resp_succeeded(resp)
        expect = [
            [TestBugUpdateFilterOut.vertex, 2333, 0, 0, 'shylock', 99.0, True, 0]
        ]
        self.check_result(resp.rows, expect)

    @classmethod
    def cleanup(self):
        print('Debug Point Clean Up')
        resp = self.execute('DROP SPACE {}'.format(
            TestBugUpdateFilterOut.space))
        self.check_resp_succeeded(resp)
