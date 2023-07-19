import time

from tests.common.nebula_test_suite import NebulaTestSuite

class TestBugUpdate(NebulaTestSuite):
    @classmethod
    def prepare(self):
        resp = self.execute(
            'CREATE SPACE IF NOT EXISTS issue1827_update(partition_num={partition_num}, replica_factor={replica_factor})'
            .format(partition_num=self.partition_num,
                    replica_factor=self.replica_factor))
        self.check_resp_succeeded(resp)

    def test_bugs_issue1827(self):
        time.sleep(self.delay)
        resp = self.execute('USE issue1827_update')
        self.check_resp_succeeded(resp)
        resp = self.execute(
            "CREATE TAG IF NOT EXISTS player(name string, age int)")
        self.check_resp_succeeded(resp)
        resp = self.execute("CREATE EDGE IF NOT EXISTS teammate(years int)")
        self.check_resp_succeeded(resp)
        time.sleep(self.delay)
        resp = self.execute(
            'INSERT VERTEX player(name, age) VALUES 100:(\"burning\", 20)')
        self.check_resp_succeeded(resp)
        resp = self.execute(
            'INSERT VERTEX player(name, age) VALUES 101:(\"longdd\", 31)')
        resp = self.execute('INSERT EDGE teammate(years) VALUES 100->101:(8)')
        self.check_resp_succeeded(resp)

        self.close_nebula_clients()
        self.create_nebula_clients()

        resp = self.execute("USE issue1827_update;UPDATE VERTEX 100 SET player.age = 31;")
        self.check_resp_succeeded(resp)

        self.close_nebula_clients()
        self.create_nebula_clients()
        resp = self.execute("USE issue1827_update;UPDATE EDGE 100 -> 101@0 OF teammate SET years = 7;")
        self.check_resp_succeeded(resp)

    @classmethod
    def cleanup(self):
        resp = self.execute('drop space issue1827_update')
        self.check_resp_succeeded(resp)
