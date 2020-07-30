import time
import pytest
from graph import ttypes
from tests.common.nebula_test_suite import NebulaTestSuite
from tests.bench.data_generate import insert_vertexs, insert_edges


class TestLookupBench(NebulaTestSuite):
    @classmethod
    def prepare(self):
        resp = self.execute(
            'CREATE SPACE IF NOT EXISTS benchlookupspace(partition_num={partition_num}, replica_factor={replica_factor})'
            .format(partition_num=self.partition_num,
                    replica_factor=self.replica_factor)')
        self.check_resp_succeeded(resp)
        time.sleep(4)
        resp = self.execute('USE benchlookupspace')
        self.check_resp_succeeded(resp)
        resp = self.execute(
            'CREATE TAG IF NOT EXISTS person(name string, age int)')
        self.check_resp_succeeded(resp)
        resp = self.execute(
            'CREATE TAG index IF NOT EXISTS personName on person(name)')
        self.check_resp_succeeded(resp)
        resp = self.execute(
            'CREATE TAG index IF NOT EXISTS personAge on person(age)')
        self.check_resp_succeeded(resp)
        time.sleep(4)
        insert_vertexs(self, "benchlookupspace", 50, 20000)
        self.execute('CREATE EDGE IF NOT EXISTS like(likeness int)')
        self.check_resp_succeeded(resp)
        time.sleep(4)
        insert_edges(self, "benchlookupspace", 50, 20000)

    @classmethod
    def cleanup(self):
        resp = self.execute('drop space benchlookupspace')
        self.check_resp_succeeded(resp)

    def lookup(self):
        resp = self.execute('USE benchlookupspace')
        self.check_resp_succeeded(resp)
        resp = self.execute('lookup on person where person.age < 0 ')
        self.check_resp_succeeded(resp)
        resp = self.execute('lookup on person where person.age > 0 ')
        self.check_resp_succeeded(resp)
        resp = self.execute('lookup on person where person.age > 60')
        self.check_resp_succeeded(resp)
        resp = self.execute('lookup on person where person.age > 90')
        self.check_resp_succeeded(resp)
        resp = self.execute('lookup on person where person.name == "sssssaass" ')
        self.check_resp_succeeded(resp)
        resp = self.execute('lookup on person where person.name == "saaaaaass" ')
        self.check_resp_succeeded(resp)
        resp = self.execute('lookup on person where person.age < 10 ')
        self.check_resp_succeeded(resp)
        resp = self.execute('lookup on person where person.age > 80 ')
        self.check_resp_succeeded(resp)
        resp = self.execute('lookup on person where person.age > 60')
        self.check_resp_succeeded(resp)
        resp = self.execute('lookup on person where person.age > 90')
        self.check_resp_succeeded(resp)

    @pytest.mark.benchmark(
        group="lookup",
        min_time=0.1,
        max_time=0.5,
        min_rounds=10,
        timer=time.time,
        disable_gc=True,
        warmup=False
    )
    def test_lookup(self, benchmark):
        benchmark(self.lookup)
