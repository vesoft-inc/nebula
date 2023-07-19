import time
import pytest

from tests.common.nebula_test_suite import NebulaTestSuite
from tests.bench.data_generate import generate_insert_student_vertex, generate_insert_likeness_edge


class TestInsertBench(NebulaTestSuite):
    @classmethod
    def prepare(self):
        resp = self.execute(
            'CREATE SPACE IF NOT EXISTS benchinsertspace(partition_num={partition_num}, replica_factor={replica_factor})'
            .format(partition_num=self.partition_num,
                    replica_factor=self.replica_factor))
        self.check_resp_succeeded(resp)
        time.sleep(self.graph_delay)
        resp = self.execute('USE benchinsertspace')
        self.check_resp_succeeded(resp)
        resp = self.execute(
            'CREATE TAG IF NOT EXISTS person(name string, age int)')
        self.check_resp_succeeded(resp)

        self.execute('CREATE EDGE IF NOT EXISTS like(likeness int)')
        self.check_resp_succeeded(resp)
        time.sleep(self.graph_delay)

    @classmethod
    def cleanup(self):
        resp = self.execute('drop space benchinsertspace')
        self.check_resp_succeeded(resp)

    def insert_vertex(self, duration=0.000001):
        resp = self.execute('USE benchinsertspace')
        self.check_resp_succeeded(resp)

        for i in range(20000):
            query = generate_insert_student_vertex(50, 50 * i)
            resp = self.execute(query)
            self.check_resp_succeeded(resp)

    def insert_edge(self, duration=0.000001):
        resp = self.execute('USE benchinsertspace')
        self.check_resp_succeeded(resp)

        for i in range(20000):
            query = generate_insert_likeness_edge(50, 50 * i)
            resp = self.execute(query)
            self.check_resp_succeeded(resp)

    @pytest.mark.benchmark(
        group="insert",
        min_time=0.1,
        max_time=0.5,
        min_rounds=1,
        timer=time.time,
        disable_gc=True,
        warmup=False
    )
    def test_insert_vertex(self, benchmark):
        benchmark(self.insert_vertex)

    @pytest.mark.benchmark(
        group="insert",
        min_time=0.1,
        max_time=0.5,
        min_rounds=1,
        timer=time.time,
        disable_gc=True,
        warmup=False
    )
    def test_insert_edge(self, benchmark):
        benchmark(self.insert_edge)
