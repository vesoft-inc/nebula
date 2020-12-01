# --coding:utf-8--
#
# Copyright (c) 2020 vesoft inc. All rights reserved.
#
# This source code is licensed under Apache 2.0 License,
# attached with Common Clause Condition 1.0, found in the LICENSES directory.

import time

from tests.common.nebula_test_suite import NebulaTestSuite


class TestDeleteEdges(NebulaTestSuite):
    @classmethod
    def prepare(self):
        resp = self.execute('CREATE SPACE TestDeleteEdges(partition_num=1, replica_factor=1);'
                            'USE TestDeleteEdges;'
                            'CREATE TAG person(name string, age int);'
                            'CREATE EDGE friend(intimacy int);'
                            'CREATE EDGE schoolmate(likeness int);'
                            'CREATE EDGE transfer(money int);')
        self.check_resp_succeeded(resp)
        time.sleep(self.delay)

    @classmethod
    def cleanup(self):
        resp = self.execute('DROP SPACE TestDeleteEdges;')
        self.check_resp_succeeded(resp)

    def test_delete_edge(self):
        resp = self.execute('INSERT VERTEX person(name, age) VALUES '
                            '"Zhangsan":("Zhangsan", 22), "Lisi":("Lisi", 23),'
                            '"Jack":("Jack", 18), "Rose":("Rose", 19);')
        self.check_resp_succeeded(resp)

        resp = self.execute('INSERT  EDGE friend(intimacy) VALUES '
                            '"Zhangsan"->"Lisi"@15:(90), '
                            '"Zhangsan"->"Jack"@12:(50),'
                            '"Jack"->"Rose"@13:(100);')
        self.check_resp_succeeded(resp)

        resp = self.execute('INSERT EDGE schoolmate(likeness) VALUES '
                            '"Zhangsan"->"Jack":(60),'
                            '"Lisi"->"Rose":(70);')
        self.check_resp_succeeded(resp)

        resp = self.execute('INSERT EDGE transfer(money) VALUES '
                            '"Zhangsan"->"Lisi"@1561013236:(33),'
                            '"Zhangsan"->"Lisi"@1561013237:(77)')
        self.check_resp_succeeded(resp)

        resp = self.execute('GO FROM "Zhangsan", "Jack" OVER friend '
                                  'YIELD $^.person.name, friend.intimacy, friend._dst')
        self.check_resp_succeeded(resp)
        expect_result = [["Zhangsan", 90, "Lisi"], ["Zhangsan", 50, "Jack"], ["Jack", 100, "Rose"]]
        self.check_out_of_order_result(resp, expect_result)

        resp = self.execute('GO FROM "Zhangsan", "Lisi" OVER schoolmate '
                                  'YIELD $^.person.name, schoolmate.likeness, schoolmate._dst')
        self.check_resp_succeeded(resp)
        expect_result = [["Zhangsan", 60, "Jack"], ["Lisi", 70, "Rose"]]
        self.check_out_of_order_result(resp, expect_result)

        resp = self.execute('GO FROM "Zhangsan" OVER transfer YIELD $^.person.name,'
                                  'transfer._rank, transfer.money, transfer._dst')
        self.check_resp_succeeded(resp)
        expect_result = [["Zhangsan", 1561013236, 33, "Lisi"], ["Zhangsan", 1561013237, 77, "Lisi"]]
        self.check_out_of_order_result(resp, expect_result)

        resp = self.execute('DELETE EDGE friend "Zhangsan"->"Lisi"@15, "Jack"->"Rose"@13')
        self.check_resp_succeeded(resp)

        resp = self.execute('DELETE EDGE schoolmate "Lisi"->"Rose"')
        self.check_resp_succeeded(resp)

        resp = self.execute('DELETE EDGE transfer "Zhangsan"->"Lisi"@1561013237')
        self.check_resp_succeeded(resp)

        # check
        resp = self.execute('GO FROM "Zhangsan", "Jack" OVER friend '
                                  'YIELD $^.person.name, friend.intimacy, friend._dst')
        self.check_resp_succeeded(resp)
        expect_result = [["Zhangsan", 50, "Jack"]]
        self.check_result(resp, expect_result)

        resp = self.execute('GO FROM "Zhangsan", "Lisi" OVER schoolmate '
                                  'YIELD $^.person.name, schoolmate.likeness, schoolmate._dst')
        self.check_resp_succeeded(resp)
        expect_result = [["Zhangsan", 60, "Jack"]]
        self.check_result(resp, expect_result)

        resp = self.execute('GO FROM "Zhangsan", "Jack" OVER transfer '
                                  'YIELD $^.person.name, transfer._rank, transfer.money, transfer._dst')
        self.check_resp_succeeded(resp)
        expect_result = [["Zhangsan", 1561013236, 33, "Lisi"]]
        self.check_result(resp, expect_result)

        # delete non-existing edges and a same edge
        resp = self.execute('DELETE EDGE friend "Zhangsan"->"Rose", "1008"->"1009"@17,'
                            '"Zhangsan"->"Lisi"@15')
        self.check_resp_succeeded(resp)

        resp = self.execute('GO FROM "Zhangsan","Jack" OVER friend '
                                  'YIELD $^.person.name, friend.intimacy, friend._dst')
        self.check_resp_succeeded(resp)
        expect_result = [["Zhangsan", 50, "Jack"]]
        self.check_result(resp, expect_result)
