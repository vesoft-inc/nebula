# --coding:utf-8--
#
# Copyright (c) 2020 vesoft inc. All rights reserved.
#
# This source code is licensed under Apache 2.0 License,
# attached with Common Clause Condition 1.0, found in the LICENSES directory.

import time

from tests.common.nebula_test_suite import NebulaTestSuite

class TestUpdateVertex(NebulaTestSuite):
    @classmethod
    def prepare(self):
        resp = self.execute('CREATE SPACE myspace_test_update(partition_num=1, replica_factor=1, vid_type=FIXED_STRING(20));'
                            'USE myspace_test_update;'
                            'CREATE TAG course(name string, credits int);'
                            'CREATE TAG building(name string);'
                            'CREATE TAG student(name string, age int, gender string);'
                            'CREATE TAG student_default(name string NOT NULL, age int NOT NULL, '
                            'gender string DEFAULT "one", birthday int DEFAULT 2010);'
                            'CREATE EDGE like(likeness double);'
                            'CREATE EDGE select(grade int, year int);'
                            'CREATE EDGE select_default(grade int NOT NULL, '
                            'year TIMESTAMP DEFAULT 1546308000)')
        self.check_resp_succeeded(resp)
        time.sleep(self.delay)

        resp = self.execute('INSERT VERTEX student(name, age, gender) VALUES  '
                            '"200":("Monica", 16, "female"),'
                            '"201":("Mike", 18, "male"),'
                            '"202":("Jane", 17, "female");')
        self.check_resp_succeeded(resp)

        resp = self.execute('INSERT VERTEX course(name, credits),building(name) VALUES '
                            '"101":("Math", 3, "No5"), '
                            '"102":("English", 6, "No11");')
        self.check_resp_succeeded(resp)

        resp = self.execute('INSERT VERTEX course(name, credits) VALUES "103":("CS", 5);')
        self.check_resp_succeeded(resp)

        resp = self.execute('INSERT EDGE select(grade, year) VALUES '
                            '"200" -> "101"@0:(5, 2018), '
                            '"200" -> "102"@0:(3, 2018), '
                            '"201" -> "102"@0:(3, 2019), '
                            '"202" -> "102"@0:(3, 2019);')
        self.check_resp_succeeded(resp)

        resp = self.execute('INSERT EDGE like(likeness) VALUES '
                            '"200" -> "201"@0:(92.5), '
                            '"201" -> "200"@0:(85.6), '
                            '"201" -> "202"@0:(93.2);')
        self.check_resp_succeeded(resp)

    @classmethod
    def cleanup(self):
        resp = self.execute('DROP SPACE myspace_test_update;')
        self.check_resp_succeeded(resp)

    def test_update_vertex(self):
        resp = self.execute('UPDATE VERTEX ON course "101" '
                            'SET credits = credits + 1;')
        self.check_resp_succeeded(resp)

        # filter out
        resp = self.execute('UPDATE VERTEX ON course "101" '
                            'SET credits = credits + 1 '
                            'WHEN name == "English" && credits > 2')
        self.check_resp_succeeded(resp)

        # set filter
        resp = self.execute('UPDATE VERTEX ON course "101" '
                            'SET credits = credits + 1 '
                            'WHEN name == "Math" && credits > 2')
        self.check_resp_succeeded(resp)

        # set yield
        resp = self.execute_query('UPDATE VERTEX ON course "101" '
                                  'SET credits = credits + 1 '
                                  'YIELD name AS Name, credits AS Credits')
        self.check_resp_succeeded(resp)
        expected_result = [["Math", 6]]
        self.check_result(resp, expected_result)

        # set filter and yield
        resp = self.execute_query('UPDATE VERTEX ON course "101" '
                                  'SET credits = credits + 1 '
                                  'WHEN name == "Math" && credits > 2 '
                                  'YIELD name AS Name, credits AS Credits')
        self.check_resp_succeeded(resp)
        expected_result = [["Math", 7]]
        self.check_result(resp, expected_result)

        # set filter out and yield
        resp = self.execute_query('UPDATE VERTEX ON course "101" '
                                  'SET credits = credits + 1 '
                                  'WHEN name == "notexist" && credits > 2'
                                  'YIELD name AS Name, credits AS Credits')
        self.check_resp_succeeded(resp)
        expected_result = [["Math", 7]]
        self.check_result(resp, expected_result)


    def test_update_edge(self):
        # resp = self.execute_query('FETCH PROP ON select "200"->"101"@0 '
        #                           'YIELD select.grade, select.year')
        # self.check_resp_succeeded(resp)
        # expected_result = [[200, 101, 0, 5, 2018]]
        # self.check_result(resp, expected_result)

        # update edge
        resp = self.execute('UPDATE EDGE ON select "200" -> "101"@0 '
                            'SET grade = grade + 1, year = 2000;')
        self.check_resp_succeeded(resp)

        # check
        # resp = self.execute_query('FETCH PROP ON select "200"->"101"@0 '
        #                           'YIELD select.grade, select.year')
        # self.check_resp_succeeded(resp)
        # expected_result = [[200, 101, 0, 6, 2000]]
        # self.check_result(resp, expected_result)

        resp = self.execute_query('GO FROM "101" OVER select REVERSELY '
                                  'YIELD select.grade, select.year')
        self.check_resp_succeeded(resp)
        expected_result = [[6, 2000]]
        self.check_result(resp, expected_result)

        # filter out, 2.0 storage not support update edge can use vertex
        resp = self.execute('UPDATE EDGE ON select "200" -> "101"@0 '
                            'SET grade = select.grade + 1, year = 2000 '
                            'WHEN select.grade > 1024 && $^.student.age > 15;')
        self.check_resp_failed(resp)

        # 2.0 test, filter out
        resp = self.execute('UPDATE EDGE ON select "200" -> "101"@0 '
                            'SET grade = select.grade + 1, year = 2000 '
                            'WHEN select.grade > 1024')
        self.check_resp_succeeded(resp)

        # set filter, 2.0 storage not support update edge can use vertex
        resp = self.execute('UPDATE EDGE ON select "200" -> "101"@0 '
                            'SET grade = select.grade + 1, year = 2000 '
                            'WHEN select.grade > 4 && $^.student.age > 15;')
        self.check_resp_failed(resp)

        # set filter
        resp = self.execute('UPDATE EDGE ON select "200" -> "101"@0 '
                            'SET grade = select.grade + 1, year = 2000 '
                            'WHEN select.grade > 4')
        self.check_resp_succeeded(resp)

        # set yield, 2.0 storage not support update edge can use vertex
        resp = self.execute_query('UPDATE EDGE ON select "200" -> "101"@0 '
                            'SET grade = select.grade + 1, year = 2018 '
                            'YIELD $^.student.name AS Name, select.grade AS Grade, select.year AS Year')
        self.check_resp_failed(resp)


        # set yield
        resp = self.execute_query('UPDATE EDGE ON select "200" -> "101"@0 '
                            'SET grade = select.grade + 1, year = 2018 '
                            'YIELD select.grade AS Grade, select.year AS Year')
        self.check_resp_succeeded(resp)
        expected_result = [[8, 2018]]
        self.check_result(resp, expected_result)

        # set filter and yield, 2.0 storage not support update edge can use vertex
        resp = self.execute_query('UPDATE EDGE ON select "200" -> "101"@0 '
                            'SET grade = select.grade + 1, year = 2019 '
                            'WHEN select.grade > 4 && $^.student.age > 15 '
                            'YIELD $^.student.name AS Name, select.grade AS Grade, select.year AS Year')
        self.check_resp_failed(resp)

        resp = self.execute_query('UPDATE EDGE ON select "200" -> "101"@0 '
                            'SET grade = select.grade + 1, year = 2019 '
                            'WHEN select.grade > 4 '
                            'YIELD select.grade AS Grade, select.year AS Year')
        self.check_resp_succeeded(resp)
        expected_result = [[9, 2019]]
        self.check_result(resp, expected_result)

        # set filter out and yield, 2.0 storage not support update edge can use vertex
        resp = self.execute_query('UPDATE EDGE ON select "200" -> "101"@0 '
                            'SET grade = select.grade + 1, year = 2019 '
                            'WHEN select.grade > 233333333333 && $^.student.age > 15 '
                            'YIELD $^.student.name AS Name, select.grade AS Grade, select.year AS Year')
        self.check_resp_failed(resp)

        # set filter out and yield
        resp = self.execute_query('UPDATE EDGE ON select "200" -> "101"@0 '
                                  'SET grade = select.grade + 1, year = 2019 '
                                  'WHEN select.grade > 233333333333'
                                  'YIELD select.grade AS Grade, select.year AS Year')
        self.check_resp_succeeded(resp)
        expected_result = [[9, 2019]]
        self.check_result(resp, expected_result)

    def test_update_vertex_failed(self):
        # update vertex: the item is TagName.PropName = Expression in SET clause, 2.0 supported
        resp = self.execute_query('UPDATE VERTEX ON course "101" '
                                  'SET credits = credits + 1, name = "No9"')
        self.check_resp_succeeded(resp)

        # the $$.TagName.PropName expressions are not allowed in any update sentence
        resp = self.execute_query('UPDATE VERTEX ON course "101" '
                                  'SET credits = $$.course.credits + 1 '
                                  'WHEN $$.course.name == "Math" && credits > $$.course.credits + 1 '
                                  'YIELD name AS Name, credits AS Credits, $$.building.name')
        self.check_resp_failed(resp)

        # make sure TagName and PropertyName must exist in all clauses
        resp = self.execute_query('UPDATE VERTEX ON nonexistentTag "101" '
                                  'SET credits = credits + 1, name = "No9" '
                                  'WHEN name == "Math" && nonexistentProperty > 2 '
                                  'YIELD name AS Name, $^.nonexistentTag.nonexistentProperty')
        self.check_resp_failed(resp)

    def test_update_edge_failed(self):
        # update edge: the item is PropName = Expression in SET clause
        resp = self.execute_query('UPDATE EDGE ON select "200" -> "101"@0'
                                  'SET select = select.grade + 1, select.year = 2019')
        self.check_resp_failed(resp)

        # make sure EdgeName and PropertyName must exist in all clauses
        resp = self.execute_query('UPDATE EDGE ON select "200" -> "101"@0 '
                                  'SET nonexistentProperty = select.grade + 1, year = 2019 '
                                  'WHEN nonexistentEdgeName.grade > 4 && $^.student.nonexistentProperty > 15 '
                                  'YIELD $^.nonexistentTag.name AS Name, select.nonexistentProperty AS Grade')
        self.check_resp_failed(resp)

        # make sure the edge_type must not exist
        resp = self.execute_query('UPDATE EDGE ON nonexistentEdgeTypeName "200" -> "101"@0 '
                                  'SET grade = select.grade + 1, year = 2019')
        self.check_resp_failed(resp)


    def test_upsert_vertex(self):
        # vertex not exist
        # resp = self.execute_query('FETCH PROP ON course "103" YIELD course.name, course.credits')
        # self.check_resp_succeeded(resp)
        # expected_result = [["103", "CS", 5]]
        # self.check_result(resp, expected_result)

        # not allow to handle multi tagid when update
        resp = self.execute('UPDATE VERTEX ON course "103" '
                            'SET credits = credits + 1, name = $^.building.name')
        self.check_resp_failed(resp)

        # update: vertex 103 ("CS", 5) --> ("CS", 6)
        resp = self.execute_query('UPDATE VERTEX ON course "103" '
                                  'SET credits = credits + 1 '
                                  'WHEN name == "CS" && credits > 2 '
                                  "YIELD name AS Name, credits AS Credits")
        self.check_resp_succeeded(resp)
        expected_result = [["CS", 6]]
        self.check_result(resp, expected_result)

        # when tag on vertex not exists, update failed
        resp = self.execute_query('UPDATE VERTEX ON course "104" '
                                  'SET credits = credits + 1  '
                                  'WHEN name == "CS" && credits > 2 '
                                  "YIELD name AS Name, credits AS Credits")
        self.check_resp_failed(resp)

        # has default value test,
        # Insertable: vertex 110 ("Ann") --> ("Ann", "one"),
        # 110 is nonexistent, gender with default value
        # TODO: storage not ready
        # resp = self.execute_query('UPSERT VERTEX ON student_default "110" '
        #                           'SET name = "Ann", age = 10 '
        #                           "YIELD name AS Name, gender AS Gender")
        # self.check_resp_succeeded(resp)
        # expected_result = [["Ann", "one"]]
        # self.check_result(resp, expected_result)

        # Insertable failed, 111 is nonexistent, name and age without default value
        resp = self.execute_query('UPSERT VERTEX ON student_default "111" '
                                  'SET name = "Tom", '
                                  'age = age + 8 '
                                  "YIELD name AS Name, age AS Age")
        self.check_resp_failed(resp)

        # Insertable failed, 111 is nonexistent, name without default value
        resp = self.execute_query('UPSERT VERTEX ON student_default "111" '
                                  'SET gender = "two", age = 10 '
                                  "YIELD name AS Name, gender AS Gender")
        self.check_resp_failed(resp)

        # Insertable: vertex 112 ("Lily") --> ("Lily", "one")
        # 112 is nonexistent, gender with default value
        # update student_default.age with string value
        resp = self.execute_query('UPSERT VERTEX ON student_default "112" '
                                  'SET name = "Lily", age = "10"'
                                  "YIELD name AS Name, gender AS Gender")
        self.check_resp_failed(resp)

        # Insertable: vertex 113 ("Jack") --> ("Jack", "Three")
        # 113 is nonexistent, gender with default value,
        # update student_default.age with string value
        resp = self.execute_query('UPSERT VERTEX ON student_default "113" '
                                  'SET name = "Ann", age = "10"'
                                  "YIELD name AS Name, gender AS Gender")
        self.check_resp_failed(resp)

        # when tag on vertex not exists, update failed
        resp = self.execute_query('UPDATE VERTEX ON course "104" '
                                  'SET credits = credits + 1 '
                                  'WHEN name == \"CS\" && credits > 2 '
                                  'YIELD name AS Name, credits AS Credits')
        self.check_resp_failed(resp)

        # Insertable success, 115 is nonexistent, name and age without default value,
        # the filter is always true.
        # TODO: storage not ready
        # resp = self.execute_query('UPSERT VERTEX ON student_default "115" '
        #                           'SET name = "Kate", age = 12 '
        #                           'WHEN gender == "two" '
        #                           "YIELD name AS Name, age AS Age, gender AS gender")
        # self.check_resp_succeeded(resp)
        # expected_result = [["Kate", 12, "one"]]
        # self.check_result(resp, expected_result)

        # Order problem
        # Insertable success, 116 is nonexistent, name and age without default value,
        # the filter is always true.
        # TODO: storage not ready
        # resp = self.execute_query('UPSERT VERTEX ON student_default "116" '
        #                           'SET name = "Kate", age = birthday + 1,'
        #                           'birthday = birthday + 1 '
        #                           'WHEN gender == "two" '
        #                           'YIELD name AS Name, age AS Age, '
        #                           'gender AS gender, birthday AS birthday')
        # self.check_resp_succeeded(resp)
        # expected_result = [['Kate', 2011, 'one', 2011]]
        # self.check_result(resp, expected_result)

        # Order problem
        # Insertable success, 117 is nonexistent, name and age without default value,
        # the filter is always true.
        # TODO: storage not ready
        # resp = self.execute_query('UPSERT VERTEX ON student_default "117" '
        #                           'SET birthday = birthday + 1, name = "Kate", age = birthday + 1 '
        #                           'YIELD name AS Name, age AS Age, gender AS gender, birthday AS birthday')
        # self.check_resp_succeeded(resp)
        # expected_result = [["Kate", 2012, "one", 2011]]
        # self.check_result(resp, expected_result)

    def test_upsert_edge(self):
        # resp = self.execute_query('FETCH PROP ON select "200"->"101"@0 YIELD select.grade, select.year')
        # self.check_resp_succeeded(resp)
        # expected_result = [["200", "101", 0, 5, 2018]]
        # self.check_result(resp, expected_result)

        # Insertable, upsert when edge exists, 2.0 storage not support
        resp = self.execute_query('UPSERT EDGE ON select "201" -> "101"@0'
                                  'SET grade = 3, year = 2019 '
                                  'WHEN $^.student.age > 15 && $^.student.gender == "male" '
                                  'YIELD select.grade AS Grade, select.year AS Year')
        self.check_resp_failed(resp)

        # Insertable, upsert when edge not exist
        resp = self.execute_query('UPSERT EDGE ON select "201" -> "101"@0'
                                  'SET grade = 3, year = 2019 '
                                  'WHEN select.grade > 3 '
                                  'YIELD select.grade AS Grade, select.year AS Year')
        self.check_resp_succeeded(resp)
        expected_result = [[3, 2019]]
        self.check_result(resp, expected_result)

        # update when edge not exists, failed
        resp = self.execute_query('UPDATE EDGE ON select "601" -> "101"@0'
                                  'SET year = 2019 '
                                  'WHEN select.grade >10 '
                                  'YIELD select.grade AS Grade, select.year AS Year')
        self.check_resp_failed(resp)

        # upsert when edge not exists,success
        # filter condition is always true, insert default value or update value
        resp = self.execute_query('UPSERT EDGE ON select "601" -> "101"@0'
                                  'SET year = 2019, grade = 3 '
                                  'WHEN grade >10 '
                                  'YIELD grade AS Grade, year AS Year')
        self.check_resp_succeeded(resp)
        expected_result = [[3, 2019]]
        self.check_result(resp, expected_result)

        # resp = self.execute_query('FETCH PROP ON select 601->101@0 YIELD select.grade, select.year')
        # self.check_resp_succeeded(resp)
        # expected_result = [["601", "101", 0, 3, 2019]]
        # self.check_result(resp, expected_result)

        # select_default's year with default value, timestamp not support
        # resp = self.execute_query('UPSERT EDGE ON select_default "111" -> "222"@0 '
        #                           'SET grade = 3 '
        #                           'YIELD select_default.grade AS Grade, select_default.year AS Year')
        # self.check_resp_succeeded(resp)
        # expected_result = [[3, 1546308000]]
        # self.check_result(resp, expected_result)

        # select_default's year is timestamp type, set str type, , timestamp not support
        # resp = self.execute_query('UPSERT EDGE ON select_default "222" -> "333"@0 '
        #                           'SET grade = 3, year = "2020-01-10 10:00:00" '
        #                           'YIELD select_default.grade AS Grade, select_default.year AS Year')
        # self.check_resp_succeeded(resp)
        # expected_result = [[3, 1578621600]]
        # self.check_result(resp, expected_result)

        # select_default's grade without default value
        resp = self.execute_query('UPSERT EDGE ON select_default "444" -> "555"@0 '
                                  'SET year = 1546279201 '
                                  'YIELD grade AS Grade, year AS Year')
        self.check_resp_failed(resp)

        # select_default's grade without default value
        resp = self.execute_query('UPSERT EDGE ON select_default "333" -> "444"@0 '
                                  'SET grade = 3 + grade '
                                  'YIELD grade AS Grade, year AS Year')
        self.check_resp_failed(resp)

        # update select_default's year with edge prop value
        # TODO: storage not ready
        # resp = self.execute_query('UPSERT EDGE ON select_default "222" -> "444"@0 '
        #                           'SET grade = 3, year = year + 10 '
        #                           'YIELD grade AS Grade, year AS Year')
        # self.check_resp_succeeded(resp)

        # TODO: timestamp has not supported
        # expected_result = [[3, 1546308010]]
        # self.check_result(resp, expected_result)


    def test_update_not_exist(self):
        # make sure the vertex must not exist
        resp = self.execute_query('UPDATE VERTEX ON course "1010000" '
                                  'SET credits = credits + 1, name = "No9" '
                                  'WHEN name == "Math" && credits > 2 '
                                  'YIELD name as Name')
        self.check_resp_failed(resp)

        # make sure the edge(src, dst) must not exist
        resp = self.execute_query('UPDATE EDGE ON select "200" -> "101000000000000"@0  '
                                  'SET grade = 1, year = 2019 ')
        self.check_resp_failed(resp)

        # make sure the edge(src, ranking, dst) must not exist
        resp = self.execute_query('UPDATE EDGE ON select "200" -> "101"@123456789  '
                                  'SET grade = grade + 1, year = 2019 ')
        self.check_resp_failed(resp)


    def test_upsert_then_insert(self):
        resp = self.execute_query('UPSERT VERTEX ON building "100" SET name = "No1"')
        self.check_resp_succeeded(resp)

        # resp = self.execute_query('FETCH PROP on building "100"')
        # self.check_resp_succeeded(resp)
        # expected_result = [["100", "No1"]]
        # self.check_result(resp, expected_result)

        resp = self.execute('INSERT VERTEX building(name) VALUES "100": ("No2")')
        self.check_resp_succeeded(resp)

        # resp = self.execute_query('FETCH PROP on building "100"')
        # self.check_resp_succeeded(resp)
        # expected_result = [["100", "No2"]]
        # self.check_result(resp, expected_result)

        resp = self.execute_query('UPSERT VERTEX ON building "101" SET name = "No1"')
        self.check_resp_succeeded(resp)

        # resp = self.execute_query('FETCH PROP on building "101"')
        # self.check_resp_succeeded(resp)
        # expected_result = [["101", "No1"]]
        # self.check_result(resp, expected_result)

        resp = self.execute('INSERT VERTEX building(name) VALUES "101": ("No2")')
        self.check_resp_succeeded(resp)

        # resp = self.execute_query('FETCH PROP on building "101"')
        # self.check_resp_succeeded(resp)
        # expected_result = [["101", "No2"]]
        # self.check_result(resp, expected_result)

    def test_upsert_after_alter_schema(self):
        resp = self.execute('INSERT VERTEX building(name) VALUES "100": ("No1")')
        self.check_resp_succeeded(resp)

        resp = self.execute('ALTER TAG building ADD (new_field string default "123")')
        self.check_resp_succeeded(resp)

        time.sleep(self.delay)

        resp = self.execute('UPSERT VERTEX ON building "100" SET name = "No2"')
        self.check_resp_succeeded(resp)

        # resp = self.execute_query('FETCH PROP on building "100"')
        # self.check_resp_succeeded(resp)
        # expected_result = [["100", "No2", "123"]]
        # self.check_result(resp, expected_result)

        resp = self.execute('UPSERT VERTEX ON building "100" SET name = "No3", new_field = "321"')
        self.check_resp_succeeded(resp)

        # resp = self.execute_query('FETCH PROP on building "101"')
        # self.check_resp_succeeded(resp)
        # expected_result = [["100", "No3", "321"]]
        # self.check_result(resp, expected_result)

        resp = self.execute('UPSERT VERTEX ON building "101" SET name = "No1", new_field = "No2"')
        self.check_resp_succeeded(resp)

        # resp = self.execute_query('FETCH PROP on building "101"')
        # self.check_resp_succeeded(resp)
        # expected_result = [["100",  "No1", "No2"]]
        # self.check_result(resp, expected_result)

        # Test upsert edge after alter schema
        resp = self.execute('INSERT EDGE like(likeness) VALUES "1" -> "100":(1.0)')
        self.check_resp_succeeded(resp)

        resp = self.execute('ALTER EDGE like ADD (new_field string default "123")')
        self.check_resp_succeeded(resp)

        time.sleep(self.delay)

        resp = self.execute('UPSERT EDGE ON like "1"->"100" SET likeness = 2.0')
        self.check_resp_succeeded(resp)

        # resp = self.execute_query('FETCH PROP ON like "1"->"100"@0')
        # self.check_resp_succeeded(resp)
        # expected_result = [["1", "100", 0, 2.0, "123"]]
        # self.check_result(resp, expected_result)

        resp = self.execute('UPSERT EDGE ON like "1"->"100" SET likeness = 3.0, new_field = "321"')
        self.check_resp_succeeded(resp)

        # resp = self.execute_query('FETCH PROP ON like "1"->"100"@0')
        # self.check_resp_succeeded(resp)
        # expected_result = [["1", "100", 0, 3.0, "321"]]
        # self.check_result(resp, expected_result)

        resp = self.execute('UPSERT EDGE ON like "1"->"101" SET likeness = 1.0, new_field = "111"')
        self.check_resp_succeeded(resp)

        # resp = self.execute_query('FETCH PROP ON like "1"->"101"@0')
        # self.check_resp_succeeded(resp)
        # expected_result = [["1", "101", 0, 1.0, "111"]]
        # self.check_result(resp, expected_result)

