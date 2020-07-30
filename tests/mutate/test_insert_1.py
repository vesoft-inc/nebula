# --coding:utf-8--
#
# Copyright (c) 2020 vesoft inc. All rights reserved.
#
# This source code is licensed under Apache 2.0 License,
# attached with Common Clause Condition 1.0, found in the LICENSES directory.

import time
import pytest

from tests.common.nebula_test_suite import NebulaTestSuite


class TestInsert1(NebulaTestSuite):
    @classmethod
    def prepare(self):
        resp = self.execute(
            'CREATE SPACE IF NOT EXISTS mySpace(partition_num=1, vid_size=20)')
        self.check_resp_succeeded(resp)

        # 2.0 use space get from cache
        time.sleep(self.delay)

        resp = self.execute('USE mySpace')
        self.check_resp_succeeded(resp)

        resp = self.execute('CREATE TAG person(name string, age int)')
        self.check_resp_succeeded(resp)

        resp = self.execute('CREATE TAG personWithDefault(name string DEFAULT "", '
                            'age int DEFAULT 18, '
                            'isMarried bool DEFAULT false, '
                            'BMI double DEFAULT 18.5, '
                            'department string DEFAULT "engineering",'
                            'birthday timestamp DEFAULT '
                            '"2020-01-10 10:00:00")')
        self.check_resp_succeeded(resp)

        resp = self.execute('CREATE TAG student(grade string, number int)')
        self.check_resp_succeeded(resp)

        resp = self.execute('CREATE TAG studentWithDefault'
                            '(grade string DEFAULT "one", number int)')
        self.check_resp_succeeded(resp)

        resp = self.execute('CREATE TAG employee(name int)')
        self.check_resp_succeeded(resp)

        resp = self.execute('CREATE TAG interest(name string)')
        self.check_resp_succeeded(resp)

        resp = self.execute('CREATE TAG school(name string, create_time timestamp)')
        self.check_resp_succeeded(resp)

        resp = self.execute('CREATE EDGE schoolmate(likeness int, nickname string)')
        self.check_resp_succeeded(resp)

        resp = self.execute('CREATE EDGE schoolmateWithDefault(likeness int DEFAULT 80)')
        self.check_resp_succeeded(resp)

        resp = self.execute('CREATE EDGE study(start_time timestamp, end_time timestamp)')
        self.check_resp_succeeded(resp)

        time.sleep(self.delay)

    @classmethod
    def cleanup(self):
        resp = self.execute('DROP SPACE mySpace')
        self.check_resp_succeeded(resp)

    def test_insert_failed(self):
        # insert vertex wrong type value
        resp = self.execute('INSERT VERTEX person(name, age) VALUES "Tom":("Tom", "2")')
        self.check_resp_failed(resp)

        # insert vertex wrong num of value
        resp = self.execute('INSERT VERTEX person(name) VALUES "Tom":("Tom", 2)')
        self.check_resp_failed(resp)

        # insert vertex wrong field
        resp = self.execute('INSERT VERTEX person(Name, age) VALUES "Tom":("Tom", 3)')
        self.check_resp_failed(resp)

        # insert vertex wrong type
        resp = self.execute('INSERT EDGE schoolmate(likeness, nickname) VALUES '
                            '"Laura"->"Amber":("87", "")')
        self.check_resp_failed(resp)

        # insert edge wrong number of value
        resp = self.execute('INSERT EDGE schoolmate(likeness, nickname) VALUES '
                            '"Laura"->"Amber":("hello", "87", "")')
        self.check_resp_failed(resp)

        # insert edge wrong num of prop
        resp = self.execute('INSERT EDGE schoolmate(likeness) VALUES '
                            '"Laura"->"Amber":("hello", "87", "")')
        self.check_resp_failed(resp)

        # insert edge wrong field name
        resp = self.execute('INSERT EDGE schoolmate(like, HH) VALUES '
                            '"Laura"->"Amber":(88)')
        self.check_resp_failed(resp)

        # insert edge invalid timestamp, TODO
        # resp = self.execute('INSERT EDGE study(start_time, end_time) '
        #                     'VALUES "Laura"->"sun_school":'
        #                     '("2300-01-01 10:00:00", now()+3600*24*365*3)')
        # self.check_resp_failed(resp)

    def test_insert_succeeded(self):
        # insert vertex succeeded
        resp = self.execute('INSERT VERTEX person(name, age) VALUES "Tom":("Tom", 22)')
        self.check_resp_succeeded(resp)

        # insert vertex unordered order prop vertex succeeded
        resp = self.execute('INSERT VERTEX person(age, name) VALUES "Conan":(10, "Conan")')
        self.check_resp_succeeded(resp)

        # check result, does not support
        # resp = self.execute('FETCH PROP ON person "Conan"')
        # self.check_resp_succeeded(resp)
        # expect_result = [['Conan', 'Conan', 10]]
        # self.check_out_of_order_result(resp, expect_result)

        # insert vertex with uuid
        # resp = self.execute('INSERT VERTEX person(name, age) VALUES uuid("Tom"):("Tom", 22)')
        # self.check_resp_succeeded(resp)

        # insert vertex with timestamp succeeded, timestamp not supported, TODO
        # resp = self.execute('INSERT VERTEX school(name, create_time) VALUES '
        #                     '"sun_school":("sun_school", "2010-01-01 10:00:00")')
        # self.check_resp_succeeded(resp)

        # insert vertex with timestamp succeeded uuid
        # resp = self.execute('INSERT VERTEX school(name, create_time) VALUES '
        #                     'uuid("sun_school"):("sun_school", "2010-01-01 10:00:00")')
        # self.check_resp_succeeded(resp)

        # succeeded
        resp = self.execute('INSERT EDGE schoolmate(likeness, nickname) VALUES '
                            '"Tom"->"Lucy":(85, "Lily")')
        self.check_resp_succeeded(resp)

        # insert edge with uuid succeeded
        # resp = self.execute('INSERT EDGE schoolmate(likeness, nickname) VALUES '
        #                     'uuid("Tom")->uuid("Lucy"):(85, "Lucy")')
        # self.check_resp_succeeded(resp)

        # insert edge with unordered order prop edge succeeded
        resp = self.execute('INSERT EDGE schoolmate(nickname, likeness) VALUES '
                            '"Tom"->"Bob":("Superman", 87)')
        self.check_resp_succeeded(resp)

        # check result, does not support
        # resp = self.execute('FETCH PROP ON schoolmate "Tom"->"Bob"')
        # self.check_resp_succeeded(resp)
        # expect_result = [['Tom', 'Bob', 0, 87, 'Superman']]
        # self.check_result(resp, expect_result)

        # insert edge with timestamp succeed
        resp = self.execute('INSERT EDGE study(start_time, end_time) '
                            'VALUES "Laura"->"sun_school":'
                            '("2019-01-01 10:00:00", now()+3600*24*365*3)')
        self.check_resp_succeeded(resp)

        # insert edge with timestamp succeed by uuid, does not support
        # resp = self.execute('INSERT EDGE study(start_time, end_time) '
        #                     'VALUES uuid("Laura")->uuid("sun_school"):'
        #                     '("2019-01-01 10:00:00", now()+3600*24*365*3)')
        # self.check_resp_succeeded(resp)

        # get result, type cast is not yet implemented in go
        # resp = self.execute('GO FROM "Laura" OVER study '
        #                     'YIELD $$.school.name, study._dst,'
        #                     '$$.school.create_time, (string)study.start_time')
        # self.check_resp_succeeded(resp)
        # expect_result = [["sun_school", "sun_school", 1262311200, "1546308000"]]
        # self.check_result(resp, expect_result)

        # get uuid result
        # resp = self.execute('GO FROM uuid("Laura") OVER study '
        #                     'YIELD $$.school.name,'
        #                     '$$.school.create_time, (string)study.start_time')
        # self.check_resp_succeeded(resp)
        # expect_result = [["sun_school", 1262311200, "1546308000"]]
        # self.check_result(resp, expect_result)

        # fetch sun_school, does not support
        # resp = self.execute('FETCH PROP ON school "sun_school"')
        # self.check_resp_succeeded(resp)
        # expect_result = [["sun_school", "sun_school", 1262311200]]
        # self.check_result(resp, expect_result)

        # fetch sun_school by uuid
        # resp = self.execute('FETCH PROP ON school uuid("sun_school")')
        # self.check_resp_succeeded(resp)
        # expect_result = [["sun_school", 1262311200]]
        # self.check_result(resp, expect_result, {0})


    def test_insert_multi_tag_succeeded(self):
        # One vertex multi tags
        resp = self.execute('INSERT VERTEX person(name, age), student(grade, number) '
                            'VALUES "Lucy":("Lucy", 8, "three", 20190901001)')
        self.check_resp_succeeded(resp)

        # One vertex multi tags use uuid, 2.0 unsupported
        # resp = self.execute('INSERT VERTEX person(name, age), student(grade, number) '
        #                     'VALUES uuid("Lucy"):("Lucy", 8, "three", 20190901001)')
        # self.check_resp_succeeded(resp)

        # One vertex multi tags with unordered order prop
        resp = self.execute('INSERT VERTEX person(age, name),student(number, grade) '
                            'VALUES "Bob":(9, "Bob", 20191106001, "four")')
        self.check_resp_succeeded(resp)

        # check result, does not support
        # resp = self.execute('FETCH PROP ON person "Bob"')
        # self.check_resp_succeeded(resp)
        # expect_result = [['Bob', 'Bob', 9]]
        # self.check_out_of_order_result(resp, expect_result)
        # resp = self.execute('FETCH PROP ON student "Bob"')
        # self.check_resp_succeeded(resp)
        # expect_result = [['Bob', 'Bob', 20191106001]]
        # self.check_out_of_order_result(resp, expect_result)

        # multi vertex multi tags
        resp = self.execute('INSERT VERTEX person(name, age),student(grade, number) '
                            'VALUES "Laura":("Laura", 8, "three", 20190901008),'
                            '"Amber":("Amber", 9, "four", 20180901003)')
        self.check_resp_succeeded(resp)

        # multi vertex multi tags with uuid
        # resp = self.execute('INSERT VERTEX person(name, age),student(grade, number) '
        #                     'VALUES uuid("Laura"):("Laura", 8, "three", 20190901008),'
        #                     'uuid("Amber"):("Amber", 9, "four", 20180901003)')
        # self.check_resp_succeeded(resp)

        # multi vertex one tags
        resp = self.execute('INSERT VERTEX person(name, age) '
                            'VALUES "Kitty":("Kitty", 8), "Peter":("Peter", 9)')
        self.check_resp_succeeded(resp)

        # multi vertex one tags with uuid
        # resp = self.execute('INSERT VERTEX person(name, age) '
        #                     'VALUES uuid("Kitty"):("Kitty", 8), uuid("Peter"):("Peter", 9)')
        # self.check_resp_succeeded(resp)

    def test_insert_multi_edges_succeeded(self):
        # multi edges
        resp = self.execute('INSERT EDGE schoolmate(likeness, nickname) VALUES '
                            '"Tom"->"Kitty":(81, "Kitty"),'
                            '"Tom"->"Peter":(83, "Kitty")')
        self.check_resp_succeeded(resp)

        # One vertex multi tags use uuid, 2.0 unsupported
        # resp = self.execute('INSERT EDGE schoolmate(likeness, nickname) VALUES '
        #                     'uuid("Tom")->uuid("Kitty"):(81, "Kitty"),'
        #                     'uuid("Tom")->uuid("Peter"):(83, "Petter")')
        # self.check_resp_succeeded(resp)

        # get result, does not support get dst vertex
        resp = self.execute_query('GO FROM "Tom" OVER schoolmate YIELD $^.person.name,'
                                  'schoolmate.likeness, $$.person.name')
        self.check_resp_succeeded(resp)
        expect_result = [['Tom', 85, 'Lucy'], ['Tom', 81, 'Kitty'], ['Tom', 83, 'Peter'], ['Tom', 87, 'Bob']]
        self.check_out_of_order_result(resp, expect_result)

        # get result use uuid
        # resp = self.execute('GO FROM uuid("Tom") OVER schoolmate YIELD $^.person.name,'
        #                     'schoolmate.likeness, $$.person.name')
        # self.check_resp_succeeded(resp)
        # expect_result = [['Tom', 85, 'Lucy'], ['Tom', 81, 'Kitty'], ['Tom', 83, 'Peter']]
        # self.check_out_of_order_result(resp, expect_result)

    def test_get_multi_tag_succeeded(self):
        resp = self.execute('INSERT EDGE schoolmate(likeness, nickname) VALUES  '
                            '"Lucy"->"Laura":(90, "Laura"),'
                            '"Lucy"->"Amber":(95, "Amber")')
        self.check_resp_succeeded(resp)

        # use uuid
        # resp = self.execute('INSERT EDGE schoolmate(likeness, nickname) VALUES  '
        #                     'uuid("Lucy")->uuid("Laura"):(90, "Laura"),'
        #                     'uuid("Lucy")->uuid("Amber"):(95, "Amber")')
        # self.check_resp_succeeded(resp)

        # get multi tag through go, does not support get dst vertex
        resp = self.execute_query('GO FROM "Lucy" OVER schoolmate YIELD '
                                  'schoolmate.likeness, $$.person.name,'
                                  '$$.student.grade, $$.student.number')
        self.check_resp_succeeded(resp)
        expect_result = [[90, 'Laura', 'three', 20190901008], [95, 'Amber', 'four', 20180901003]]
        self.check_out_of_order_result(resp, expect_result)

        # get multi tag  use uuid through go
        # resp = self.execute('GO FROM uuid("Lucy") OVER schoolmate YIELD '
        #                     'schoolmate.likeness, $$.person.name,'
        #                     '$$.student.grade, $$.student.number')
        # self.check_resp_succeeded(resp)
        # expect_result = [[90, 'Laura', 'three', 20190901008], [95, 'Amber', 'four', 20180901003]]
        # self.check_out_of_order_result(resp, expect_result)

    def test_multi_sentences_multi_tags_succeeded(self):
        resp = self.execute('INSERT VERTEX person(name, age) VALUES "Aero":("Aero", 8);'
                            'INSERT VERTEX student(grade, number) VALUES "Aero":("four", 20190901003)')
        self.check_resp_succeeded(resp)

        # use uuid
        # resp = self.execute('INSERT VERTEX person(name, age) VALUES uuid("Aero"):("Aero", 8);'
        #                     'INSERT VERTEX student(grade, number) VALUES uuid("Aero"):("four", 20190901003)')
        # self.check_resp_succeeded(resp)

        # insert edge
        resp = self.execute('INSERT EDGE schoolmate(likeness, nickname) VALUES '
                            '"Laura"->"Aero":(90, "Aero")')
        self.check_resp_succeeded(resp)

        # insert edge with uuid, does not support
        # resp = self.execute('INSERT EDGE schoolmate(likeness, nickname) VALUES '
        #                     'uuid("Laura")->uuid("Aero"):(90, "Aero")')
        # self.check_resp_succeeded(resp)

        # get result, does not support get dst vertex
        resp = self.execute_query('GO FROM "Laura" OVER schoolmate '
                                  'YIELD $$.student.number, $$.person.name')
        self.check_resp_succeeded(resp)
        expect_result = [[20190901003, 'Aero']]
        self.check_result(resp, expect_result)

        # get uuid result
        # resp = self.execute('GO FROM uuid("Laura") OVER schoolmate YIELD $$.student.number, $$.person.name')
        # self.check_resp_succeeded(resp)
        # expect_result = [[20190901003, 'Aero']]
        # self.check_result(resp, expect_result)

    def test_same_prop_name_diff_type(self):
        resp = self.execute('INSERT VERTEX person(name, age), employee(name) '
                            'VALUES "Joy":("Joy", 18, 123),'
                            '"Petter":("Petter", 19, 456)')
        self.check_resp_succeeded(resp)

        # uuid, does not support
        # resp = self.execute('INSERT VERTEX person(name, age), employee(name) '
        #                     'VALUES uuid("Joy"):("Joy", 18, 123),'
        #                     'uuid("Petter"):("Petter", 19, 456)')
        # self.check_resp_succeeded(resp)

        resp = self.execute('INSERT EDGE schoolmate(likeness, nickname) VALUES '
                            '"Joy"->"Petter":(90, "Petter")')
        self.check_resp_succeeded(resp)

        # use uuid, does not support
        # resp = self.execute('INSERT EDGE schoolmate(likeness, nickname) VALUES '
        #                     'uuid("Joy")->uuid("Petter"):(90, "Petter")')
        # self.check_resp_succeeded(resp)

        # TODO: does not support get dst vertex
        resp = self.execute_query('GO FROM "Joy" OVER schoolmate YIELD $^.person.name,'
                                  'schoolmate.likeness, $$.person.name, $$.person.age,$$.employee.name')
        self.check_resp_succeeded(resp)
        expect_result = [['Joy', 90, 'Petter', 19, 456]]
        self.check_result(resp, expect_result)

        # uuid, TODO: does not support get dst vertex
        # resp = self.execute('GO FROM uuid("Joy") OVER schoolmate YIELD $^.person.name,'
        #                     'schoolmate.likeness, $$.person.name, $$.person.age,$$.employee.name')
        # self.check_resp_succeeded(resp)
        # expect_result = [['Joy', 90, 'Petter', 19, 456]]
        # self.check_result(resp, expect_result)

    def test_same_prop_name_same_type_diff_type(self):
        resp = self.execute('INSERT VERTEX person(name, age),interest(name) '
                            'VALUES "Bob":("Bob", 19, "basketball")')
        self.check_resp_succeeded(resp)

        # uuid
        # resp = self.execute('INSERT VERTEX person(name, age),interest(name) '
        #                     'VALUES uuid("Bob"0:("Bob", 19, "basketball")')
        # self.check_resp_succeeded(resp)

        resp = self.execute('INSERT EDGE schoolmate(likeness, nickname) VALUES '
                            '"Petter"->"Bob":(90, "Bob")')
        self.check_resp_succeeded(resp)

        # use uuid
        # resp = self.execute('INSERT EDGE schoolmate(likeness, nickname) VALUES '
        #                     'uuid("Petter")->uuid("Bob"):(90, "Bob")')
        # self.check_resp_succeeded(resp)

        # get result
        resp = self.execute_query('GO FROM "Petter" OVER schoolmate '
                                  'YIELD $^.person.name, $^.employee.name, '
                                  'schoolmate.likeness, $$.person.name, '
                                  '$$.interest.name, $$.person.age')
        self.check_resp_succeeded(resp)
        expect_result = [['Petter', 456, 90, 'Bob', 'basketball', 19]]
        self.check_result(resp, expect_result)

        # uuid
        # resp = self.execute('GO FROM uuid("Petter") OVER schoolmate '
        #                     'YIELD $^.person.name, $^.employee.name, '
        #                     'schoolmate.likeness, $$.person.name, '
        #                     '$$.interest.name, $$.person.age')
        # self.check_resp_succeeded(resp)
        # expect_result = [['Petter', 456, 90, 'Bob', 'basketball', 19]]
        # self.check_result(resp, expect_result)

    def test_with_default_value(self):
        # insert vertex using default value
        resp = self.execute('INSERT VERTEX personWithDefault() VALUES "":()')
        self.check_resp_succeeded(resp)

        # insert lack of the column value
        resp = self.execute('INSERT VERTEX personWithDefault'
                            '(age, isMarried, BMI) VALUES "Tom":(18, false)')
        self.check_resp_failed(resp)

        # insert column doesn't match value count
        resp = self.execute('INSERT VERTEX studentWithDefault'
                            '(grade, number) VALUES "Tom":("one", 111, "")')
        self.check_resp_failed(resp)

        # succeeded
        resp = self.execute('INSERT VERTEX personWithDefault(name) '
                            'VALUES "Tom":("Tom")')
        self.check_resp_succeeded(resp)

        resp = self.execute('INSERT VERTEX personWithDefault(name, age) '
                            'VALUES "Tom":("Tom", 20)')
        self.check_resp_succeeded(resp)

        resp = self.execute('INSERT VERTEX personWithDefault(name, BMI) '
                            'VALUES "Tom":("Tom", 20.5)')
        self.check_resp_succeeded(resp)

        # insert vertices multi tags
        resp = self.execute('INSERT VERTEX personWithDefault(name, BMI),'
                            'studentWithDefault(number) VALUES '
                            '"Laura":("Laura", 21.5, 20190901008),'
                            '"Amber":("Amber", 22.5, 20180901003)')
        self.check_resp_succeeded(resp)

        # multi vertices one tag
        resp = self.execute('INSERT VERTEX personWithDefault(name) VALUES '
                            '"Kitty":("Kitty"), "Peter":("Peter")')
        self.check_resp_succeeded(resp)

        # insert lack of the column value
        resp = self.execute('INSERT EDGE schoolmateWithDefault(likeness) VALUES "Tom"->"Lucy":()')
        self.check_resp_failed(resp)

        # column count doesn't match value count
        resp = self.execute('INSERT EDGE schoolmateWithDefault(likeness) VALUES "Tom"->"Lucy":(60, "")')
        self.check_resp_failed(resp)

        # succeed
        resp = self.execute('INSERT EDGE schoolmateWithDefault() VALUES "Tom"->"Lucy":()')
        self.check_resp_succeeded(resp)

        # unknown filed name
        resp = self.execute('INSERT EDGE schoolmateWithDefault(likeness, redundant)'
                            ' VALUES "Tom"->"Lucy":(90, 0)')
        self.check_resp_failed(resp)

        # Insert multi edges with default value
        resp = self.execute('INSERT EDGE schoolmateWithDefault() VALUES '
                            '"Tom"->"Kitty":(), "Tom"->"Peter":(), "Lucy"->"Laura":(), "Lucy"->"Amber":()')
        self.check_resp_succeeded(resp)

        # get result
        resp = self.execute_query('GO FROM "Tom" OVER schoolmateWithDefault YIELD $^.person.name,'
                                  'schoolmateWithDefault.likeness, $$.person.name')
        self.check_resp_succeeded(resp)
        expect_result = [['Tom', 80, 'Lucy'], ['Tom', 80, 'Kitty'], ['Tom', 80, 'Peter']]
        self.check_out_of_order_result(resp, expect_result)

        # timestamp has not implement
        # resp = self.execute_query('GO FROM "Lucy" OVER schoolmateWithDefault YIELD '
        #                           'schoolmateWithDefault.likeness, $$.personWithDefault.name,'
        #                           '$$.personWithDefault.birthday, $$.personWithDefault.department,'
        #                           '$$.studentWithDefault.grade, $$.studentWithDefault.number')
        # self.check_resp_succeeded(resp)
        # expect_result = [[80, 'Laura', 1578621600, 'engineering', 'one', 20190901008],
        #                  [80, 'Amber', 1578621600, 'engineering', 'one', 20180901003]]
        # self.check_out_of_order_result(resp, expect_result)

    def test_multi_version(self):
        # insert multi version vertex
        resp = self.execute('INSERT VERTEX person(name, age) VALUES '
                            '"Tony":("Tony", 18), "Mack":("Mack", 19)')
        self.check_resp_succeeded(resp)

        resp = self.execute('INSERT VERTEX person(name, age) VALUES "Mack":("Mack", 20)')
        self.check_resp_succeeded(resp)

        resp = self.execute('INSERT VERTEX person(name, age) VALUES "Mack":("Mack", 21)')
        self.check_resp_succeeded(resp)

        # insert multi version edge
        resp = self.execute('INSERT EDGE schoolmate(likeness, nickname)'
                            ' VALUES "Tony"->"Mack"@1:(1, "")')
        self.check_resp_succeeded(resp)

        resp = self.execute('INSERT EDGE schoolmate(likeness, nickname)'
                            ' VALUES "Tony"->"Mack"@1:(2, "")')
        self.check_resp_succeeded(resp)

        resp = self.execute('INSERT EDGE schoolmate(likeness, nickname)'
                            ' VALUES "Tony"->"Mack"@1:(3, "")')
        self.check_resp_succeeded(resp)

        # get result
        resp = self.execute_query('GO FROM "Tony" OVER schoolmate '
                                  'YIELD $$.person.name, $$.person.age, schoolmate.likeness')
        self.check_resp_succeeded(resp)
        expect_result = [['Mack', 21, 3]]
        self.check_result(resp, expect_result)

    @pytest.mark.skip(reason="does not support uuid")
    def test_multi_version_with_uuid(self):
        # insert multi version vertex
        resp = self.execute('INSERT VERTEX person(name, age) VALUES '
                            'uuid("Tony"):("Tony", 18), "Mack":("Mack", 19)')
        self.check_resp_succeeded(resp)

        resp = self.execute('INSERT VERTEX person(name, age) VALUES uuid("Mack"):("Mack", 20)')
        self.check_resp_succeeded(resp)

        resp = self.execute('INSERT VERTEX person(name, age) VALUES uuid("Mack"):("Mack", 21)')
        self.check_resp_succeeded(resp)

        # insert multi version edge
        resp = self.execute('INSERT EDGE schoolmate(likeness, nickname)'
                            ' VALUES uuid("Tony")->uuid("Mack")@1:(1, "")')
        self.check_resp_succeeded(resp)

        resp = self.execute('INSERT EDGE schoolmate(likeness, nickname)'
                            ' VALUES uuid("Tony")->uuid("Mack")@1:(2, "")')
        self.check_resp_succeeded(resp)

        resp = self.execute('INSERT EDGE schoolmate(likeness, nickname)'
                            ' VALUES uuid("Tony")->uuid("Mack")@1:(3, "")')
        self.check_resp_succeeded(resp)

        # get result
        resp = self.execute_query('GO FROM uuid("Tony") OVER schoolmate '
                                  'YIELD $$.person.name, $$.person.age, schoolmate.likeness')
        self.check_resp_succeeded(resp)
        expect_result = [['Mack', 21, 3]]
        self.check_result(resp, expect_result)

    def unsupported_sentence(self):
        resp = self.execute('LOOKUP ON person where person.name == "Tony"'
                            ' YIELD person.name, person.age')
        self.check_resp_failed(resp)

        resp = self.execute('MATCH')
        self.check_resp_failed(resp)
