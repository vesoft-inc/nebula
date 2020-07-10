# --coding:utf-8--
#
# Copyright (c) 2019 vesoft inc. All rights reserved.
#
# This source code is licensed under Apache 2.0 License,
# attached with Common Clause Condition 1.0, found in the LICENSES directory.
import random
import string


def insert_vertexs(client, ns, batchCount, batchSize):
    resp = client.execute('USE ' + ns)
    client.check_resp_succeeded(resp)
    for i in range(batchCount):
        query = generate_insert_student_vertex(batchSize, batchSize * i)
        resp = client.execute(query)
        client.check_resp_succeeded(resp)


def insert_edges(client, ns, batchCount, batchSize):
    resp = client.execute('USE ' + ns)
    client.check_resp_succeeded(resp)
    for i in range(batchCount):
        query = generate_insert_likeness_edge(batchSize, batchSize * i)
        resp = client.execute(query)
        client.check_resp_succeeded(resp)


def randomString(stringLength):
    letters = string.ascii_lowercase
    return ''.join(random.choice(letters) for i in range(stringLength))


def random_students(size=100):
    students = []
    for i in range(size):
        name = randomString(10)
        age = random.randint(1, 100)
        students.append((name, age))
    return students


def generate_insert_student_vertex(size, idOffset):
    students = random_students(size)
    length = len(students)
    query = "INSERT VERTEX person (name, age) VALUES "
    for i in range(length):
        student = students[i]
        query += '{0}:("{1}",{2})'.format(i + idOffset, student[0], student[1])
        if i < length - 1:
            query += ", "
    return query


def generate_insert_likeness_edge(length, idOffset):
    query = "INSERT EDGE like (likeness) VALUES "
    for i in range(length):
        firstId = random.randint(idOffset, length + idOffset)
        secondId = random.randint(idOffset, length + idOffset)
        query += '{0}->{1}:({2})'.format(firstId, secondId, random.randint(100, 1000000))
        if i < length - 1:
            query += ", "

    return query
