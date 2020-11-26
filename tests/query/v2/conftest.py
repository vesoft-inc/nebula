# --coding:utf-8--
#
# Copyright (c) 2020 vesoft inc. All rights reserved.
#
# This source code is licensed under Apache 2.0 License,
# attached with Common Clause Condition 1.0, found in the LICENSES directory.

import re
import pytest

from nebula2.common import ttypes
from tests.common.utils import utf8b, utf8s

from tests.query.v2.utils import check_subgraph


def edgekey(edge):
    return utf8s(edge.src) + utf8s(edge.dst) + utf8s(edge.name) \
        + str(edge.ranking)


def create_vertex_team(line):
    assert len(line) == 2
    vertex = ttypes.Vertex()
    vertex.vid = utf8b(line[0])
    tags = []
    tag = ttypes.Tag()
    tag.name = utf8b('team')

    props = dict()
    name = ttypes.Value()
    name.set_sVal(utf8b(line[1]))
    props[utf8b('name')] = name
    tag.props = props
    tags.append(tag)
    vertex.tags = tags
    return vertex


def create_vertex_player(line):
    assert len(line) == 3
    vertex = ttypes.Vertex()
    vertex.vid = utf8b(line[0])
    tags = []
    tag = ttypes.Tag()
    tag.name = utf8b('player')

    props = dict()
    name = ttypes.Value()
    name.set_sVal(utf8b(line[1]))
    props[utf8b('name')] = name
    age = ttypes.Value()
    age.set_iVal(int(line[2]))
    props[utf8b('age')] = age
    tag.props = props
    tags.append(tag)
    vertex.tags = tags
    return vertex


def create_vertex_bachelor(line):
    assert len(line) == 3
    vertex = ttypes.Vertex()
    vertex.vid = utf8b(line[0])
    tags = []
    tag = ttypes.Tag()
    tag.name = utf8b('bachelor')

    props = dict()
    name = ttypes.Value()
    name.set_sVal(utf8b(line[1]))
    props[utf8b('name')] = name
    speciality = ttypes.Value()
    speciality.set_sVal(utf8b(line[2]))
    props[utf8b('speciality')] = speciality
    tag.props = props
    tags.append(tag)
    vertex.tags = tags
    return vertex


def create_edge_serve(line):
    assert len(line) == 4
    edge = ttypes.Edge()
    edge.src = utf8b(line[0])
    if '@' in line[1]:
        temp = list(map(lambda i: i.strip('"'), re.split('@', line[1])))
        edge.dst = utf8b(temp[0])
        edge.ranking = int(temp[1])
    else:
        edge.dst = utf8b(line[1])
        edge.ranking = 0
    edge.type = 1
    edge.name = utf8b('serve')
    props = dict()
    start_year = ttypes.Value()
    start_year.set_iVal(int(line[2]))
    end_year = ttypes.Value()
    end_year.set_iVal(int(line[3]))
    props[utf8b('start_year')] = start_year
    props[utf8b('end_year')] = end_year
    edge.props = props
    return edge


def create_edge_like(line):
    assert len(line) == 3
    edge = ttypes.Edge()

    edge.src = utf8b(line[0])
    edge.dst = utf8b(line[1])
    edge.type = 1
    edge.ranking = 0
    edge.name = utf8b('like')
    props = dict()
    likeness = ttypes.Value()
    likeness.set_iVal(int(line[2]))
    props[utf8b('likeness')] = likeness
    edge.props = props
    return edge


def create_edge_teammate(line):
    assert len(line) == 4
    edge = ttypes.Edge()
    edge.src = utf8b(line[0])
    edge.dst = utf8b(line[1])
    edge.type = 1
    edge.ranking = 0
    edge.name = utf8b('teammate')
    props = dict()
    start_year = ttypes.Value()
    start_year.set_iVal(int(line[2]))
    end_year = ttypes.Value()
    end_year.set_iVal(int(line[3]))
    props[utf8b('start_year')] = start_year
    props[utf8b('end_year')] = end_year
    edge.props = props
    return edge


def get_datatype(line):
    if line.startswith('VERTEX player'):
        return 'player'
    elif line.startswith('VERTEX team'):
        return 'team'
    elif line.startswith('VERTEX bachelor'):
        return 'bachelor'
    elif line.startswith('EDGE serve'):
        return 'serve'
    elif line.startswith('EDGE like'):
        return 'like'
    elif line.startswith('EDGE teammate'):
        return 'teammate'
    return None


def fill_ve(line, datatype: str, VERTEXS, EDGES):
    line = re.split(':|,|->', line.strip(',; \t'))
    line = list(map(lambda i: i.strip(' ()"'), line))
    value = ttypes.Value()
    assert datatype != 'none'
    if datatype == 'player':
        vertex = create_vertex_player(line)
        key = utf8s(vertex.vid)
        if key in VERTEXS:
            temp = VERTEXS[key].get_vVal()
            temp.tags.append(vertex.tags[0])
            temp.tags.sort(key=lambda x: x.name)
            value.set_vVal(temp)
            VERTEXS[key] = value
        else:
            value.set_vVal(vertex)
            VERTEXS[key] = value
    elif datatype == 'team':
        vertex = create_vertex_team(line)
        value.set_vVal(vertex)
        key = utf8s(vertex.vid)
        VERTEXS[key] = value
    elif datatype == 'bachelor':
        vertex = create_vertex_bachelor(line)
        key = utf8s(vertex.vid)
        if key in VERTEXS:
            temp = VERTEXS[key].get_vVal()
            temp.tags.append(vertex.tags[0])
            temp.tags.sort(key=lambda x: x.name)
            value.set_vVal(temp)
            VERTEXS[key] = value
        else:
            value.set_vVal(vertex)
            VERTEXS[key] = value
    elif datatype == 'serve':
        edge = create_edge_serve(line)
        value.set_eVal(edge)
        key = edgekey(edge)
        EDGES[key] = value
    elif datatype == 'like':
        edge = create_edge_like(line)
        value.set_eVal(edge)
        key = edgekey(edge)
        EDGES[key] = value
    elif datatype == 'teammate':
        edge = create_edge_teammate(line)
        value.set_eVal(edge)
        key = edgekey(edge)
        EDGES[key] = value
    else:
        raise ValueError('datatype is {}'.format(datatype))


def parse_line(line, dataType, VERTEXS, EDGES):
    if line.startswith('INSERT') or line.startswith('VALUES'):
        return ''

    dt = get_datatype(line)
    if dt is not None:
        dataType[0] = dt
    else:
        fill_ve(line, dataType[0], VERTEXS, EDGES)


@pytest.fixture(scope="class")
def set_vertices_and_edges(request):
    VERTEXS = {}
    EDGES = {}
    nba_file = request.cls.data_dir + '/data/nba.ngql'
    print("load will open ", nba_file)
    with open(nba_file, 'r') as data_file:
        lines = data_file.readlines()
        ddl = False
        dataType = ['none']
        for line in lines:
            strip_line = line.strip()
            if len(strip_line) == 0:
                continue
            elif strip_line.startswith('--'):
                comment = strip_line[2:]
                if comment == 'DDL':
                    ddl = True
                elif comment == 'END':
                    if ddl:
                        ddl = False
            else:
                if not ddl:
                    parse_line(line.strip(), dataType, VERTEXS, EDGES)
                if line.endswith(';'):
                    dataType[0] = 'none'

    request.cls.VERTEXS = VERTEXS
    request.cls.EDGES = EDGES


@pytest.fixture
def check_subgraph_result():
    return check_subgraph
