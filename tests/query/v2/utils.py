# --coding:utf-8--
#
# Copyright (c) 2019 vesoft inc. All rights reserved.
#
# This source code is licensed under Apache 2.0 License,
# attached with Common Clause Condition 1.0, found in the LICENSES directory.

import functools

from nebula2.common import ttypes as CommonTtypes


def compare_vertex(vertex1, vertex2):
    assert isinstance(vertex1, CommonTtypes.Vertex) and isinstance(vertex2, CommonTtypes.Vertex)
    if vertex1.vid != vertex2.vid:
        if vertex1.vid < vertex2.vid:
            return -1
        return 1
    if len(vertex1.tags) != len(vertex2.tags):
        return len(vertex1.tags) - len(vertex2.tags)
    return 0


def compare_edge(edge1, edge2):
    assert isinstance(edge1, CommonTtypes.Edge) and isinstance(edge2, CommonTtypes.Edge)
    if edge1.src != edge2.src:
        if edge1.src < edge2.src:
            return -1
        return 1
    if edge1.dst != edge2.dst:
        if edge1.dst < edge2.dst:
            return -1
        return 1
    if edge1.type != edge2.type:
        return edge1.type - edge2.type
    if edge1.ranking != edge2.ranking:
        return edge1.ranking - edge2.ranking
    if len(edge1.props) != len(edge2.props):
        return len(edge1.props) - len(edge2.props)
    return 0


def sort_vertex_list(rows):
    assert len(rows) == 1, 'rows: {}'.format(rows)
    if isinstance(rows[0], CommonTtypes.Row):
        vertex_list = list(map(lambda v: v.get_vVal(), rows[0].values[0].get_lVal().values))
        sort_vertex_list = sorted(vertex_list, key=functools.cmp_to_key(compare_vertex))
    elif isinstance(rows[0], list):
        vertex_list = list(map(lambda v: v.get_vVal(), rows[0][0]))
        sort_vertex_list = sorted(vertex_list, key=functools.cmp_to_key(compare_vertex))
    else:
        assert False
    return sort_vertex_list


def sort_vertex_edge_list(rows):
    new_rows = list()
    for row in rows:
        new_row = list()
        if isinstance(row, CommonTtypes.Row):
            vertex_list = row.values[0].get_lVal().values
            new_vertex_list = list(map(lambda v: v.get_vVal(), vertex_list))
            new_row.extend(sorted(new_vertex_list, key=functools.cmp_to_key(compare_vertex)))

            edge_list = row.values[1].get_lVal().values
            new_edge_list = list(map(lambda e: e.get_eVal(), edge_list))
            new_row.extend(sorted(new_edge_list, key=functools.cmp_to_key(compare_edge)))
        elif isinstance(row, list):
            vertex_list = list(map(lambda v: v.get_vVal(), row[0]))
            sort_vertex_list = sorted(vertex_list, key=functools.cmp_to_key(compare_vertex))
            new_row.extend(sort_vertex_list)

            edge_list = list(map(lambda e: e.get_eVal(), row[1]))
            sort_edge_list = sorted(edge_list, key=functools.cmp_to_key(compare_edge))
            new_row.extend(sort_edge_list)
        else:
            assert False, "Unsupport type : {}".format(type(row))

        new_rows.append(new_row)
    return new_rows


def check_subgraph(resp, expect):
    if resp.is_empty() and len(expect) == 0:
        return True

    assert not resp.is_empty(), 'resp.data is None'
    rows = resp.rows()

    msg = 'len(rows)[%d] != len(expect)[%d]' % (len(rows), len(expect))
    assert len(rows) == len(expect), msg

    if resp.col_size() == 1:
        new_rows = sort_vertex_list(rows)
        new_expect = sort_vertex_list(expect)
    else:
        new_rows = sort_vertex_edge_list(rows)
        new_expect = sort_vertex_edge_list(expect)

    for exp in new_expect:
        find = False
        for row in new_rows:
            if row == exp:
                find = True
                new_rows.remove(row)
                break
        assert find, 'Can not find {}'.format(exp)

    assert len(new_rows) == 0
