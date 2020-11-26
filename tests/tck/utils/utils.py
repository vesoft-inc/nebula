# Copyright (c) 2020 vesoft inc. All rights reserved.
#
# This source code is licensed under Apache 2.0 License,
# attached with Common Clause Condition 1.0, found in the LICENSES directory.
#

from nebula2.common.ttypes import Value,Vertex,Edge,Path,Step,NullType,DataSet,Row,List
import nbv
from behave import model as bh

def parse(input):
    if isinstance(input, bh.Table):
        return parse_table(input)
    elif isinstance(input, bh.Row):
        return parse_row(input)
    else:
        raise Exception('Unable to parse %s' % str(input))

def parse_table(table):
    names = table.headings
    rows = []
    for row in table:
        rows.append(parse_row(row))
    return DataSet(column_names = table.headings, rows = rows)

def parse_row(row):
    list = []
    for cell in row:
        v = nbv.parse(cell)
        if v is None:
            raise Exception('Unable to parse %s' % cell)
        list.append(v)
    return Row(list)

if __name__ == '__main__':
    headings = ['m', 'r', 'n']
    rows = [
        [
            '1', '2', '3'
        ],
        [
            '()', '-->', '()'
        ],
        [
            '("vid")', '<-[:e "1" -> "2" @-1 {p1: 0, p2: [1, 2, 3]}]-', '()'
        ],
        [
            '<()-->()<--()>', '()', '"prop"'
        ],
        [
            'EMPTY', 'NULL', 'BAD_TYPE'
        ],
    ]
    expected = DataSet(column_names = headings,\
            rows = [
                Row([Value(iVal=1), Value(iVal=2), Value(iVal=3)]),
                Row([Value(vVal=Vertex()), Value(eVal=Edge(type=1)), Value(vVal=Vertex())]),
                Row([Value(vVal=Vertex('vid')), Value(eVal=Edge(name='e',type=-1,src='1',dst='2',ranking=-1,props={'p1': Value(iVal=0), 'p2': Value(lVal=List([Value(iVal=1),Value(iVal=2),Value(iVal=3)]))})), Value(vVal=Vertex())]),
                Row([Value(pVal=Path(src=Vertex(),steps=[Step(type=1,dst=Vertex()),Step(type=-1,dst=Vertex())])), Value(vVal=Vertex()), Value(sVal='prop')]),
                Row([Value(), Value(nVal=NullType.__NULL__), Value(nVal=NullType.BAD_TYPE)])
            ])

    table = bh.Table(headings = headings, rows = rows)
    dataset = parse(table)
    assert dataset == expected,\
                    "Parsed DataSet doesn't match, \nexpected: %s, \nactual: %s" % (expected, dataset)
