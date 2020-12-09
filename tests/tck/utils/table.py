# Copyright (c) 2020 vesoft inc. All rights reserved.
#
# This source code is licensed under Apache 2.0 License,
# attached with Common Clause Condition 1.0, found in the LICENSES directory.

from tests.tck.utils.nbv import parse
from nebula2.common.ttypes import DataSet, Row


def table(text):
    lines = text.splitlines()
    assert len(lines) >= 1

    def parse_line(line):
        return list(
            map(lambda x: x.strip(), filter(lambda x: x, line.split('|'))))

    table = []
    column_names = list(map(lambda x: bytes(x, 'utf-8'), parse_line(lines[0])))
    for line in lines[1:]:
        row = {}
        cells = parse_line(line)
        for i, cell in enumerate(cells):
            row[column_names[i]] = cell
        table.append(row)

    return {
        "column_names": column_names,
        "rows": table,
    }


def dataset(string_table):
    ds = DataSet()
    ds.column_names = string_table['column_names']
    ds.rows = []
    for row in string_table['rows']:
        nrow = Row()
        nrow.values = []
        for column in ds.column_names:
            value = parse(row[column])
            assert value is not None, \
                    f'parse error: column is {column}:{row[column]}'
            nrow.values.append(value)
        ds.rows.append(nrow)
    return ds
