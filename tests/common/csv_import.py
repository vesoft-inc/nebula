# Copyright (c) 2020 vesoft inc. All rights reserved.
#
# This source code is licensed under Apache 2.0 License,
# attached with Common Clause Condition 1.0, found in the LICENSES directory.

import csv
import re

from tests.common.types import (
    VID,
    Rank,
    Prop,
    Tag,
    Edge,
    Vertex,
)


class CSVImporter:
    _SRC_VID = ':SRC_VID'
    _DST_VID = ':DST_VID'
    _VID = ':VID'
    _RANK = ':RANK'

    def __init__(self, filepath):
        self._filepath = filepath
        self._insert_stmt = ""
        self._create_stmt = ""
        self._type = None

    def __iter__(self):
        with open(self._filepath, 'r') as f:
            for i, row in enumerate(csv.reader(f)):
                if i == 0:
                    yield self.parse_header(row)
                else:
                    yield self.process(row)

    def process(self, row: list):
        if isinstance(self._type, Vertex):
            return self.build_vertex_insert_stmt(row)
        return self.build_edge_insert_stmt(row)

    def build_vertex_insert_stmt(self, row: list):
        props = []
        for p in self._type.tags[0].props:
            col = row[p.index]
            props.append(self.value(p.ptype, col))
        vid = self._type.vid
        id_val = self.value(vid.id_type, row[vid.index])
        return f'{self._insert_stmt} {id_val}:({",".join(props)});'

    def build_edge_insert_stmt(self, row: list):
        props = []
        for p in self._type.props:
            col = row[p.index]
            props.append(self.value(p.ptype, col))
        src = self._type.src
        dst = self._type.dst
        src_vid = self.value(src.id_type, row[src.index])
        dst_vid = self.value(dst.id_type, row[dst.index])
        if self._type.rank is None:
            return f'{self._insert_stmt} {src_vid}->{dst_vid}:({",".join(props)});'
        rank = row[self._type.rank.index]
        return f'{self._insert_stmt} {src_vid}->{dst_vid}@{rank}:({",".join(props)});'

    def value(self, ptype: str, col):
        return f'"{col}"' if ptype == 'string' else f'{col}'

    def parse_header(self, row):
        """
        Only parse the scenario that one tag in each file
        """
        for col in row:
            if self._SRC_VID in col or self._DST_VID in col:
                self._type = Edge()
                self.parse_edge(row)
                break
            if self._VID in col:
                self._type = Vertex()
                self.parse_vertex(row)
                break
        if self._type is None:
            raise ValueError(f'Invalid csv header: {",".join(row)}')
        return self._create_stmt

    def parse_edge(self, row):
        props = []
        name = ''
        for i, col in enumerate(row):
            if col == self._RANK:
                self._type.rank = Rank(i)
                continue
            m = re.search(r':SRC_VID\((.*)\)', col)
            if m:
                self._type.src = VID(i, m.group(1))
                continue
            m = re.search(r':DST_VID\((.*)\)', col)
            if m:
                self._type.dst = VID(i, m.group(1))
                continue
            m = re.search(r'(\w+)\.(\w+):(\w+)', col)
            if not m:
                raise ValueError(f'Invalid csv header format {col}')
            g1 = m.group(1)
            if not name:
                name = g1
            assert name == g1, f'Different edge type {g1}'
            props.append(Prop(i, m.group(2), m.group(3)))

        self._type.name = name
        self._type.props = props
        pdecl = ','.join(p.name for p in props)
        self._insert_stmt = f"INSERT EDGE {name}({pdecl}) VALUES"
        pdecl = ','.join(f"`{p.name}` {p.ptype}" for p in props)
        self._create_stmt = f"CREATE EDGE IF NOT EXISTS `{name}`({pdecl});"

    def parse_vertex(self, row):
        tag = Tag()
        props = []
        for i, col in enumerate(row):
            m = re.search(r':VID\((.*)\)', col)
            if m:
                self._type.vid = VID(i, m.group(1))
                continue
            m = re.search(r'(\w+)\.(\w+):(\w+)', col)
            if not m:
                raise ValueError(f'Invalid csv header format {col}')
            g1 = m.group(1)
            if not tag.name:
                tag.name = g1
            assert tag.name == g1, f'Different tag name {g1}'
            props.append(Prop(i, m.group(2), m.group(3)))

        tag.props = props
        self._type.tags = [tag]
        pdecl = ','.join(p.name for p in tag.props)
        self._insert_stmt = f"INSERT VERTEX {tag.name}({pdecl}) VALUES"
        pdecl = ','.join(f"`{p.name}` {p.ptype}" for p in tag.props)
        self._create_stmt = f"CREATE TAG IF NOT EXISTS `{tag.name}`({pdecl});"


if __name__ == '__main__':
    for row in CSVImporter('../data/nba/player.csv'):
        print(row)
