# Copyright (c) 2020 vesoft inc. All rights reserved.
#
# This source code is licensed under Apache 2.0 License,
# attached with Common Clause Condition 1.0, found in the LICENSES directory.

import os
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

    def __init__(self, file_desc, data_dir):
        self._filepath = os.path.join(data_dir, file_desc['path'])
        self._insert_stmt = ""
        self._create_stmt = ""
        self._file_desc = file_desc
        self._type = None
        self._gen_header(file_desc)

    def __iter__(self):
        with open(self._filepath, 'r') as f:
            with_header = self._file_desc.get("withHeader", False)
            for i, row in enumerate(csv.reader(f)):
                if with_header and i == 0:
                    header = self._header if len(self._header) > 0 else row
                    yield self.parse_header(header)
                else:
                    yield self.process(row)

    def _gen_header(self, file_desc):
        header = {}
        if file_desc["type"] == "vertex":
            vertex = file_desc["vertex"]
            vid = vertex["vid"]
            header[self._key(vid)] = self._vid(vid, self._VID)
            tags = vertex["tags"]
            for tag in tags:
                tagname = tag['name']
                for prop in tag['props']:
                    header[self._key(prop)] = self._prop(prop, tagname)
        elif file_desc["type"] == "edge":
            edge = file_desc["edge"]
            name = edge['name']
            src = edge['srcVID']
            header[self._key(src)] = self._vid(src, self._SRC_VID)
            dst = edge['dstVID']
            header[self._key(dst)] = self._vid(dst, self._DST_VID)
            if "rank" in edge:
                header[self._key(edge['rank'])] = self._RANK
            for prop in edge['props']:
                header[self._key(prop)] = self._prop(prop, name)
        else:
            raise ValueError("Invalid config file")
        self._header = [header[str(i)] for i in range(len(header))]

    @staticmethod
    def _vid(vid, name: str):
        vtype = vid['type']
        if 'function' not in vid:
            return f"{name}({vtype})"
        return f"{name}({vtype},{vid['function']})"

    @staticmethod
    def _key(v):
        return str(v['index'])

    @staticmethod
    def _prop(prop, prefix):
        return f"{prefix}.{prop['name']}:{prop['type']}"

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
        id_val = self.vid_str(vid, row[vid.index])
        return f'{self._insert_stmt} {id_val}:({",".join(props)});'

    def build_edge_insert_stmt(self, row: list):
        props = []
        for p in self._type.props:
            col = row[p.index]
            props.append(self.value(p.ptype, col))
        src = self._type.src
        dst = self._type.dst
        src_vid = self.vid_str(src, row[src.index])
        dst_vid = self.vid_str(dst, row[dst.index])
        if self._type.rank is None:
            return f'{self._insert_stmt} {src_vid}->{dst_vid}:({",".join(props)});'
        rank = row[self._type.rank.index]
        return f'{self._insert_stmt} {src_vid}->{dst_vid}@{rank}:({",".join(props)});'

    def value(self, ptype: str, col):
        if type(col) == str and col.lower() in ["__null__", "null"]:
            return "NULL"
        return f'"{col}"' if ptype == 'string' else f'{col}'

    def vid_str(self, vid: VID, col: str):
        is_str = (vid.id_type == 'string')
        if vid.function is None:
            return f'"{col}"' if is_str else f'{col}'
        if is_str:
            return f'{vid.function}("{col}")'
        return f'{vid.function}({col})'

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
            b, src_vid = self._search_vid(i, "SRC_VID", col)
            if b:
                self._type.src = src_vid
                continue
            b, dst_vid = self._search_vid(i, "DST_VID", col)
            if b:
                self._type.dst = dst_vid
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
            b, vid = self._search_vid(i, "VID", col)
            if b:
                self._type.vid = vid
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

    def _search_vid(self, i: int, prefix: str, value: str):
        m = re.search(r":{}\((int|string)\)".format(prefix), value)
        if m:
            return True, VID(i, m.group(1))
        m = re.search(r":{}\((int|string),\s*(hash|uuid)\)".format(prefix),
                      value)
        if m:
            return True, VID(i, m.group(1), m.group(2))
        return False, None
