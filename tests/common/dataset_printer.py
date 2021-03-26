# Copyright (c) 2020 vesoft inc. All rights reserved.
#
# This source code is licensed under Apache 2.0 License,
# attached with Common Clause Condition 1.0, found in the LICENSES directory.

import re

from typing import List, Union

from nebula2.common.ttypes import DataSet, Edge, NullType, Path, Value, Vertex

Pattern = type(re.compile(r'\d+'))


class DataSetPrinter:
    def __init__(self, decode_type='utf-8', vid_fn=None):
        self._decode_type = decode_type
        self._vid_fn = vid_fn

    def sstr(self, b) -> str:
        if not type(b) == bytes:
            return b
        return b.decode(self._decode_type)

    def vid(self, v) -> str:
        if type(v) is str:
            return f'"{v}"' if self._vid_fn is None else f"{self._vid_fn(v)}"
        if type(v) is bytes:
            if self._vid_fn is None:
                return f'"{self.sstr(v)}"'
            return f"{self._vid_fn(v)}"
        if type(v) is int:
            return f'{v}'
        if isinstance(v, Value):
            return self.vid(self.to_string(v))
        return str(v)

    def ds_to_string(self, ds: DataSet) -> str:
        col_names = '|' + '|'.join(self.sstr(col)
                                   for col in ds.column_names) + '|'
        data_rows = '\n'.join(
            f'{i}: |' + self.list_to_string(row.values, delimiter='|') + '|'
            for (i, row) in enumerate(ds.rows))
        return '\n'.join([col_names, data_rows])

    def to_string(self, val: Union[Value, Pattern]):
        if isinstance(val, Pattern):
            return str(val)
        return self.value_to_string(val)

    def value_to_string(self, val: Value) -> str:
        if val.getType() == Value.NVAL:
            return NullType._VALUES_TO_NAMES[val.get_nVal()]
        if val.getType() == Value.__EMPTY__:
            return "EMPTY"
        if val.getType() == Value.BVAL:
            return "true" if val.get_bVal() else "false"
        if val.getType() == Value.IVAL:
            return str(val.get_iVal())
        if val.getType() == Value.FVAL:
            return str(val.get_fVal())
        if val.getType() == Value.SVAL:
            return self.sstr(val.get_sVal())
        if val.getType() == Value.DVAL:
            ld = val.get_dVal()
            return "%d-%02d-%02d" % (ld.year, ld.month, ld.day)
        if val.getType() == Value.TVAL:
            lt = val.get_tVal()
            return "%02d:%02d:%02d.%06d" % (lt.hour, lt.minute, lt.sec,
                                            lt.microsec)
        if val.getType() == Value.DTVAL:
            ldt = val.get_dtVal()
            return "%d-%02d-%02dT%02d:%02d:%02d.%06d" % (
                ldt.year, ldt.month, ldt.day, ldt.hour, ldt.minute, ldt.sec,
                ldt.microsec)
        if val.getType() == Value.VVAL:
            return self.vertex_to_string(val.get_vVal())
        if val.getType() == Value.EVAL:
            return self.edge_to_string(val.get_eVal())
        if val.getType() == Value.PVAL:
            return self.path_to_string(val.get_pVal())
        if val.getType() == Value.LVAL:
            return '[' + self.list_to_string(val.get_lVal().values) + ']'
        if val.getType() == Value.MVAL:
            return self.map_to_string(val.get_mVal().kvs)
        if val.getType() == Value.UVAL:
            return '{' + self.list_to_string(val.get_uVal().values) + '}'
        if val.getType() == Value.GVAL:
            return self.ds_to_string(val.get_gVal())
        return ""

    def list_to_string(self, lst: List[Value], delimiter: str = ",") -> str:
        return delimiter.join(self.to_string(val) for val in lst)

    def vertex_to_string(self, v: Vertex):
        if v.vid is None:
            return "()"
        if v.tags is None:
            return f'({self.vid(v.vid)})'
        tags = []
        for tag in v.tags:
            name = self.sstr(tag.name)
            tags.append(f":{name}{self.map_to_string(tag.props)}")
        return f'({self.vid(v.vid)}{"".join(tags)})'

    def map_to_string(self, m: dict) -> str:
        if m is None:
            return ""
        kvs = [f"{self.sstr(k)}:{self.to_string(v)}" for k, v in m.items()]
        return '{' + ','.join(sorted(kvs)) + '}'

    def edge_to_string(self, e: Edge) -> str:
        name = "" if e.name is None else ":" + self.sstr(e.name)
        arrow = "->" if e.type is None or e.type > 0 else "<-"
        direct = f'{self.vid(e.src)}{arrow}{self.vid(e.dst)}'
        rank = "" if e.ranking is None else f"@{e.ranking}"
        return f"[{name} {direct}{rank}{self.map_to_string(e.props)}]"

    def path_to_string(self, p: Path) -> str:
        src = self.vertex_to_string(p.src)
        path = []
        for step in p.steps:
            name = "" if step.name is None else ":" + self.sstr(step.name)
            rank = "" if step.ranking is None else f"@{step.ranking}"
            dst = self.vertex_to_string(step.dst)
            props = self.map_to_string(step.props)
            if step.type is None or step.type > 0:
                path.append(f"-[{name}{rank}{props}]->{dst}")
            else:
                path.append(f"<-[{name}{rank}{props}]-{dst}")
        return f"<{src}{''.join(path)}>"
