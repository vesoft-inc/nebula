# Copyright (c) 2020 vesoft inc. All rights reserved.
#
# This source code is licensed under Apache 2.0 License,
# attached with Common Clause Condition 1.0, found in the LICENSES directory.

import math
import re

from enum import Enum
from typing import Union, Dict, List
from nebula2.common.ttypes import (
    DataSet,
    Edge,
    Path,
    Row,
    Value,
    Vertex,
)
from tests.common.dataset_printer import DataSetPrinter

KV = Dict[Union[str, bytes], Value]
Pattern = type(re.compile(r'/'))

CmpType = Enum('CmpType', ('EQUAL', 'CONTAINS', 'NOT_CONTAINS'))


class DataSetComparator:
    def __init__(self,
                 strict=True,
                 order=False,
                 contains=CmpType.EQUAL,
                 decode_type='utf-8',
                 vid_fn=None):
        self._strict = strict
        self._order = order
        self._contains = contains
        self._decode_type = decode_type
        self._vid_fn = vid_fn

    def __call__(self, resp: DataSet, expect: DataSet):
        return self.compare(resp, expect)

    def b(self, v: str) -> bytes:
        return v.encode(self._decode_type)

    def s(self, b: bytes) -> str:
        return b.decode(self._decode_type)

    def _whether_return(self, cmp: bool) -> bool:
        return ((self._contains == CmpType.EQUAL and not cmp)
                or (self._contains == CmpType.NOT_CONTAINS and cmp))

    def compare(self, resp: DataSet, expect: DataSet):
        if self._contains == CmpType.NOT_CONTAINS and len(resp.rows) == 0:
            return True, None
        if all(x is None for x in [expect, resp]):
            return True, None
        if None in [expect, resp]:
            return False, -1
        if len(resp.rows) < len(expect.rows) and self._contains == CmpType.EQUAL:
            return False, -1
        if len(resp.column_names) != len(expect.column_names):
            return False, -1
        for (ln, rn) in zip(resp.column_names, expect.column_names):
            if ln != self.bstr(rn):
                return False, -2
        if self._order:
            for i in range(0, len(expect.rows)):
                cmp = self.compare_row(resp.rows[i], expect.rows[i])
                if self._whether_return(cmp):
                    return False, i
            if self._contains == CmpType.CONTAINS:
                return True, None
            return len(resp.rows) == len(expect.rows), -1
        return self._compare_list(resp.rows, expect.rows, self.compare_row,
                                  self._contains)

    def compare_value(self, lhs: Value, rhs: Union[Value, Pattern]) -> bool:
        """
        lhs and rhs represent response data and expected data respectively
        """
        if type(rhs) is Pattern:
            dsp = DataSetPrinter(self._decode_type)
            return bool(rhs.match(dsp.to_string(lhs)))
        if lhs.getType() == Value.__EMPTY__:
            return rhs.getType() == Value.__EMPTY__
        if lhs.getType() == Value.NVAL:
            if not rhs.getType() == Value.NVAL:
                return False
            return lhs.get_nVal() == rhs.get_nVal()
        if lhs.getType() == Value.BVAL:
            if not rhs.getType() == Value.BVAL:
                return False
            return lhs.get_bVal() == rhs.get_bVal()
        if lhs.getType() == Value.IVAL:
            if not rhs.getType() == Value.IVAL:
                return False
            return lhs.get_iVal() == rhs.get_iVal()
        if lhs.getType() == Value.FVAL:
            if not rhs.getType() == Value.FVAL:
                return False
            return math.fabs(lhs.get_fVal() - rhs.get_fVal()) < 1.0E-8
        if lhs.getType() == Value.SVAL:
            if not rhs.getType() == Value.SVAL:
                return False
            return lhs.get_sVal() == self.bstr(rhs.get_sVal())
        if lhs.getType() == Value.DVAL:
            if rhs.getType() == Value.DVAL:
                return lhs.get_dVal() == rhs.get_dVal()
            if rhs.getType() == Value.SVAL:
                ld = lhs.get_dVal()
                lds = "%d-%02d-%02d" % (ld.year, ld.month, ld.day)
                rv = rhs.get_sVal()
                return lds == rv if type(rv) == str else self.b(lds) == rv
            return False
        if lhs.getType() == Value.TVAL:
            if rhs.getType() == Value.TVAL:
                return lhs.get_tVal() == rhs.get_tVal()
            if rhs.getType() == Value.SVAL:
                lt = lhs.get_tVal()
                lts = "%02d:%02d:%02d.%06d" % (lt.hour, lt.minute, lt.sec,
                                               lt.microsec)
                rv = rhs.get_sVal()
                return lts == rv if type(rv) == str else self.b(lts) == rv
            return False
        if lhs.getType() == Value.DTVAL:
            if rhs.getType() == Value.DTVAL:
                return lhs.get_dtVal() == rhs.get_dtVal()
            if rhs.getType() == Value.SVAL:
                ldt = lhs.get_dtVal()
                ldts = "%d-%02d-%02dT%02d:%02d:%02d.%06d" % (
                    ldt.year, ldt.month, ldt.day, ldt.hour, ldt.minute,
                    ldt.sec, ldt.microsec)
                rv = rhs.get_sVal()
                return ldts == rv if type(rv) == str else self.b(ldts) == rv
            return False
        if lhs.getType() == Value.LVAL:
            if not rhs.getType() == Value.LVAL:
                return False
            lvals = lhs.get_lVal().values
            rvals = rhs.get_lVal().values
            return self.compare_list(lvals, rvals)
        if lhs.getType() == Value.UVAL:
            if not rhs.getType() == Value.UVAL:
                return False
            lvals = lhs.get_uVal().values
            rvals = rhs.get_uVal().values
            res, _ = self._compare_list(lvals, rvals, self.compare_value)
            return res
        if lhs.getType() == Value.MVAL:
            if not rhs.getType() == Value.MVAL:
                return False
            lkvs = lhs.get_mVal().kvs
            rkvs = rhs.get_mVal().kvs
            return self.compare_map(lkvs, rkvs)
        if lhs.getType() == Value.VVAL:
            if not rhs.getType() == Value.VVAL:
                return False
            return self.compare_node(lhs.get_vVal(), rhs.get_vVal())
        if lhs.getType() == Value.EVAL:
            if not rhs.getType() == Value.EVAL:
                return False
            return self.compare_edge(lhs.get_eVal(), rhs.get_eVal())
        if lhs.getType() == Value.PVAL:
            if not rhs.getType() == Value.PVAL:
                return False
            return self.compare_path(lhs.get_pVal(), rhs.get_pVal())
        return False

    def compare_path(self, lhs: Path, rhs: Path):
        if rhs.steps is None and len(lhs.steps) > 0:
            return False
        if rhs.steps is None and len(lhs.steps) == 0:
            return self.compare_node(lhs.src, rhs.src)
        if len(lhs.steps) != len(rhs.steps):
            return False
        lsrc, rsrc = lhs.src, rhs.src
        for (l, r) in zip(lhs.steps, rhs.steps):
            lreverse = l.type is not None and l.type < 0
            rreverse = r.type is not None and r.type < 0
            lsrc, ldst = (lsrc, l.dst) if not lreverse else (l.dst, lsrc)
            rsrc, rdst = (rsrc, r.dst) if not rreverse else (r.dst, rsrc)
            if not self.compare_node(lsrc, rsrc):
                return False
            if not self.compare_node(ldst, rdst):
                return False
            if self._strict:
                if l.ranking != r.ranking:
                    return False
                if r.name is None or l.name != self.bstr(r.name):
                    return False
                if r.props is None or not self.compare_map(l.props, r.props):
                    return False
            else:
                if r.ranking is not None and l.ranking != r.ranking:
                    return False
                if r.name is not None and l.name != self.bstr(r.name):
                    return False
                if not (r.props is None or self.compare_map(l.props, r.props)):
                    return False
            lsrc, rsrc = ldst, rdst
        return True

    def eid(self, e: Edge, etype: int):
        src, dst = e.src, e.dst
        if e.type is None:
            if etype < 0:
                src, dst = e.dst, e.src
        else:
            if etype != e.type:
                src, dst = e.dst, e.src
        if type(src) == str:
            src = self.bstr(src)
        if type(dst) == str:
            dst = self.bstr(dst)
        return src, dst

    def compare_edge(self, lhs: Edge, rhs: Edge):
        if self._strict:
            if not lhs.name == self.bstr(rhs.name):
                return False
            if not lhs.ranking == rhs.ranking:
                return False
            rsrc, rdst = self.eid(rhs, lhs.type)
            if not (self.compare_vid(lhs.src, rsrc)
                    and self.compare_vid(lhs.dst, rdst)):
                return False
            if rhs.props is None or len(lhs.props) != len(rhs.props):
                return False
        else:
            if rhs.src is not None and rhs.dst is not None:
                rsrc, rdst = self.eid(rhs, lhs.type)
                if not (self.compare_vid(lhs.src, rsrc)
                        and self.compare_vid(lhs.dst, rdst)):
                    return False
            if rhs.ranking is not None:
                if lhs.ranking != rhs.ranking:
                    return False
            if rhs.name is not None:
                if lhs.name != self.bstr(rhs.name):
                    return False
            if rhs.props is None:
                return True
        return self.compare_map(lhs.props, rhs.props)

    def bstr(self, vid) -> bytes:
        return self.b(vid) if type(vid) == str else vid

    def _compare_vid(
            self,
            lid: Union[int, bytes],
            rid: Union[int, bytes, str],
    ) -> bool:
        if type(lid) is bytes:
            return type(rid) in [str, bytes] and lid == self.bstr(rid)
        if type(lid) is int:
            if type(rid) is int:
                return lid == rid
            if type(rid) not in [str, bytes] or self._vid_fn is None:
                return False
            return lid == self._vid_fn(rid)
        return False

    def compare_vid(self, lid: Value, rid: Value) -> bool:
        if lid.getType() == Value.IVAL:
            if rid.getType() == Value.IVAL:
                return self._compare_vid(lid.get_iVal(), rid.get_iVal())
            if rid.getType() == Value.SVAL:
                return self._compare_vid(lid.get_iVal(), rid.get_sVal())
            return False
        if lid.getType() == Value.SVAL:
            if rid.getType() == Value.SVAL:
                return self._compare_vid(lid.get_sVal(), rid.get_sVal())
            return False
        return False

    def compare_node(self, lhs: Vertex, rhs: Vertex):
        rtags = []
        if self._strict:
            assert rhs.vid is not None
            if not self.compare_vid(lhs.vid, rhs.vid):
                return False
            if rhs.tags is None or len(lhs.tags) != len(rhs.tags):
                return False
            rtags = rhs.tags
        else:
            if rhs.vid is not None:
                if not self.compare_vid(lhs.vid, rhs.vid):
                    return False
            if rhs.tags is not None and len(lhs.tags) < len(rhs.tags):
                return False
            rtags = [] if rhs.tags is None else rhs.tags
        for tag in rtags:
            ltag = [[lt.name, lt.props] for lt in lhs.tags
                    if self.bstr(tag.name) == lt.name]
            if len(ltag) != 1:
                return False
            if self._strict:
                if tag.props is None:
                    return False
            else:
                if tag.props is None:
                    continue
            lprops = ltag[0][1]
            if not self.compare_map(lprops, tag.props):
                return False
        return True

    def _get_map_value_by_key(self,
                              key: bytes,
                              kv: Dict[Union[str, bytes], Value]):
        for k, v in kv.items():
            if key == self.bstr(k):
                return True, v
        return False, None

    def compare_map(self, lhs: Dict[bytes, Value], rhs: KV):
        if len(lhs) != len(rhs):
            return False
        for lkey, lvalue in lhs.items():
            found, rvalue = self._get_map_value_by_key(lkey, rhs)
            if not found:
                return False
            if not self.compare_value(lvalue, rvalue):
                return False
        return True

    def compare_list(self, lhs: List[Value], rhs: List[Value]):
        if len(lhs) != len(rhs):
            return False
        res, _ = self._compare_list(lhs, rhs, self.compare_value)
        return res

    def compare_row(self, lhs: Row, rhs: Row):
        if not len(lhs.values) == len(rhs.values):
            return False
        return all(
            self.compare_value(l, r) for (l, r) in zip(lhs.values, rhs.values))

    def _compare_list(self, lhs, rhs, cmp_fn, contains=False):
        visited = []
        for j, rr in enumerate(rhs):
            found = False
            for i, lr in enumerate(lhs):
                if i not in visited and cmp_fn(lr, rr):
                    visited.append(i)
                    found = True
                    break
            if self._whether_return(found):
                return False, j
        size = len(lhs)
        if contains == CmpType.CONTAINS:
            return len(visited) <= size, -1
        if contains == CmpType.NOT_CONTAINS:
            return True, -1
        return len(visited) == size, -1
