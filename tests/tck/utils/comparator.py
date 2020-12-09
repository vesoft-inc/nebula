# Copyright (c) 2020 vesoft inc. All rights reserved.
#
# This source code is licensed under Apache 2.0 License,
# attached with Common Clause Condition 1.0, found in the LICENSES directory.

import math

from nebula2.data.DataObject import (
    Node,
    Record,
    Relationship,
    PathWrapper,
    DataSetWrapper,
    ValueWrapper,
)


class DataSetWrapperComparator:
    def __init__(self, strict=True, order=False):
        self._strict = strict
        self._order = order

    def __call__(self, lhs: DataSetWrapper, rhs: DataSetWrapper):
        return self.compare(lhs, rhs)

    def compare(self, lhs: DataSetWrapper, rhs: DataSetWrapper):
        if lhs.get_row_size() != rhs.get_row_size():
            return False
        if not lhs.get_col_names() == rhs.get_col_names():
            return False
        if self._order:
            return all(self.compare_row(l, r) for (l, r) in zip(lhs, rhs))
        return self._compare_list(lhs, rhs, self.compare_row)

    def compare_value(self, lhs: ValueWrapper, rhs: ValueWrapper):
        """
        lhs and rhs represent response data and expected data respectively
        """
        if lhs.is_null():
            return rhs.is_null() and lhs.as_null() == rhs.as_null()
        if lhs.is_empty():
            return rhs.is_empty()
        if lhs.is_bool():
            return rhs.is_bool() and lhs.as_bool() == rhs.as_bool()
        if lhs.is_int():
            return rhs.is_int() and lhs.as_int() == rhs.as_int()
        if lhs.is_double():
            return (rhs.is_double()
                    and math.fabs(lhs.as_double() - rhs.as_double()) < 1.0E-8)
        if lhs.is_string():
            return rhs.is_string() and lhs.as_string() == rhs.as_string()
        if lhs.is_date():
            return (rhs.is_date() and lhs.as_date() == rhs.as_date()) or (
                rhs.is_string() and str(lhs.as_date()) == rhs.as_string())
        if lhs.is_time():
            return (rhs.is_time() and lhs.as_time() == rhs.as_time()) or (
                rhs.is_string() and str(lhs.as_time()) == rhs.as_string())
        if lhs.is_datetime():
            return ((rhs.is_datetime()
                     and lhs.as_datetime() == rhs.as_datetime())
                    or (rhs.is_string()
                        and str(lhs.as_datetime()) == rhs.as_string()))
        if lhs.is_list():
            return rhs.is_list() and self.compare_list(lhs.as_list(),
                                                       rhs.as_list())
        if lhs.is_set():
            return rhs.is_set() and self._compare_list(
                lhs.as_set(), rhs.as_set(), self.compare_value)
        if lhs.is_map():
            return rhs.is_map() and self.compare_map(lhs.as_map(),
                                                     rhs.as_map())
        if lhs.is_vertex():
            return rhs.is_vertex() and self.compare_node(
                lhs.as_node(), rhs.as_node())
        if lhs.is_edge():
            return rhs.is_edge() and self.compare_edge(lhs.as_relationship(),
                                                       rhs.as_relationship())
        if lhs.is_path():
            return rhs.is_path() and self.compare_path(lhs.as_path(),
                                                       rhs.as_path())
        return False

    def compare_path(self, lhs: PathWrapper, rhs: PathWrapper):
        if lhs.length() != rhs.length():
            return False
        return all(
            self.compare_node(l.start_node, r.start_node)
            and self.compare_node(l.end_node, r.end_node)
            and self.compare_edge(l.relationship, r.relationship)
            for (l, r) in zip(lhs, rhs))

    def compare_edge(self, lhs: Relationship, rhs: Relationship):
        if not lhs == rhs:
            return False
        if not self._strict:
            return True
        return self.compare_map(lhs.propertys(), rhs.propertys())

    def compare_node(self, lhs: Node, rhs: Node):
        if lhs.get_id() != rhs.get_id():
            return False
        if not self._strict:
            return True
        if len(lhs.tags()) != len(rhs.tags()):
            return False
        for tag in lhs.tags():
            if not rhs.has_tag(tag):
                return False
            lprops = lhs.propertys(tag)
            rprops = rhs.propertys(tag)
            if not self.compare_map(lprops, rprops):
                return False
        return True

    def compare_map(self, lhs: dict, rhs: dict):
        if len(lhs) != len(rhs):
            return False
        for lkey, lvalue in lhs.items():
            if lkey not in rhs:
                return False
            rvalue = rhs[lkey]
            if not self.compare_value(lvalue, rvalue):
                return False
        return True

    def compare_list(self, lhs, rhs):
        if len(lhs) != len(rhs):
            return False
        if self._strict:
            return all(self.compare_value(l, r) for (l, r) in zip(lhs, rhs))
        return self._compare_list(lhs, rhs, self.compare_value)

    def compare_row(self, lrecord: Record, rrecord: Record):
        if not lrecord.size() == rrecord.size():
            return False
        return all(self.compare_value(l, r)
                   for (l, r) in zip(lrecord, rrecord))

    def _compare_list(self, lhs, rhs, cmp_fn):
        visited = []
        size = 0
        for lr in lhs:
            size += 1
            found = False
            for i, rr in enumerate(rhs):
                if i not in visited and cmp_fn(lr, rr):
                    visited.append(i)
                    found = True
                    break
            if not found:
                return False
        return len(visited) == size
