# Copyright (c) 2020 vesoft inc. All rights reserved.
#
# This source code is licensed under Apache 2.0 License,
# attached with Common Clause Condition 1.0, found in the LICENSES directory.

import re


class PlanDiffer:
    OP_INFO = "operator info"
    DEPENDS = "dependencies"
    NAME = "name"
    PATTERN = re.compile(r"loopBody: (\d+)")

    def __init__(self, resp, expect):
        self._resp_plan = resp
        self._expect_plan = expect
        self._err_msg = ""

    def err_msg(self) -> str:
        return self._err_msg

    def diff(self) -> bool:
        if self._expect_plan is None:
            return True
        if self._resp_plan is None:
            return False

        return self._diff_plan_node(self._resp_plan, 0, self._expect_plan, 0)

    def _diff_plan_node(self, plan_desc, line_num, expect, expect_idx) -> bool:
        rows = expect.get("rows", [])
        column_names = expect.get("column_names", [])

        plan_node_desc = plan_desc.plan_node_descs[line_num]
        expect_node = rows[expect_idx]
        name = bytes.decode(plan_node_desc.name)

        idx = column_names.index(self.NAME)

        if not self._is_same_node(name, expect_node[idx]):
            self._err_msg = f"{name} is not expected {expect_node[idx]}"
            return False

        if self._is_same_node(name, "Loop"):
            op = expect_node[column_names.index(self.OP_INFO)]
            res = self.PATTERN.match(op)
            if not res:
                return False
            bodyId = int(res.group(1))
            loopBodyIdx = self._loop_body(plan_desc,
                                          plan_node_desc.description)
            if loopBodyIdx is None:
                self._err_msg = "Could not find loop body"
                return False
            if not self._diff_plan_node(plan_desc, loopBodyIdx, expect,
                                        bodyId):
                return False
        elif self._is_same_node(name, "Select"):
            # TODO(yee): check select node
            pass
        elif self.OP_INFO in column_names:
            # TODO(yee): Parse the op info as a list
            op = expect_node[column_names.index(self.OP_INFO)]
            self._err_msg = self._check_op_info(plan_node_desc.description, op)
            if self._err_msg:
                return False

        if plan_node_desc.dependencies is None:
            return True

        idx = column_names.index(self.DEPENDS)
        exp_deps = expect_node[idx]
        if not len(exp_deps) == len(plan_node_desc.dependencies):
            self._err_msg = "Different plan node dependencies: {} vs. {}".format(
                len(plan_node_desc.dependencies), len(exp_deps))
            return False

        for i in range(len(plan_node_desc.dependencies)):
            line_num = plan_desc.node_index_map[plan_node_desc.dependencies[i]]
            if not self._diff_plan_node(plan_desc, line_num, expect,
                                        int(exp_deps[i])):
                return False
        return True

    def _check_op_info(self, resp, exp):
        if resp is None:
            if exp:
                return f"expect: {exp} but resp plan node is None"
        else:
            descs = {
                f"{bytes.decode(pair.key)}: {bytes.decode(pair.value)}"
                for pair in resp
            }
            if exp and not set([exp]).issubset(descs):
                return "Invalid descriptions, expect: {} vs. resp: {}".format(
                    '; '.join(map(str, [exp])), '; '.join(map(str, descs)))
        return None

    def _is_same_node(self, lhs: str, rhs: str) -> bool:
        return lhs.lower().startswith(rhs.lower())

    def _loop_body(self, plan_desc, description):
        for pair in description:
            if self._is_same_node(bytes.decode(pair.key), "loopBody"):
                id = int(bytes.decode(pair.value))
                return plan_desc.node_index_map[id]
        return None
