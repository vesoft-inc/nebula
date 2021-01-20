# Copyright (c) 2020 vesoft inc. All rights reserved.
#
# This source code is licensed under Apache 2.0 License,
# attached with Common Clause Condition 1.0, found in the LICENSES directory.


class PlanDiffer:
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

        idx = column_names.index("name")

        if not name.lower().startswith(expect_node[idx].lower()):
            self._err_msg = f"{name} is not expected {expect_node[idx]}"
            return False

        if "operator info" in expect_node:
            descs = {
                f"{bytes.decode(pair.key)}: {bytes.decode(pair.value)}"
                for pair in plan_node_desc.description
            }
            op_info = expect_node['operator info']
            if not set(op_info).issubset(descs):
                self._err_msg = "Invalid descriptions, expect: {} vs. resp: {}".format(
                    '; '.join(map(str, op_info)), '; '.join(map(str, descs)))
                return False

        if plan_node_desc.dependencies is None:
            return True

        idx = column_names.index('dependencies')
        exp_deps = expect_node[idx]
        if not len(exp_deps) == len(plan_node_desc.dependencies):
            self._err_msg = "Different plan node dependencies: {} vs. {}".format(
                len(plan_node_desc.dependencies), len(exp_deps))
            return False

        for i in range(len(plan_node_desc.dependencies)):
            line_num = plan_desc.node_index_map[plan_node_desc.dependencies[i]]
            if not self._diff_plan_node(plan_desc, line_num, expect, exp_deps[i]):
                return False
        return True
