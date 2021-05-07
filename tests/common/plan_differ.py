# Copyright (c) 2021 vesoft inc. All rights reserved.
#
# This source code is licensed under Apache 2.0 License,
# attached with Common Clause Condition 1.0, found in the LICENSES directory.

import re
import json

class PlanDiffer:
    ID = "id"
    NAME = "name"
    DEPENDS = "dependencies"
    OP_INFO = "operator info"
    PATTERN = re.compile(r"\{\"loopBody\": \"(\d+)\"\}")

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

        rows = self._expect_plan.get("rows", [])
        column_names = self._expect_plan.get("column_names", [])
        if not self._validate_expect(rows, column_names):
            return False

        return self._diff_plan_node(self._resp_plan, 0, rows, column_names)

    def _diff_plan_node(self, plan_desc, line_num, rows, column_names) -> bool:
        plan_node_desc = plan_desc.plan_node_descs[line_num]
        expect_node = rows[line_num]
        name = bytes.decode(plan_node_desc.name)

        name_col_idx = column_names.index(self.NAME)
        if not self._is_same_node(name, expect_node[name_col_idx]):
            self._err_msg = f"{name} is not expected {expect_node[name_col_idx]}"
            return False

        if self._is_same_node(name, "Loop"):
            op = expect_node[column_names.index(self.OP_INFO)]
            res = self.PATTERN.match(op)
            if not res:
                return False
            body_id = int(res.group(1))
            loop_body_idx = self._loop_body(plan_desc,
                                            plan_node_desc.description)
            if loop_body_idx is None:
                self._err_msg = "Could not find loop body"
                return False
            if not self._diff_plan_node(plan_desc, loop_body_idx, rows, column_names):
                return False
        elif self._is_same_node(name, "Select"):
            # TODO(yee): check select node
            pass
        elif self.OP_INFO in column_names:
            op = expect_node[column_names.index(self.OP_INFO)]
            # Parse expected operator info jsonStr to dict
            expect_op_dict = {}
            if op:
                expect_op_dict = json.loads(op)
            self._err_msg = self._check_op_info(
                expect_op_dict, plan_node_desc.description)
            if self._err_msg:
                return False

        if plan_node_desc.dependencies is None:
            return True

        dep_col_idx = column_names.index(self.DEPENDS)
        exp_deps = expect_node[dep_col_idx]
        if not len(exp_deps) == len(plan_node_desc.dependencies):
            self._err_msg = "Different plan node dependencies: {} vs. {}".format(
                len(plan_node_desc.dependencies), len(exp_deps))
            return False

        for i in range(len(plan_node_desc.dependencies)):
            line_num = plan_desc.node_index_map[plan_node_desc.dependencies[i]]
            if not self._diff_plan_node(plan_desc, line_num, rows, column_names):
                return False
        return True

    def _check_op_info(self, exp, resp):
        if resp is None:
            if exp:
                return f"expect: {exp} but resp plan node is None"
        if exp:
            resp_dict = {
                f"{bytes.decode(pair.key)}": f"{bytes.decode(pair.value)}"
                for pair in resp
            }
            if not self._is_subdict_nested(exp, resp_dict):
                return "Invalid descriptions, expect: {} vs. resp: {}".format(
                    json.dumps(exp), json.dumps(resp_dict))
        return None

    def _is_same_node(self, lhs: str, rhs: str) -> bool:
        return lhs.lower().startswith(rhs.lower())

    def _loop_body(self, plan_desc, description):
        for pair in description:
            if self._is_same_node(bytes.decode(pair.key), "loopBody"):
                id = int(bytes.decode(pair.value))
                return plan_desc.node_index_map[id]
        return None

    def _is_subdict_nested(self, expect, resp):
        key_list = []
        extracted_expected_dict = expect

        # Extract the innermost dict of nested dict and save the keys into key_list
        while (len(extracted_expected_dict) == 1 and
               isinstance(list(extracted_expected_dict.values())[0], dict)):
            k = list(extracted_expected_dict.keys())[0]
            v = list(extracted_expected_dict.values())[0]
            key_list.append(k)
            extracted_expected_dict = v
        # The inner map cannot be empty
        if len(extracted_expected_dict) == 0:
            return None
        # Unnested dict, push the first key into list
        if extracted_expected_dict == expect:
            key_list.append(list(expect.keys())[0])

        extracted_resp_dict = {}
        if len(key_list) == 1:
            extracted_resp_dict = resp
        else:
            extracted_resp_dict = self._convert_jsonStr_to_dict(resp, key_list)

        def _is_subdict(small, big):
            return dict(big, **small) == big

        return _is_subdict(extracted_expected_dict, extracted_resp_dict)
    
    # resp: pair(key, jsonStr)
    def _convert_jsonStr_to_dict(self, resp, key_list):
        resp_json_str = ''
        init_key = key_list[0]

        # Check if the first key can be found in resp
        if init_key in resp:
            resp_json_str = resp[init_key]
        else:
            return "Failed to find the expected key: {} in the response: {}".format(
                init_key, json.dumps(resp))

        # Convert json str to dict
        resp_json_dict = {}
        parsed_obj = json.loads(resp_json_str)
        resp_json_dict = self._extract_dict_from_obj(parsed_obj)

        # Check if the rest keys exist in resp
        for i in range(1, len(key_list)):
            key = key_list[i]
            if key in resp_json_dict:
                resp_json_dict = self._extract_dict_from_obj(
                    resp_json_dict[key])
            else:
                return "Failed to find the expected key: {} in the response: {}".format(
                    key, json.dumps(resp))
        return resp_json_dict

    def _extract_dict_from_obj(self, obj) -> dict:
        if isinstance(obj, list):
            merged_dict = {}
            for dict_ in obj:
                merged_dict.update(dict_)
            return merged_dict
        elif isinstance(obj, dict):
            return obj
        else:
            return "Failed to extract dict from unknown object: {} type: {}".format(
                obj, type(obj))

    def _validate_expect(self, rows, column_names):
        # Check expected plan column
        if self.ID not in column_names:
            self._err_msg = "Plan node id column is missing in expectde plan"
            return False
        if self.NAME not in column_names:
            self._err_msg = "Plan node name column is missing in expectde plan"
            return False
        if self.DEPENDS not in column_names:
            self._err_msg = "Plan node dependencies column is missing in expectde plan"
            return False
        if self.OP_INFO not in column_names:
            self._err_msg = "Plan node operator info column is missing in expectde plan"
            return False
        
        id_idx_dict = {}
        # Check node id existence
        for i in range(len(rows)):
            node_id = rows[i][0]
            if not node_id:
                self._err_msg = "Plan node id is missing in expectde plan"
                return False
            id_idx_dict[int(node_id)] = i

        # Check dependencies
        for i in range(len(rows)):
            deps = rows[i][2]
            if deps:
                for dep in deps:
                    if dep not in id_idx_dict:
                        self._err_msg = "Failed to find dependencies: {}".format(dep)
                        return False
        return True
