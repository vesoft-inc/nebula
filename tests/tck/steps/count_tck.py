# -*- coding: utf-8 -*-

# Copyright (c) 2020 vesoft inc. All rights reserved.
#
# This source code is licensed under Apache 2.0 License,
# attached with Common Clause Condition 1.0, found in the LICENSES directory.

import sys
import json

from pytest_bdd.parser import Scenario
from pytest_bdd import scenarios

import pandas as pd

columns = ("feature_path", "name", "backgrounds", "run_steps", "expected")

scenarios("features", "openCypher/features", "slowquery")
caller = sys._getframe(0).f_locals
frame_keys = caller.copy().keys()


class TckScenario(object):
    feature_path: str
    name: str
    backgrounds: str
    run_steps: str
    expected: str

    def __repr__(self):
        return json.dumps(self.to_json(), indent=4)

    def to_json(self):
        attrs = ["feature_path", "name", "backgrounds", "run_steps", "expected"]
        return {key: getattr(self, key) for key in attrs}


def collect_scenario(_scenario):
    assert isinstance(_scenario, Scenario)
    steps = _scenario.steps
    background_steps = (
        _scenario.feature.background.steps if _scenario.feature.background is not None else []
    )
    tck_scenario = TckScenario()
    tck_scenario.feature_path = _scenario.feature.filename
    tck_scenario.name = _scenario.name
    tck_scenario.backgrounds = "\n".join(
        ["{} {}".format(step.type, step.name) for step in background_steps]
    )
    _run_steps, _expect_steps = [], []
    _execute_index = _expect_index = 0
    for step in steps:
        if step.type.upper() == "WHEN":
            _execute_index += 1
            _run_steps.append("{}. {} {}".format(_execute_index, "WHEN", step.name))
        elif step.type.upper() == "THEN":
            _expect_index += 1
            _expect_steps.append("{}. {} {}".format(_expect_index, "THEN", step.name))

    tck_scenario.run_steps = "\n".join(_run_steps)
    tck_scenario.expected = "\n".join(_expect_steps)
    return tck_scenario


df = pd.DataFrame(columns=columns)

for item in frame_keys:
    if item.startswith("test"):
        r = {}
        func = caller[item]
        scenario = func.__scenario__
        data = collect_scenario(scenario)
        new = pd.Series(data.to_json())
        df = df.append(new, ignore_index=True)

df.to_excel("scenarios.xlsx", index=False)

print("finish")
