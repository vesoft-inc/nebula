# Copyright (c) 2021 vesoft inc. All rights reserved.
#
# This source code is licensed under Apache 2.0 License.
Feature: LDBC Interactive Workload - Short Reads

  Background:
    Given a graph with space named "ldbc-v0.3.3"

  Scenario: Friends with certain name
    When executing query:
    """

    """
