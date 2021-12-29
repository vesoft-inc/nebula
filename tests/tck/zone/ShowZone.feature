# Copyright (c) 2021 vesoft inc. All rights reserved.
#
# This source code is licensed under Apache 2.0 License.
Feature: Show zones
 Scenario: show zone
    Given a nebulacluster with 1 graphd and 3 metad and 1 storaged
    When executing query:
      """
      ADD HOST
      """
