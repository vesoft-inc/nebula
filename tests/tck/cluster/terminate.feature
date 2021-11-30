# Copyright (c) 2021 vesoft inc. All rights reserved.
#
# This source code is licensed under Apache 2.0 License.
Feature: Nebula service termination test

  # All nebula services shold exit as expected after termination
  Scenario: Basic termination test
    Given a nebulacluster with 1 graphd and 1 metad and 1 storaged
    When the cluster was terminated
    Then no service should still running after 4s
