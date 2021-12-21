# Copyright (c) 2021 vesoft inc. All rights reserved.
#
# This source code is licensed under Apache 2.0 License.
Feature: Verify client version

  Scenario: compatible version
    Given nothing
    When connecting the servers with a compatible client version
    Then the connection should be established

  Scenario: incompatible version
    Given nothing
    When connecting the servers with a client version of 100.0.0
    Then the connection should be rejected
