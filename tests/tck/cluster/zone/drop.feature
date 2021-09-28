# Copyright (c) 2021 vesoft inc. All rights reserved.
#
# This source code is licensed under Apache 2.0 License,
# attached with Common Clause Condition 1.0, found in the LICENSES directory.
Feature: Zone drop

  Scenario: drop zone
    Given a nebulacluster with 1 graphd and 1 metad and 6 storaged
    When executing query, fill replace holders with cluster_name:
      """
      ADD ZONE z1 "{}-storaged-0.{}-storaged-headless.default.svc.cluster.local":9779,"{}-storaged-1.{}-storaged-headless.default.svc.cluster.local":9779;
      ADD ZONE z2 "{}-storaged-2.{}-storaged-headless.default.svc.cluster.local":9779;
      ADD ZONE z3 "{}-storaged-3.{}-storaged-headless.default.svc.cluster.local":9779;
      ADD ZONE z4 "{}-storaged-4.{}-storaged-headless.default.svc.cluster.local":9779,"{}-storaged-5.{}-storaged-headless.default.svc.cluster.local":9779;
      """
    Then the execution should be successful
    When executing query:
      """
      ADD GROUP g2 z2,z3,z4;
      ADD GROUP g3 z3,z4;
      """
    Then the execution should be successful
    When executing query:
      """
      SHOW ZONES
      """
    Then the result should be, in any order:
      | Name | Host                                                          | Port |
      | "z1" | /.*-storaged-0.*-storaged-headless.default.svc.cluster.local/ | 9779 |
      | "z1" | /.*-storaged-1.*-storaged-headless.default.svc.cluster.local/ | 9779 |
      | "z2" | /.*-storaged-2.*-storaged-headless.default.svc.cluster.local/ | 9779 |
      | "z3" | /.*-storaged-3.*-storaged-headless.default.svc.cluster.local/ | 9779 |
      | "z4" | /.*-storaged-4.*-storaged-headless.default.svc.cluster.local/ | 9779 |
      | "z4" | /.*-storaged-5.*-storaged-headless.default.svc.cluster.local/ | 9779 |
    # drop no group zone
    When executing query:
      """
      DROP ZONE z1
      """
    Then the execution should be successful
    When executing query:
      """
      SHOW ZONES
      """
    Then the result should be, in any order:
      | Name | Host                                                          | Port |
      | "z2" | /.*-storaged-2.*-storaged-headless.default.svc.cluster.local/ | 9779 |
      | "z3" | /.*-storaged-3.*-storaged-headless.default.svc.cluster.local/ | 9779 |
      | "z4" | /.*-storaged-4.*-storaged-headless.default.svc.cluster.local/ | 9779 |
      | "z4" | /.*-storaged-5.*-storaged-headless.default.svc.cluster.local/ | 9779 |
    # cannot drop zone in groups
    When executing query:
      """
      DROP ZONE z2
      """
    Then an ExecutionError should be raised at runtime: Not allowed to drop!
    When executing query:
      """
      DROP ZONE z2 FROM GROUP g2
      """
    Then the execution should be successful
    When executing query:
      """
      DROP ZONE z2
      """
    Then the execution should be successful
    Then drop the used nebulacluster
