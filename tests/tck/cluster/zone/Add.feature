# Copyright (c) 2021 vesoft inc. All rights reserved.
#
# This source code is licensed under Apache 2.0 License,
# attached with Common Clause Condition 1.0, found in the LICENSES directory.
Feature: Zone add

  Scenario: add host into zone
    Given a nebulacluster with 1 graphd and 1 metad and 6 storaged
    When executing query:
      """
      SHOW HOSTS
      """
    Then the result should be, in order:
      | Host                                             | Port  | Status   | Leader count | Leader distribution  | Partition distribution |
      | /.*-storaged-headless.default.svc.cluster.local/ | 9779  | "ONLINE" | 0            | "No valid partition" | "No valid partition"   |
      | /.*-storaged-headless.default.svc.cluster.local/ | 9779  | "ONLINE" | 0            | "No valid partition" | "No valid partition"   |
      | /.*-storaged-headless.default.svc.cluster.local/ | 9779  | "ONLINE" | 0            | "No valid partition" | "No valid partition"   |
      | /.*-storaged-headless.default.svc.cluster.local/ | 9779  | "ONLINE" | 0            | "No valid partition" | "No valid partition"   |
      | /.*-storaged-headless.default.svc.cluster.local/ | 9779  | "ONLINE" | 0            | "No valid partition" | "No valid partition"   |
      | /.*-storaged-headless.default.svc.cluster.local/ | 9779  | "ONLINE" | 0            | "No valid partition" | "No valid partition"   |
      | "Total"                                          | EMPTY | EMPTY    | 0            | EMPTY                | EMPTY                  |
    When executing query, fill replace holders with cluster_name:
      """
      ADD ZONE z1 "{}-storaged-0.{}-storaged-headless.default.svc.cluster.local":9779,"{}-storaged-1.{}-storaged-headless.default.svc.cluster.local":9779;
      ADD ZONE z2 "{}-storaged-2.{}-storaged-headless.default.svc.cluster.local":9779;
      ADD ZONE z3 "{}-storaged-3.{}-storaged-headless.default.svc.cluster.local":9779;
      """
    Then the execution should be successful
    When executing query, fill replace holders with cluster_name:
      """
      ADD HOST "{}-storaged-4.{}-storaged-headless.default.svc.cluster.local":9779 INTO ZONE z1;
      """
    Then the execution should be successful
    When executing query, fill replace holders with cluster_name:
      """
      ADD HOST "{}-storaged-4.{}-storaged-headless.default.svc.cluster.local":9779 INTO ZONE z2;
      """
    Then an ExecutionError should be raised at runtime: Existed!
    # host could not join a zone if it's already in a zone.
    When executing query, fill replace holders with cluster_name:
      """
      ADD HOST "{}-storaged-2.{}-storaged-headless.default.svc.cluster.local":9779 INTO ZONE z1;
      """
    Then an ExecutionError should be raised at runtime: Existed!
    When executing query, fill replace holders with cluster_name:
      """
      DROP HOST "{}-storaged-2.{}-storaged-headless.default.svc.cluster.local":9779 FROM ZONE z2;
      """
    Then the execution should be successful
    When executing query, fill replace holders with cluster_name:
      """
      ADD HOST "{}-storaged-2.{}-storaged-headless.default.svc.cluster.local":9779 INTO ZONE z1;
      """
    Then the execution should be successful
    When executing query, fill replace holders with cluster_name:
      """
      ADD HOST "{}-storaged-8.{}-storaged-headless.default.svc.cluster.local":9779 INTO ZONE z1;
      """
    Then an ExecutionError should be raised at runtime: Invalid parm!
    When executing query, fill replace holders with cluster_name:
      """
      ADD HOST "{}-storaged-5.{}-storaged-headless.default.svc.cluster.local":9779 INTO ZONE z22;
      """
    Then an ExecutionError should be raised at runtime: Zone not existed!

  Scenario: add zone into group
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
      ADD GROUP g1 z1;
      ADD GROUP g2 z2;
      """
    Then the execution should be successful
    When executing query:
      """
      ADD ZONE z3 INTO GROUP g1;
      """
    Then the execution should be successful
    # add a zone in other group
    When executing query:
      """
      ADD ZONE z2 INTO GROUP g1;
      """
    Then the execution should be successful
    # add a zone without any host
    When executing query, fill replace holders with cluster_name:
      """
      DROP HOST "{}-storaged-4.{}-storaged-headless.default.svc.cluster.local":9779 FROM ZONE z4;
      DROP HOST "{}-storaged-5.{}-storaged-headless.default.svc.cluster.local":9779 FROM ZONE z4;
      """
    Then the execution should be successful
    # TODO has bug here,don't know the error message.
    When executing query:
      """
      ADD ZONE z4 INTO GROUP g1;
      """
    Then an ExecutionError should be raised at runtime: Conflict!
    When executing query:
      """
      ADD ZONE z2 INTO GROUP g22;
      """
    Then an ExecutionError should be raised at runtime: Group not existed!
    When executing query:
      """
      ADD ZONE z11 INTO GROUP g2;
      """
    Then an ExecutionError should be raised at runtime: Zone not existed!
