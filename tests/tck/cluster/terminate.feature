@cluster
Feature: Nebula service termination test
  All nebula services shold exit as expected after termination

  Scenario: Basic termination test
    Given a small nebula cluster
    When the cluster was terminated
    Then no service should still running after 4s