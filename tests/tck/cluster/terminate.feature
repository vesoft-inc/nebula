@cluster
Feature: Nebula service termination test
  All nebula services shold exit as expected after termination

  # The "@" annotations are tags
  # One feature can have multiple scenarios
  # The lines immediately after the feature title are just comments

  Scenario: Basic termination test
    Given a small nebula cluster
    When the cluster was terminated
    Then no service should still running after 4s