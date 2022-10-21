# Copyright (c) 2021 vesoft inc. All rights reserved.
#
# This source code is licensed under Apache 2.0 License.
Feature: Singular Optional Match

  Background:
    Given a graph with space named "nba"

  Scenario: singular optional match
    When executing query:
      """
      OPTIONAL MATCH (v:player)
      WHERE v.player.name == "Cheng Xuntao"
      RETURN v
      LIMIT 10;
      """
    Then the result should be, in any order:
      | v |
    # the above result set is actually wrong. it shall produce a null record.
    # but we leave it as an todo. Currently, we prevent it from producing
    # random records.
    # TODO(Xuntao): produce null results on failed matches in
    # all optional match queries.
    When executing query:
      """
      OPTIONAL MATCH (v:player)
      WHERE v.player.name == "Tim Duncan"
      RETURN v.player.name as name
      LIMIT 10;
      """
    Then the result should be, in any order:
      | name         |
      | "Tim Duncan" |
