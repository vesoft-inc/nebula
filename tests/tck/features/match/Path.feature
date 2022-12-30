# Copyright (c) 2020 vesoft inc. All rights reserved.
#
# This source code is licensed under Apache 2.0 License.
Feature: Matching paths

  Background:
    Given a graph with space named "test"

  Scenario: distinct edges and paths
    When executing query:
      """
      match p = (v:Label_0)-[e:Rel_0]-(v1:Label_1)-[e1:Rel_1]-(v2:Label_2),
      p = (v:Label_0)-[e:Rel_0]-(v1:Label_1)-[e1:Rel_1]-(v2:Label_2)
      return *
      """
    Then a SemanticError should be raised at runtime: `e': Redefined alias
    When executing query:
      """
      match p = (v:Label_0)-[e:Rel_0]-(v1:Label_1)-[e1:Rel_1]-(v2:Label_2),
      p = (v:Label_0)-[e2:Rel_0]-(v1:Label_1)-[e1:Rel_1]-(v2:Label_2)
      return *
      """
    Then a SemanticError should be raised at runtime: `e1': Redefined alias
    When executing query:
      """
      match p = (v:Label_0)-[e:Rel_0]-(v1:Label_1)-[e1:Rel_1]-(v2:Label_2),
      p = (v:Label_0)-[e2:Rel_0]-(v1:Label_1)-[er:Rel_1]-(v2:Label_2)
      return *
      """
    Then a SemanticError should be raised at runtime: `p': Redefined alias
    When executing query:
      """
      match p = (v:Label_5)-[e:Rel_0]-(v1:Label_1)-[e1:Rel_1]-(v2:Label_2),
      p2 = (v:Label_8)-[e2:Rel_0]-(v1:Label_1)-[e3:Rel_1]-(v2:Label_2)
      return count(p), count(p2)
      """
    Then the result should be, in any order:
      | count(p) | count(p2) |
      | 966      | 966       |

  Scenario: symmetry paths
    When executing query:
      """
      match p1 = (v1)-[e*1..2]->(v2) where id(v1) in [6]
      match p2 = (v2)-[e*1..2]->(v1)
      return count(*)
      """
    Then the result should be, in any order:
      | count(*) |
      | 16       |
    When executing query:
      """
      match p1 = (v1)-[e*1..2]->(v2) where id(v1) in [6]
      match p2 = (v2)-[e*1..2]->(v1)
      return size(e)
      """
    Then the result should be, in any order:
      | size(e) |
      | 1       |
      | 1       |
      | 1       |
      | 1       |
      | 2       |
      | 2       |
      | 2       |
      | 2       |
      | 2       |
      | 2       |
      | 2       |
      | 2       |
      | 2       |
      | 2       |
      | 2       |
      | 2       |
