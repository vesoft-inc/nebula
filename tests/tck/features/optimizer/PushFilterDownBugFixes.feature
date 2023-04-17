# Copyright (c) 2023 vesoft inc. All rights reserved.
#
# This source code is licensed under Apache 2.0 License.
Feature: Bug fixes on filter push-down

  Background:
    Given a graph with space named "ngdata"

  Scenario: missing predicate bug
    When profiling query:
      """
      MATCH (v0)<-[e0]-()<-[e1]-(v1)
      WHERE (id(v0) == 6)
      AND ((NOT (NOT ((v1.Label_6.Label_6_5_Int > (e1.Rel_0_5_Int - 0.300177))
      AND v1.Label_6.Label_6_1_Bool))))
      RETURN count(*)
      """
    Then the result should be, in any order:
      | count(*) |
      | 0        |
    And the execution plan should be:
      | id | name           | dependencies | operator info |
      | 9  | Aggregate      | 11           |               |
      | 11 | Project        | 10           |               |
      | 10 | Filter         | 6            |               |
      | 6  | AppendVertices | 5            |               |
      | 5  | Filter         | 5            |               |
      | 5  | Traverse       | 4            |               |
      | 4  | Traverse       | 2            |               |
      | 2  | Dedup          | 1            |               |
      | 1  | PassThrough    | 3            |               |
      | 3  | Start          |              |               |

# FIXME(czp): Disable this for now. The ngdata test set lacks validation mechanisms
# When profiling query:
# """
# MATCH (v0)<-[e0]-()<-[e1]-(v1)
# WHERE id(v0) == 6
# AND v1.Label_6.Label_6_1_Bool
# RETURN count(*)
# """
# Then the result should be, in any order:
# | count(*) |
# | 126      |
# And the execution plan should be:
# | id | name           | dependencies | operator info |
# | 9  | Aggregate      | 11           |               |
# | 11 | Project        | 10           |               |
# | 10 | Filter         | 6            |               |
# | 6  | AppendVertices | 5            |               |
# | 5  | Filter         | 5            |               |
# | 5  | Traverse       | 4            |               |
# | 4  | Traverse       | 2            |               |
# | 2  | Dedup          | 1            |               |
# | 1  | PassThrough    | 3            |               |
# | 3  | Start          |              |               |
