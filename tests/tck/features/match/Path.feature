# Copyright (c) 2020 vesoft inc. All rights reserved.
#
# This source code is licensed under Apache 2.0 License.
@xuntaotest
Feature: Matching paths

  Background:
    Given load "test" csv data to a new space

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

  @skip #bug to fix: https://github.com/vesoft-inc/nebula/issues/5185
  Scenario: conflicting type
    When executing query:
      """
      match p = (v:Label_5)-[e:Rel_0]-(v1:Label_1),
      p2 = (p)-[e2:Rel_2]-(v3:Label_3)
      where id(v) in [100] and id(v3) in [80]
      return count(p), count(p2)
      """
    Then a SemanticError should be raised at runtime: `p': defined with conflicting type

  Scenario: use of defined vertices
    # edges cannot be redefined, tested in Scenario: distinct edges and paths
    When executing query:
      """
      match p = (v:Label_5)-[e:Rel_0]->(v1:Label_1),
      p2 = (v)-[e1:Rel_0]->(v1)
      where p!=p2
      return count(p), count(p2)
      """
    Then the result should be, in any order:
      | count(p) | count(p2) |
      | 0        | 0         |
    When executing query:
      """
      match p = (v:Label_5)-[e:Rel_0]->(v1:Label_1),
      p2 = (v)-[e1:Rel_0]->(v1)
      where p==p2
      return count(p), count(p2)
      """
    Then the result should be, in any order:
      | count(p) | count(p2) |
      | 94       | 94        |
    When executing query:
      """
      match p = (v:Label_5)-[e:Rel_0]->(v1:Label_1)
      match p2 = (v)-[e1:Rel_0]->(v1)
      where p!=p2
      return count(p), count(p2)
      """
    Then the result should be, in any order:
      | count(p) | count(p2) |
      | 0        | 0         |
    When executing query:
      """
      match p = (v:Label_5)-[e:Rel_0]->(v1:Label_1)
      match p2 = (v)-[e1:Rel_0]->(v1)
      where p==p2
      return count(p), count(p2)
      """
    Then the result should be, in any order:
      | count(p) | count(p2) |
      | 94       | 94        |

  @skip #bug to fix: https://github.com/vesoft-inc/nebula/issues/5187
  Scenario: edge directions in paths
    When executing query:
      """
      match p = (v:Label_5)-[e:Rel_0]->(v1:Label_1),
      p2 = (v)-[e1:Rel_0]->(v1)
      where id(v) == 47
      and p != p2
      and e == e1
      and v == v
      and v1 == v1
      return count(*)
      """
    Then the result should be, in any order:
      | count(*) |
      | 0        |

  Scenario: use defined path variable
    When executing query:
      """
      match p = (v:Label_5)-[e:Rel_0]->(v1:Label_1)
      return count(nodes(p)), count(keys(v)), count(labels(v)), id(v)
      """
    Then the result should be, in any order:
      | count(nodes(p)) | count(keys(v)) | count(labels(v)) | id(v) |
      | 1               | 1              | 1                | 45    |
      | 1               | 1              | 1                | 30    |
      | 1               | 1              | 1                | 20    |
      | 1               | 1              | 1                | 16    |
      | 2               | 2              | 2                | 68    |
      | 1               | 1              | 1                | 41    |
      | 1               | 1              | 1                | 100   |
      | 1               | 1              | 1                | 65    |
      | 1               | 1              | 1                | 11    |
      | 2               | 2              | 2                | 3     |
      | 3               | 3              | 3                | 77    |
      | 3               | 3              | 3                | 48    |
      | 2               | 2              | 2                | 72    |
      | 5               | 5              | 5                | 67    |
      | 2               | 2              | 2                | 8     |
      | 2               | 2              | 2                | 36    |
      | 2               | 2              | 2                | 95    |
      | 1               | 1              | 1                | 1     |
      | 2               | 2              | 2                | 60    |
      | 1               | 1              | 1                | 94    |
      | 2               | 2              | 2                | 35    |
      | 1               | 1              | 1                | 98    |
      | 2               | 2              | 2                | 39    |
      | 3               | 3              | 3                | 54    |
      | 2               | 2              | 2                | 24    |
      | 1               | 1              | 1                | 83    |
      | 2               | 2              | 2                | 53    |
      | 2               | 2              | 2                | 82    |
      | 1               | 1              | 1                | 102   |
      | 3               | 3              | 3                | 27    |
      | 1               | 1              | 1                | 56    |
      | 1               | 1              | 1                | 76    |
      | 2               | 2              | 2                | 47    |
      | 3               | 3              | 3                | 59    |
      | 24              | 24             | 24               | 383   |
      | 1               | 1              | 1                | 88    |
      | 2               | 2              | 2                | 97    |
      | 2               | 2              | 2                | 38    |
      | 1               | 1              | 1                | 96    |
      | 3               | 3              | 3                | 99    |

  Scenario: symmetry paths
    When executing query:
      """
      match p1 = (v1)-[e*1..2]->(v2)
      where id(v1) in [6]
      match p2 = (v2)-[e*1..2]->(v1)
      return count(*)
      """
    Then the result should be, in any order:
      | count(*) |
      | 16       |
    When executing query:
      """
      match p1 = (v1)-[e*1..2]->(v2)
      where id(v1) in [6]
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

  Scenario: src dst variables in paths
    When executing query:
      """
      match p = (v1:Label_11)-[e:Rel_0]->(v1)
      return src(e), dst(e)
      """
    Then the result should be, in any order:
      | src(e) | dst(e) |
      | 97     | 97     |
      | 47     | 47     |
      | 6      | 6      |
      | 79     | 79     |
      | 19     | 19     |
    When executing query:
      """
      match p = (v1:Label_11)-[e:Rel_0]->(v2)
      where id(v1) == id(v2)
      return src(e), dst(e)
      """
    Then the result should be, in any order:
      | src(e) | dst(e) |
      | 97     | 97     |
      | 47     | 47     |
      | 6      | 6      |
      | 79     | 79     |
      | 19     | 19     |
    When executing query:
      """
      match p = (v1:Label_11)-[e:Rel_0]->(v2)
      where id(v1) != id(v2)
      return count(*)
      """
    Then the result should be, in any order:
      | count(*) |
      | 185      |
    When executing query:
      """
      match p = (v1:Label_11)-[e:Rel_0]->(v2)
      return count(*)
      """
    Then the result should be, in any order:
      | count(*) |
      | 190      |

  Scenario: edgelist or single edge in paths
    When executing query:
      """
      match p = (v1:Label_11)-[e:Rel_0*2..4]->(v2)
      where id(v1) in [50]
      return size(e), count(*)
      """
    Then the result should be, in any order:
      | size(e) | count(*) |
      | 4       | 4        |
      | 3       | 2        |
      | 2       | 1        |
    When executing query:
      """
      match p = (v1:Label_11)-[e:Rel_0*2..4]->(v2)
      where id(v1) in [50]
      unwind e as x
      return x.Rel_0_6_datetime, src(x)
      """
    Then the result should be, in any order:
      | x.Rel_0_6_datetime         | src(x) |
      | 2023-01-04T03:24:23.222000 | 50     |
      | 2023-01-04T03:24:23.035000 | 93     |
      | 2023-01-04T03:24:23.222000 | 50     |
      | 2023-01-04T03:24:23.035000 | 93     |
      | 2023-01-04T03:24:23.289000 | 39     |
      | 2023-01-04T03:24:23.222000 | 50     |
      | 2023-01-04T03:24:23.035000 | 93     |
      | 2023-01-04T03:24:23.257000 | 39     |
      | 2023-01-04T03:24:23.222000 | 50     |
      | 2023-01-04T03:24:23.035000 | 93     |
      | 2023-01-04T03:24:23.289000 | 39     |
      | 2023-01-04T03:24:23.172000 | 12     |
      | 2023-01-04T03:24:23.222000 | 50     |
      | 2023-01-04T03:24:23.035000 | 93     |
      | 2023-01-04T03:24:23.289000 | 39     |
      | 2023-01-04T03:24:22.997000 | 12     |
      | 2023-01-04T03:24:23.222000 | 50     |
      | 2023-01-04T03:24:23.035000 | 93     |
      | 2023-01-04T03:24:23.257000 | 39     |
      | 2023-01-04T03:24:23.255000 | 76     |
      | 2023-01-04T03:24:23.222000 | 50     |
      | 2023-01-04T03:24:23.035000 | 93     |
      | 2023-01-04T03:24:23.257000 | 39     |
      | 2023-01-04T03:24:23.193000 | 76     |
    When executing query:
      """
      match p = (v1:Label_11)-[e:Rel_0*2..2]->(v2)
      where id(v1) in [50]
      unwind e as x
      return x.Rel_0_6_datetime, src(x)
      """
    Then the result should be, in any order:
      | x.Rel_0_6_datetime         | src(x) |
      | 2023-01-04T03:24:23.222000 | 50     |
      | 2023-01-04T03:24:23.035000 | 93     |
    When executing query:
      """
      match p = (v1:Label_11)-[e:Rel_0*1..1]->(v2)
      where id(v1) in [50]
      unwind e as x
      return x.Rel_0_6_datetime, src(x)
      """
    Then the result should be, in any order:
      | x.Rel_0_6_datetime         | src(x) |
      | 2023-01-04T03:24:23.222000 | 50     |
    When executing query:
      """
      match (v1)-[e*1..2]->(v2)
      where id(v1) in [6]
      match (v2)-[e*1..2]->(v1)
      return count(*)
      """
    Then the result should be, in any order:
      | count(*) |
      | 9        |
    When executing query:
      """
      match (v1)-[e*1..2]->(v2)
      where id(v1) in [6]
      match (v2)-[e*1..1]->(v1)
      return count(*)
      """
    Then a SemanticError should be raised at runtime: e binding to different type: Edge vs EdgeList
