# Copyright (c) 2020 vesoft inc. All rights reserved.
#
# This source code is licensed under Apache 2.0 License.
Feature: Matching paths

  Background:
    Given a graph with space named "ngdata"

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
    When executing query:
      """
      match p = (v:Label_5)-[e:Rel_0]->(v1:Label_1),
      p2 = (v)<-[e1:Rel_0]-(v1)
      where id(v) == 47
      and (p!=p2 or p<p2 or p>p2)
      and e == e1
      and v == v
      and v1 == v1
      return count(*)
      """
    Then the result should be, in any order:
      | count(*) |
      | 0        |

  # The correctness of the following test cases needs to be verified, mark it @skip for now
  @skip
  Scenario: overlapping aliases variables
    When executing query:
      """
      match p = (v1:Label_3)-[e1]-(v1)
      return count(p)
      """
    Then the result should be, in any order:
      | count(p) |
      | 28       |
    When executing query:
      """
      match p2 = (v2:Label_5)-[e2]-(v2)
      return count(p2)
      """
    Then the result should be, in any order:
      | count(p2) |
      | 43        |
    When executing query:
      """
      match p = (v1:Label_3)-[e1]-(v1)
      match p2 = (v2:Label_5)-[e2]-(v2)
      return count(p), count(p2), count(*)
      """
    Then the result should be, in any order:
      | count(p) | count(p2) | count(*) |
      | 1204     | 1204      | 1204     |
    When executing query:
      """
      match p = (v1:Label_3)-[e1:Rel_0]-(v1)
      match p2 = (v1:Label_3)-[e2:Rel_1]-(v2)
      return distinct id(v1)
      """
    Then the result should be, in any order:
      | id(v1) |
      | 57     |
      | 47     |
      | 97     |
    When executing query:
      """
      match p = (v1:Label_3)-[e1:Rel_0]-(v1) return distinct id(v1)
      """
    Then the result should be, in any order:
      | id(v1) |
      | 57     |
      | 47     |
      | 97     |
    When executing query:
      """
      match p2 = (v1:Label_3)-[e2:Rel_1]-(v2)
      where id(v1) in [57, 47, 97]
      with v1 as v2
      return id(v2)
      """
    Then the result should be, in any order:
      | id(v2) |
      | 57     |
      | 57     |
      | 57     |
      | 57     |
      | 47     |
      | 47     |
      | 47     |
      | 47     |
      | 47     |
      | 47     |
      | 97     |
      | 97     |
      | 97     |
      | 97     |
    When executing query:
      """
      match p = (v1:Label_3)-[e1:Rel_0]-(v1)
      match p2 = (v1:Label_3)-[e2:Rel_1]-(v2)
      return count(*)
      """
    Then the result should be, in any order:
      | count(*) |
      | 14       |
    When executing query:
      """
      match p = (v1:Label_3)-[e1:Rel_0]-(v1)
      match p2 = (v1:Label_3)-[e1:Rel_0]-(v2)
      where p != p2
      return count(*)
      """
    Then the result should be, in any order:
      | count(*) |
      | 0        |
    When executing query:
      """
      match p = (v1:Label_3)-[e1:Rel_0]-(v1)
      match p2 = (v1:Label_3)-[e1:Rel_0]-(v2)
      where p == p2
      return count(*)
      """
    Then the result should be, in any order:
      | count(*) |
      | 3        |

  Scenario: many paths
    When executing query:
      """
      match p = (v1:Label_3)-[e1:Rel_0]-(v1)
      match p2 = (v1:Label_3)-[e1:Rel_0]-(v1)
      match p3 = (v1:Label_3)-[e1:Rel_0]-(v1)
      match p4 = (v1:Label_3)-[e1:Rel_0]-(v1)
      match p5 = (v1:Label_3)-[e1:Rel_0]-(v1)
      match p6 = (v1:Label_3)-[e1:Rel_0]-(v1)
      match p7 = (v1:Label_3)-[e1:Rel_0]-(v1)
      match p8 = (v1:Label_3)-[e1:Rel_0]-(v1)
      match p9 = (v1:Label_3)-[e1:Rel_0]-(v1)
      match p10 = (v1:Label_3)-[e1:Rel_0]-(v1)
      match p11 = (v1:Label_3)-[e1:Rel_0]-(v1)
      match p12 = (v1:Label_3)-[e1:Rel_0]-(v1)
      match p13 = (v1:Label_3)-[e1:Rel_0]-(v1)
      match p14 = (v1:Label_3)-[e1:Rel_0]-(v1)
      match p15 = (v1:Label_3)-[e1:Rel_0]-(v1)
      match p16 = (v1:Label_3)-[e1:Rel_0]-(v1)
      match p17 = (v1:Label_3)-[e1:Rel_0]-(v1)
      match p18 = (v1:Label_3)-[e1:Rel_0]-(v1)
      match p19 = (v1:Label_3)-[e1:Rel_0]-(v1)
      match p20 = (v1:Label_3)-[e1:Rel_0]-(v1)
      match p21 = (v1:Label_3)-[e1:Rel_0]-(v1)
      match p22 = (v1:Label_3)-[e1:Rel_0]-(v1)
      match p23 = (v1:Label_3)-[e1:Rel_0]-(v1)
      match p24 = (v1:Label_3)-[e1:Rel_0]-(v1)
      match p25 = (v1:Label_3)-[e1:Rel_0]-(v1)
      match p26 = (v1:Label_3)-[e1:Rel_0]-(v1)
      match p27 = (v1:Label_3)-[e1:Rel_0]-(v1)
      match p28 = (v1:Label_3)-[e1:Rel_0]-(v1)
      match p29 = (v1:Label_3)-[e1:Rel_0]-(v1)
      match p30 = (v1:Label_3)-[e1:Rel_0]-(v1)
      match p31 = (v1:Label_3)-[e1:Rel_0]-(v1)
      match p32 = (v1:Label_3)-[e1:Rel_0]-(v1)
      return count(*)
      """
    Then the result should be, in any order:
      | count(*) |
      | 3        |

  Scenario: single vertex
    When executing query:
      """
      match p = (v)
      where id(v) == 1
      return count(*)
      """
    Then the result should be, in any order:
      | count(*) |
      | 1        |
    When executing query:
      """
      match p = (v:Label_3)
      return count(p)
      """
    Then the result should be, in any order:
      | count(p) |
      | 59       |
    When executing query:
      """
      match p = (v:Label_0)
      return count(p)
      """
    Then the result should be, in any order:
      | count(p) |
      | 60       |

  Scenario: conflicting type
    When executing query:
      """
      match p = (v:Label_5)-[e:Rel_0]-(v1:Label_1),
      p2 = (p)-[e2:Rel_2]-(v3:Label_3)
      where id(v) in [100] and id(v3) in [80]
      return count(p), count(p2)
      """
    Then a SemanticError should be raised at runtime: `p': alias redefined

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

  # The correctness of the following test cases needs to be verified, mark it @skip for now
  @skip
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

  # The correctness of the following test cases needs to be verified, mark it @skip for now
  @skip
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

  # The correctness of the following test cases needs to be verified, mark it @skip for now
  @skip
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
      return x.Rel_0_7_Double, src(x)
      """
    Then the result should be, in any order:
      | x.Rel_0_7_Double | src(x) |
      | 0.917773         | 50     |
      | 0.266255         | 93     |
      | 0.917773         | 50     |
      | 0.266255         | 93     |
      | 0.76606          | 39     |
      | 0.917773         | 50     |
      | 0.266255         | 93     |
      | 0.364565         | 39     |
      | 0.917773         | 50     |
      | 0.266255         | 93     |
      | 0.76606          | 39     |
      | 0.019344         | 12     |
      | 0.917773         | 50     |
      | 0.266255         | 93     |
      | 0.76606          | 39     |
      | 0.258873         | 12     |
      | 0.917773         | 50     |
      | 0.266255         | 93     |
      | 0.364565         | 39     |
      | 0.041292         | 76     |
      | 0.917773         | 50     |
      | 0.266255         | 93     |
      | 0.364565         | 39     |
      | 0.893019         | 76     |
    When executing query:
      """
      match p = (v1:Label_11)-[e:Rel_0*2..2]->(v2)
      where id(v1) in [50]
      unwind e as x
      return x.Rel_0_7_Double, src(x)
      """
    Then the result should be, in any order:
      | x.Rel_0_7_Double | src(x) |
      | 0.917773         | 50     |
      | 0.266255         | 93     |
    When executing query:
      """
      match p = (v1:Label_11)-[e:Rel_0*1..1]->(v2)
      where id(v1) in [50]
      unwind e as x
      return x.Rel_0_7_Double, src(x)
      """
    Then the result should be, in any order:
      | x.Rel_0_7_Double | src(x) |
      | 0.917773         | 50     |
    When executing query:
      """
      match (v1)-[e*1..2]->(v2)
      where id(v1) in [6]
      match (v2)-[e*1..2]->(v1)
      return count(*)
      """
    Then the result should be, in any order:
      | count(*) |
      | 16       |
    When executing query:
      """
      match (v1)-[e*1..2]->(v2)
      where id(v1) in [6]
      match (v2)-[e*1..1]->(v1)
      return count(*)
      """
    Then a SemanticError should be raised at runtime: e binding to different type: Edge vs EdgeList
