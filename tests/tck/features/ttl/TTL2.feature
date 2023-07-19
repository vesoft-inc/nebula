# Copyright (c) 2022 vesoft inc. All rights reserved.
#
# This source code is licensed under Apache 2.0 License.
Feature: ttl

  Background:
    Given an empty graph
    And create a space with following options:
      | partition_num  | 1                |
      | replica_factor | 1                |
      | vid_type       | FIXED_STRING(32) |

  Scenario: ttl base
    When executing query:
      """
      CREATE TAG ttl_tag(a timestamp, b int) ttl_duration=5, ttl_col="a";
      CREATE EDGE ttl_edge(name string, ttl timestamp) ttl_duration=5,ttl_col="ttl";
      CREATE EDGE normal_edge();
      """
    Then the execution should be successful
    When executing query:
      """
      CREATE TAG INDEX index_ttl_tag_b on ttl_tag(b);
      CREATE EDGE INDEX index_ttl_edge_name on ttl_edge(name(30));
      """
    Then the execution should be successful
    And wait 5 seconds
    When executing query:
      """
      INSERT VERTEX ttl_tag(a,b) VALUES "1":(now(),10),"2":(now(),20);
      INSERT EDGE ttl_edge(name,ttl) VALUES "1"->"2":("edge_01",now());
      INSERT EDGE normal_edge() VALUES "1"->"2":();
      """
    Then the execution should be successful
    When executing query:
      """
      FETCH PROP ON ttl_tag "1" yield ttl_tag.a as a,ttl_tag.b as b;
      """
    Then the result should be, in any order, with relax comparison:
      | a     | b  |
      | /\d+/ | 10 |
    When executing query:
      """
      LOOKUP ON ttl_tag where ttl_tag.b==10 yield ttl_tag.a as a,ttl_tag.b as b;
      """
    Then the result should be, in any order, with relax comparison:
      | a     | b  |
      | /\d+/ | 10 |
    When executing query:
      """
      FETCH PROP ON ttl_edge "1"->"2" yield ttl_edge.name as name;
      """
    Then the result should be, in any order:
      | name      |
      | "edge_01" |
    When executing query:
      """
      MATCH ()-[e:ttl_edge]->() RETURN e limit 100;
      """
    Then the result should be, in any order, with relax comparison:
      | e                                                     |
      | [:ttl_edge "1"->"2" @0 {name: "edge_01", ttl: /\d+/}] |
    When executing query:
      """
      LOOKUP ON ttl_edge where ttl_edge.name=="edge_01" yield ttl_edge.name as name;
      """
    Then the result should be, in any order, with relax comparison:
      | name      |
      | "edge_01" |
    And wait 10 seconds
    When executing query:
      """
      FETCH PROP ON ttl_tag "1" yield ttl_tag.a as a,ttl_tag.b as b;
      """
    Then the result should be, in any order, with relax comparison:
      | a | b |
    When executing query:
      """
      LOOKUP ON ttl_tag where ttl_tag.b==10 yield ttl_tag.a as a,ttl_tag.b as b;
      """
    Then the result should be, in any order, with relax comparison:
      | a | b |
    When executing query:
      """
      MATCH (v) WHERE id(v) == "1" RETURN v;
      """
    Then the result should be, in any order:
      | v |
    # # Because we do not support the existence of a point without a tag, even
    # # if the point exists, because there is no tag on it, it still cannot be
    # # found
    # Then the result should be, in any order:
    # | v     |
    # | ("1") |
    When executing query:
      """
      FETCH PROP ON ttl_edge "1"->"2" yield ttl_edge.name as name;
      """
    Then the result should be, in any order:
      | name |
    When executing query:
      """
      MATCH ()-[e:ttl_edge]->() RETURN e limit 100;
      """
    Then the result should be, in any order, with relax comparison:
      | e |
    When executing query:
      """
      LOOKUP ON ttl_edge where ttl_edge.name=="edge_01" yield ttl_edge.name as name;
      """
    Then the result should be, in any order, with relax comparison:
      | name |

  Scenario: ttl ddl
    When executing query:
      """
      CREATE TAG ttl_tag01(a int, b int) ttl_duration=5, ttl_col="a";
      """
    Then the execution should be successful
    When executing query:
      """
      CREATE TAG ttl_tag02(a float,b int) ttl_duration=5,ttl_col="a";
      """
    Then a ExecutionError should be raised at runtime: Ttl column type illegal
    When executing query:
      """
      CREATE TAG ttl_tag02(a string,b int) ttl_duration=5,ttl_col="a";
      """
    Then a ExecutionError should be raised at runtime: Ttl column type illegal
    When executing query:
      """
      CREATE TAG ttl_tag02(a timestamp,b int) ttl_duration=5,ttl_col="a";
      """
    Then the execution should be successful
    When executing query:
      """
      CREATE EDGE ttl_edge01(a timestamp, b int) ttl_duration=9223372036854775807, ttl_col="a";
      """
    Then the execution should be successful
    When executing query:
      """
      CREATE EDGE ttl_edge02(a timestamp, b int) ttl_duration=9223372036854775808, ttl_col="a";
      """
    Then a SyntaxError should be raised at runtime: Out of range: near `9223372036854775808'
    When executing query:
      """
      CREATE EDGE ttl_edge02(a timestamp, b int) ttl_duration=5.1, ttl_col="a";
      """
    Then a SyntaxError should be raised at runtime: syntax error near `5.1'
    When executing query:
      """
      CREATE EDGE ttl_edge03(a timestamp, b int) ttl_col="a";
      """
    Then the execution should be successful
    When executing query:
      """
      ALTER EDGE ttl_edge03 ttl_duration=-1
      """
    Then a SyntaxError should be raised at runtime: syntax error near `-1'
    When executing query:
      """
      ALTER EDGE ttl_edge03 ttl_duration=100;
      """
    Then the execution should be successful
    When executing query:
      """
      CREATE EDGE ttl_edge04(a timestamp,b int) ttl_duration=100;
      """
    Then a ExecutionError should be raised at runtime: Implicit ttl_col not support
    When executing query:
      """
      CREATE EDGE ttl_edge04(a timestamp,b int);
      """
    Then the execution should be successful
    When executing query:
      """
      ALTER EDGE ttl_edge04 ttl_duration=100;
      """
    Then a ExecutionError should be raised at runtime: Unsupported!
    When executing query:
      """
      CREATE TAG ttl_tag03(a timestamp,c string);
      """
    Then the execution should be successful
    When executing query:
      """
      ALTER TAG ttl_tag03 ttl_duration=10, ttl_col="b";
      """
    Then a ExecutionError should be raised at runtime: Tag prop not existed!
    When executing query:
      """
      ALTER TAG ttl_tag03 ttl_duration=10, ttl_col="a";
      """
    Then the execution should be successful
    When executing query:
      """
      CREATE TAG INDEX index_ttl_tag03 on ttl_tag03();
      """
    Then the execution should be successful
    When executing query:
      """
      CREATE TAG INDEX index_ttl_tag03_a on ttl_tag03(a);
      """
    Then the execution should be successful
    When executing query:
      """
      CREATE TAG INDEX index_ttl_tag03_c on ttl_tag03(c(10));
      """
    Then the execution should be successful
    When executing query:
      """
      ALTER TAG ttl_tag03 ttl_duration=100;
      """
    Then a ExecutionError should be raised at runtime: Unsupported!
    When executing query:
      """
      ALTER TAG ttl_tag03 ttl_col="a";
      """
    Then a ExecutionError should be raised at runtime: Unsupported!
    When executing query:
      """
      CREATE TAG ttl_tag04(a int,b int);
      """
    Then the execution should be successful
    When executing query:
      """
      CREATE TAG INDEX index_ttl_tag04_a on ttl_tag04(a);
      """
    Then the execution should be successful
    When executing query:
      """
      ALTER TAG ttl_tag04 ttl_col="a";
      """
    Then a ExecutionError should be raised at runtime: Unsupported!

  Scenario: ttl expire
    When executing query:
      """
      CREATE TAG ttl_expire_tag01(a timestamp, b int) ttl_duration=9223372036854775807, ttl_col="a";
      """
    Then the execution should be successful
    And wait 5 seconds
    When executing query:
      """
      INSERT VERTEX ttl_expire_tag01(a,b) VALUES "t-e-t-01":(now(),10);
      """
    Then the execution should be successful
    When executing query:
      """
      MATCH (v:ttl_expire_tag01) RETURN v LIMIT 10;
      """
    # #TODO: If duration+ttl overflows, the expiration of ttl may cause errors
    # Then the result should be, in any order, with relax comparison:
    # | v                                               |
    # | ("t-e-t-01" :ttl_expire_tag01{a: /\d+/, b: 10}) |
    Then the result should be, in any order, with relax comparison:
      | v |
    When executing query:
      """
      CREATE TAG ttl_expire_tag02(a int, b int) ttl_duration=9223372036854775807, ttl_col="a";
      """
    Then the execution should be successful
    And wait 5 seconds
    When executing query:
      """
      INSERT VERTEX ttl_expire_tag02(a,b) VALUES "t-e-t-02":(timestamp(),10);
      """
    Then the execution should be successful
    When executing query:
      """
      MATCH (v:ttl_expire_tag02) RETURN v LIMIT 10;
      """
    # #TODO: If duration+ttl overflows, the expiration of ttl may cause errors
    # Then the result should be, in any order, with relax comparison:
    # | v                                               |
    # | ("t-e-t-02" :ttl_expire_tag02{a: /\d+/, b: 10}) |
    Then the result should be, in any order, with relax comparison:
      | v |
    When executing query:
      """
      CREATE TAG ttl_expire_tag03(a timestamp, b int) ttl_duration=8, ttl_col="a";
      """
    Then the execution should be successful
    And wait 5 seconds
    When executing query:
      """
      INSERT VERTEX ttl_expire_tag03(a,b) VALUES "t-e-t-03":(now(),10);
      """
    Then the execution should be successful
    When executing query:
      """
      MATCH (v:ttl_expire_tag03) RETURN v LIMIT 10;
      """
    Then the result should be, in any order, with relax comparison:
      | v                                               |
      | ("t-e-t-03" :ttl_expire_tag03{a: /\d+/, b: 10}) |
    And wait 10 seconds
    When executing query:
      """
      INSERT VERTEX ttl_expire_tag03(a,b) VALUES "t-e-t-03":(now(),20);
      """
    Then the execution should be successful
    When executing query:
      """
      MATCH (v:ttl_expire_tag03) RETURN v LIMIT 10;
      """
    Then the result should be, in any order, with relax comparison:
      | v                                               |
      | ("t-e-t-03" :ttl_expire_tag03{a: /\d+/, b: 20}) |
    And wait 3 seconds
    When executing query:
      """
      UPSERT VERTEX ON ttl_expire_tag03 "t-e-t-03" set a=now(),b=30;
      """
    Then the execution should be successful
    And wait 5 seconds
    When executing query:
      """
      MATCH (v:ttl_expire_tag03) RETURN v LIMIT 10;
      """
    Then the result should be, in any order, with relax comparison:
      | v                                               |
      | ("t-e-t-03" :ttl_expire_tag03{a: /\d+/, b: 30}) |
    And wait 5 seconds
    When executing query:
      """
      MATCH (v:ttl_expire_tag03) RETURN v LIMIT 10;
      """
    Then the result should be, in any order, with relax comparison:
      | v |
    When executing query:
      """
      UPDATE VERTEX ON ttl_expire_tag03 "t-e-t-03" set a=now(),b=30;
      """
    Then a ExecutionError should be raised at runtime: Storage Error: Vertex or edge not found.
    When executing query:
      """
      INSERT VERTEX ttl_expire_tag03(a,b) VALUES "t-e-t-04":(now(),40);
      """
    Then the execution should be successful
    When executing query:
      """
      DELETE TAG ttl_expire_tag03 FROM "t-e-t-04";
      """
    Then the execution should be successful
    When executing query:
      """
      MATCH (v:ttl_expire_tag03) RETURN v LIMIT 10;
      """
    Then the result should be, in any order, with relax comparison:
      | v |

  Scenario: ttl ddl2
    When executing query:
      """
      CREATE TAG ttl_tag06(a timestamp, b timestamp) ttl_duration=3,ttl_col="a";
      """
    Then the execution should be successful
    And wait 5 seconds
    When executing query:
      """
      INSERT VERTEX ttl_tag06(a,b) VALUES "10":(now(),now());
      """
    Then the execution should be successful
    When executing query:
      """
      ALTER TAG ttl_tag06 ttl_duration=6, ttl_col="b";
      """
    Then the execution should be successful
    And wait 4 seconds
    When executing query:
      """
      MATCH (v:ttl_tag06) return v limit 10;
      """
    Then the result should be, in any order, with relax comparison:
      | v                                     |
      | ("10" :ttl_tag06{a: /\d+/, b: /\d+/}) |
    And wait 3 seconds
    When executing query:
      """
      MATCH (v:ttl_tag06) return v limit 10;
      """
    Then the result should be, in any order, with relax comparison:
      | v |
    When executing query:
      """
      ALTER TAG ttl_tag06 drop (b);
      """
    Then the execution should be successful
    When executing query:
      """
      MATCH (v:ttl_tag06) return v limit 10;
      """
    Then the result should be, in any order, with relax comparison:
      | v |
    When executing query:
      """
      CREATE TAG ttl_tag07(a timestamp, b int) ttl_duration=3,ttl_col="a";
      """
    Then the execution should be successful
    And wait 5 seconds
    When executing query:
      """
      INSERT VERTEX ttl_tag07(a,b) VALUES "11":(now(),10);
      """
    Then the execution should be successful
    And wait 1 seconds
    When executing query:
      """
      INSERT VERTEX ttl_tag07(a,b) VALUES "12":(now(),10);
      """
    Then the execution should be successful
    When executing query:
      """
      ALTER TAG ttl_tag07 ttl_col="";
      """
    Then the execution should be successful
    When executing query:
      """
      INSERT VERTEX ttl_tag07(a,b) VALUES "13":(now(),20);
      """
    Then the execution should be successful
    And wait 2 seconds
    When executing query:
      """
      MATCH (v:ttl_tag07) return v limit 10;
      """
    Then the result should be, in any order, with relax comparison:
      | v                                  |
      | ("11":ttl_tag07{a: /\d+/,b:10})    |
      | ("12" :ttl_tag07{a: /\d+/, b: 10}) |
      | ("13" :ttl_tag07{a: /\d+/, b: 20}) |

  # # TODO: Increase the ttl expiration time (the ttl_duration value is
  # # increased or the ttl is deleted). If you do not perform a compaction
  # # before alter, the expired data will reappear.
  #
  # Then the result should be, in any order, with relax comparison:
  # | v                                  |
  # | ("12" :ttl_tag07{a: /\d+/, b: 20}) |
  # | ("13" :ttl_tag07{a: /\d+/, b: 20}) |
  Scenario: ttl multi tag
    When executing query:
      """
      CREATE TAG ttl_m_tag1(a timestamp, b int) ttl_duration=10, ttl_col="a";
      CREATE TAG ttl_m_tag2(a timestamp, b int) ttl_duration=5, ttl_col="a";
      CREATE TAG ttl_m_tag3(a timestamp, b int) ttl_duration=5, ttl_col="a";
      """
    Then the execution should be successful
    And wait 5 seconds
    When executing query:
      """
      INSERT VERTEX ttl_m_tag1(a,b) VALUES "m-t-1":(now(),10);
      INSERT VERTEX ttl_m_tag2(a,b) VALUES "m-t-1":(now(),10);
      INSERT VERTEX ttl_m_tag3(a,b) VALUES "m-t-1":(now(),10);
      """
    Then the execution should be successful
    And wait 7 seconds
    When executing query:
      """
      MATCH (v) where id(v)=="m-t-1" return v limit 10;
      """
    Then the result should be, in any order, with relax comparison:
      | v                                      |
      | ("m-t-1" :ttl_m_tag1{a: /\d+/, b: 10}) |
    And wait 4 seconds
    When executing query:
      """
      MATCH (v) where id(v)=="m-t-1" return v limit 10;
      """
    Then the result should be, in any order, with relax comparison:
      | v |

  Scenario: ttl multi edge
    When executing query:
      """
      CREATE TAG ttl_m_tag2();
      CREATE EDGE ttl_m_edge1(a timestamp, b int) ttl_duration=5, ttl_col="a";
      """
    Then the execution should be successful
    And wait 5 seconds
    When executing query:
      """
      INSERT VERTEX ttl_m_tag2() VALUES "m-e-1":(),"m-e-2":();
      INSERT EDGE ttl_m_edge1(a,b) VALUES "m-e-1"->"m-e-2"@1:(now(),10);
      """
    Then the execution should be successful
    And wait 3 seconds
    When executing query:
      """
      INSERT EDGE ttl_m_edge1(a,b) VALUES "m-e-1"->"m-e-2"@2:(now(),10);
      INSERT EDGE ttl_m_edge1(a,b) VALUES "m-e-1"->"m-e-2"@3:(now(),10);
      """
    Then the execution should be successful
    When executing query:
      """
      MATCH ()-[e:ttl_m_edge1]->() return e limit 10;
      """
    Then the result should be, in any order, with relax comparison:
      | e                                                    |
      | [:ttl_m_edge1 "m-e-1"->"m-e-2" @1 {a: /\d+/, b: 10}] |
      | [:ttl_m_edge1 "m-e-1"->"m-e-2" @2 {a: /\d+/, b: 10}] |
      | [:ttl_m_edge1 "m-e-1"->"m-e-2" @3 {a: /\d+/, b: 10}] |
    And wait 3 seconds
    When executing query:
      """
      MATCH ()-[e:ttl_m_edge1]->() return e limit 10;
      """
    Then the result should be, in any order, with relax comparison:
      | e                                                    |
      | [:ttl_m_edge1 "m-e-1"->"m-e-2" @2 {a: /\d+/, b: 10}] |
      | [:ttl_m_edge1 "m-e-1"->"m-e-2" @3 {a: /\d+/, b: 10}] |
