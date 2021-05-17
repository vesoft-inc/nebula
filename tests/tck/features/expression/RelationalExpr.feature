# Copyright (c) 2020 vesoft inc. All rights reserved.
#
# This source code is licensed under Apache 2.0 License,
# attached with Common Clause Condition 1.0, found in the LICENSES directory.
Feature: RelationalExpression

  Background:
    Given a graph with space named "nba"

  Scenario: RelationalExpression basic
    When executing query:
      """
      YIELD [1<2, 1<=1, 3>2, 2>=2, 2==2, 3!=2, 4<>3] AS int_test
      """
    Then the result should be, in order:
      | int_test                                   |
      | [true, true, true, true, true, true, true] |
    When executing query:
      """
      YIELD [1.2<2.4, 1.3<=1.300000001, 3.1>2.9, 2.3>=2.11, 2.0==2.000000009,
            3.3!=2.1, 4.2<>3.001] AS float_test
      """
    Then the result should be, in order:
      | float_test                                 |
      | [true, true, true, true, true, true, true] |
    When executing query:
      """
      YIELD ["1"<'2', "abc"<="Azz", "true">'x', "null">="NULL", "abcd"<="abcde",
             "aaa"!="aaa", "\nx"<>"\nx"] AS str_test
      """
    Then the result should be, in order:
      | str_test                                       |
      | [true, false, false, true, true, false, false] |
    When executing query:
      """
      YIELD [[1]<[2,3,4.5], [1,"a"]<=[1], [2,3,"s",true]>[3], [2.0000000001,3,"s",true]>=[2],
             [1.9999999999,3,"s",true]<>[2,3,"s",true]] AS list_test
      """
    Then the result should be, in order:
      | list_test                         |
      | [true, false, false, true, false] |
    When executing query:
      """
      YIELD [1<2.4, 1<=1.300000001, 3>2.9, 2.3>=2, 2==2.000000009, 3.3!=2, 4<>3.001, 4<=[4],
             true<>[true,true], 2.0==[1.9999999999999]] AS mixed_test
      """
    Then the result should be, in order:
      | mixed_test                                                    |
      | [true, true, true, true, true, true, true, NULL, true, false] |
    When executing query:
      """
      YIELD ["10"<2.4, '1'<=1.300000001, 3>"2.9", "2.3">=true, true==2.000000009, false!=2,
             [1,3]<>3.001] AS non_numeric_test
      """
    Then the result should be, in order:
      | non_numeric_test                            |
      | [NULL, NULL, NULL, NULL, false, true, true] |
    When executing query:
      """
      YIELD ["10"<null, null<=1.300000001, 3>null, null>=true, null==null, null!=null, null<>false,
             null<[2,3,null], [2,null]==[2,null], [2,null]<>[2,null,1]] AS null_test
      """
    Then the result should be, in order:
      | null_test                                                    |
      | [NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, true, true] |
    When executing query:
      """
      YIELD [1 in [1,2], 1 in [2,3], [1] in [2,[1]],
             null in [1,2], null in [null], null in [1,null],
             1 in [1,null], 1 in [null,2],
             [1,null] in [1,null], [1,null] in [1], [1,null] in [null,[1,null]]] AS in_test
      """
    Then the result should be, in order:
      | in_test                                                              |
      | [true, false, true, NULL, NULL, NULL, true, NULL, NULL, false, true] |
    When executing query:
      """
      YIELD [1 not in [1,2], 1 not in [2,3], [1] not in [2,[1]],
             null not in [1,2], null not in [null], null not in [1,null],
             1 not in [1,null], 1 not in [null,2],
             [1,null] not in [1,null], [1,null] not in [1], [1,null] not in [null,[1,null]]] AS not_in_test
      """
    Then the result should be, in order:
      | not_in_test                                                            |
      | [false, true, false, NULL, NULL, NULL, false, NULL, NULL, true, false] |

  Scenario: Using Relational comparison in GO clause
    When executing query:
      """
      GO FROM "Tony Parker" OVER like
      WHERE $$.player.age >= 33.000000000010 OR like.likeness <> 90.0000000000001
      YIELD like._dst AS id, like.likeness AS likeness, $$.player.age AS age
      """
    Then the result should be, in any order, with relax comparison:
      | id                  | likeness | age |
      | "LaMarcus Aldridge" | 90       | 33  |
      | "Manu Ginobili"     | 95       | 41  |
      | "Tim Duncan"        | 95       | 42  |
    When executing query:
      """
      GO FROM "Tony Parker" OVER like
      WHERE $$.player.age >= 33.000000000010 AND like.likeness == 90.0000000000001
      YIELD like._dst AS id, like.likeness AS likeness, $$.player.age AS age
      """
    Then the result should be, in any order:
      | id                  | likeness | age |
      | "LaMarcus Aldridge" | 90       | 33  |
    When executing query:
      """
      GO FROM "Tony Parker" OVER like
      WHERE $$.player.age >= true OR like.likeness <> null OR $$.player.name<[2,3,4] OR $$.player.name > 23.3
      YIELD like._dst AS id, like.likeness AS likeness, $$.player.age AS age
      """
    Then the result should be, in any order, with relax comparison:
      | id | likeness | age |

  Scenario: Using Relational comparison in MATCH clause
    When executing query:
      """
      MATCH p = (n:player)<-[e:like]-(m)
      WHERE n.age >= 33 OR n.start_year <= 2010.0
            OR e.likeness <> 90 OR n.nonExistTag <> null
            OR e.likeness >= "12" OR n.age <= true
      RETURN DISTINCT m.name AS player, m.age AS age
             ORDER BY player, age
      """
    Then the result should be, in any order, with relax comparison:
      | player              | age |
      | "Amar'e Stoudemire" | 36  |
      | "Aron Baynes"       | 32  |
      | "Ben Simmons"       | 22  |
      | "Blake Griffin"     | 30  |
      | "Boris Diaw"        | 36  |
      | "Carmelo Anthony"   | 34  |
      | "Chris Paul"        | 33  |
      | "Damian Lillard"    | 28  |
      | "Danny Green"       | 31  |
      | "Dejounte Murray"   | 29  |
      | "Dirk Nowitzki"     | 40  |
      | "Dwyane Wade"       | 37  |
      | "Grant Hill"        | 46  |
      | "James Harden"      | 29  |
      | "Jason Kidd"        | 45  |
      | "Joel Embiid"       | 25  |
      | "Kyrie Irving"      | 26  |
      | "LaMarcus Aldridge" | 33  |
      | "LeBron James"      | 34  |
      | "Luka Doncic"       | 20  |
      | "Manu Ginobili"     | 41  |
      | "Marc Gasol"        | 34  |
      | "Marco Belinelli"   | 32  |
      | "Paul Gasol"        | 38  |
      | "Paul George"       | 28  |
      | "Rajon Rondo"       | 33  |
      | "Ray Allen"         | 43  |
      | "Rudy Gay"          | 32  |
      | "Shaquile O'Neal"   | 47  |
      | "Steve Nash"        | 45  |
      | "Tiago Splitter"    | 34  |
      | "Tim Duncan"        | 42  |
      | "Tony Parker"       | 36  |
      | "Tracy McGrady"     | 39  |
      | "Vince Carter"      | 42  |
      | "Yao Ming"          | 38  |
    When executing query:
      """
      MATCH p = (n:player)<-[e:like]-(m)
      WHERE n.age >= 33 OR n.name <= "2010.0"
            AND e.likeness <> 90 OR n.nonExistTag <> null
            OR e.likeness >= "12" OR n.age <= true
      RETURN DISTINCT m.name AS player, m.age AS age
             ORDER BY player, age
      """
    Then the result should be, in any order, with relax comparison:
      | player              | age |
      | "Amar'e Stoudemire" | 36  |
      | "Aron Baynes"       | 32  |
      | "Blake Griffin"     | 30  |
      | "Boris Diaw"        | 36  |
      | "Carmelo Anthony"   | 34  |
      | "Chris Paul"        | 33  |
      | "Damian Lillard"    | 28  |
      | "Danny Green"       | 31  |
      | "Dejounte Murray"   | 29  |
      | "Dirk Nowitzki"     | 40  |
      | "Dwyane Wade"       | 37  |
      | "Grant Hill"        | 46  |
      | "Jason Kidd"        | 45  |
      | "Kyrie Irving"      | 26  |
      | "LaMarcus Aldridge" | 33  |
      | "LeBron James"      | 34  |
      | "Luka Doncic"       | 20  |
      | "Manu Ginobili"     | 41  |
      | "Marc Gasol"        | 34  |
      | "Marco Belinelli"   | 32  |
      | "Paul Gasol"        | 38  |
      | "Rajon Rondo"       | 33  |
      | "Ray Allen"         | 43  |
      | "Rudy Gay"          | 32  |
      | "Shaquile O'Neal"   | 47  |
      | "Steve Nash"        | 45  |
      | "Tiago Splitter"    | 34  |
      | "Tim Duncan"        | 42  |
      | "Tony Parker"       | 36  |
      | "Tracy McGrady"     | 39  |
      | "Vince Carter"      | 42  |
      | "Yao Ming"          | 38  |
    When executing query:
      """
      MATCH p = (n:player)<-[e:like]-(m)
      WHERE n.age >= 33 AND n.name <> "2010.0"
            AND e.likeness == 90 AND n.nonExistTag <> null
            AND e.likeness >= "12"
      RETURN n.name AS player, n.age AS age
      """
    Then the result should be, in any order, with relax comparison:
      | player | age |

  Scenario: Transform Relational expr in MATCH clause
    When profiling query:
      """
      MATCH (v:player) WHERE v.age - 5 >= 40 RETURN v
      """
    Then the result should be, in any order:
      | v                                                             |
      | ("Jason Kidd" :player{age: 45, name: "Jason Kidd"})           |
      | ("Grant Hill" :player{age: 46, name: "Grant Hill"})           |
      | ("Shaquile O'Neal" :player{age: 47, name: "Shaquile O'Neal"}) |
      | ("Steve Nash" :player{age: 45, name: "Steve Nash"})           |
    And the execution plan should be:
      | id | name        | dependencies | operator info                                      |
      | 10 | Project     | 13           |                                                    |
      | 13 | Filter      | 7            |                                                    |
      | 7  | Project     | 6            |                                                    |
      | 6  | Project     | 5            |                                                    |
      | 5  | Filter      | 13           |                                                    |
      | 15 | GetVertices | 11           |                                                    |
      | 11 | IndexScan   | 0            | {"indexCtx": {"columnHints":{"scanType":"RANGE"}}} |
      | 0  | Start       |              |                                                    |
