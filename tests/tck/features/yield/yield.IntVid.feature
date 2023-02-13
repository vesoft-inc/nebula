Feature: Yield Sentence

  Background: Prepare space
    Given a graph with space named "nba_int_vid"

  Scenario: Base
    When executing query:
      """
      YIELD 1
      """
    Then the result should be, in any order:
      | 1 |
      | 1 |
    When executing query:
      """
      YIELD [1, 1.1, 1e2, 1.1e2, .3e4, 1.e4, 1234E-10, true] AS basic_value
      """
    Then the result should be, in any order, with relax comparison:
      | basic_value                                                 |
      | [1, 1.1, 100.0, 110.0, 3000.0, 10000.0, 0.0000001234, true] |
    When executing query:
      """
      YIELD {p1: 1, p2: true, p3: "test"} AS r
      """
    Then the result should be, in any order:
      | r                             |
      | {p1: 1, p2: true, p3: "test"} |
    When executing query:
      """
      YIELD true and false
      """
    Then the result should be, in any order:
      | (true AND false) |
      | false            |
    When executing query:
      """
      YIELD datetime('2012-03-04T22:11').year
      """
    Then the result should be, in any order:
      | datetime("2012-03-04T22:11").year |
      | 2012                              |
    When executing query:
      """
      YIELD datetime('2012-03-04T22:11').not_exists
      """
    Then the result should be, in any order:
      | datetime("2012-03-04T22:11").not_exists |
      | UNKNOWN_PROP                            |
    When executing query:
      """
      YIELD CASE 1 WHEN 1 THEN 2 ELSE 3 END
      """
    Then the result should be, in any order:
      | CASE 1 WHEN 1 THEN 2 ELSE 3 END |
      | 2                               |
    When executing query:
      """
      YIELD abs(-1)
      """
    Then the result should be, in any order:
      | abs(-(1)) |
      | 1         |
    When executing query:
      """
      YIELD v.l1.p1
      """
    Then a SemanticError should be raised at runtime: Invalid label identifiers: v
    When executing query:
      """
      YIELD l1
      """
    Then a SemanticError should be raised at runtime: Invalid label identifiers: l1
    When executing query:
      """
      YIELD [i in [1, 2, 3] where i > 1 | i + 1] AS r
      """
    Then the result should be, in any order:
      | r      |
      | [3, 4] |
    When executing query:
      """
      YIELD all(n IN range(1, 5) WHERE n > 2)
      """
    Then the result should be, in any order:
      | all(n IN range(1,5) WHERE (n>2)) |
      | false                            |
    When executing query:
      """
      YIELD like._dst
      """
    Then a SemanticError should be raised at runtime: Only support input and variable in yield sentence.
    When executing query:
      """
      YIELD reduce(totalNum = 10, n IN range(1, 3) | totalNum + n) AS r
      """
    Then the result should be, in any order:
      | r  |
      | 16 |
    When executing query:
      """
      YIELD [1, 2, 3][2]
      """
    Then the result should be, in any order:
      | [1,2,3][2] |
      | 3          |
    When executing query:
      """
      YIELD [1, 2, 3][0..1]
      """
    Then the result should be, in any order:
      | [1,2,3][0..1] |
      | [1]           |
    When executing query:
      """
      YIELD prefix(edge1.prop1,"高")
      """
    Then a SyntaxError should be raised at runtime: syntax error near `(edge1.p'
    When executing query:
      """
      YIELD (INT)"1"
      """
    Then the result should be, in any order:
      | (INT)"1" |
      | 1        |
    When executing query:
      """
      YIELD -(1)
      """
    Then the result should be, in any order:
      | -(1) |
      | -1   |
    When executing query:
      """
      YIELD uuid()
      """
    Then a SemanticError should be raised at runtime: Not supported expression `uuid()' for props deduction.
    When executing query:
      """
      YIELD $v
      """
    Then a SyntaxError should be raised at runtime: Direct output of variable is prohibited near `$v'
    When executing query:
      """
      YIELD 1 AS num
      """
    Then the result should be, in any order:
      | num |
      | 1   |
    When executing query:
      """
      YIELD 1+1, (int)3.14, (string)(1+1), (string)true,"1+1"
      """
    Then the result should be, in any order:
      | (1+1) | (INT)3.14 | (STRING)(1+1) | (STRING)true | "1+1" |
      | 2     | 3         | "2"           | "true"       | "1+1" |
    When executing query:
      """
      YIELD "Hello", hash("Hello")
      """
    Then the result should be, in any order:
      | "Hello" | hash("Hello")       |
      | "Hello" | 2275118702903107253 |

  # TODO fix it
  @skip
  Scenario: Mistake
    When executing query:
      """
      YIELD count(*)
      """
    Then a SemanticError should be raised at runtime: Don't support aggregate function without input.
    When executing query:
      """
      YIELD (v)-[:like]-()
      """
    Then a SemanticError should be raised at runtime: Not support pattern expression.

  Scenario: HashCall
    When executing query:
      """
      YIELD hash("Boris")
      """
    Then the result should be, in any order:
      | hash("Boris")       |
      | 9126854228122744212 |
    When executing query:
      """
      YIELD hash(123)
      """
    Then the result should be, in any order:
      | hash(123) |
      | 123       |
    When executing query:
      """
      YIELD hash(123+456)
      """
    Then the result should be, in any order:
      | hash((123+456)) |
      | 579             |
    When executing query:
      """
      YIELD hash(123.0)
      """
    Then the result should be, in any order:
      | hash(123)            |
      | -2256853663865737834 |
    When executing query:
      """
      YIELD hash(!false)
      """
    Then the result should be, in any order:
      | hash(!(false)) |
      | 1              |

  Scenario: Logic
    When executing query:
      """
      YIELD NOT false OR false AND false XOR false
      """
    Then the result should be, in any order:
      | ((!(false) OR (false AND false)) XOR false) |
      | true                                        |
    When executing query:
      """
      YIELD (NOT false OR false) AND false XOR true
      """
    Then the result should be, in any order:
      | (((!(false) OR false) AND false) XOR true) |
      | true                                       |
    When executing query:
      """
      YIELD 2.5 % 1.2
      """
    Then the result should be, in any order:
      | (2.5%1.2)           |
      | 0.10000000000000009 |

  @skip
  Scenario: InCall
    When executing query:
      """
      YIELD udf_is_in(1,0,1,2), 123
      """
    Then the result should be, in any order:
      | udf_is_in(1,0,1,2) | 123 |
      | true               | 123 |

  Scenario: YieldPipe
    When executing query:
      """
      GO FROM hash("Boris Diaw") OVER serve YIELD $^.player.name as name, serve.start_year as start, $$.team.name as team | YIELD $-.team AS team
      """
    Then the result should be, in any order:
      | team      |
      | "Hawks"   |
      | "Hornets" |
      | "Jazz"    |
      | "Spurs"   |
      | "Suns"    |
    When executing query:
      """
      GO FROM hash("Boris Diaw") OVER serve YIELD $^.player.name as name, serve.start_year as start, $$.team.name as team | YIELD $-.team WHERE 1 == 1
      """
    Then the result should be, in any order:
      | $-.team   |
      | "Hawks"   |
      | "Hornets" |
      | "Jazz"    |
      | "Spurs"   |
      | "Suns"    |
    When executing query:
      """
      GO FROM hash("Boris Diaw") OVER serve YIELD $^.player.name as name, serve.start_year as start, $$.team.name as team | YIELD $-.team WHERE $-.start > 2005
      """
    Then the result should be, in any order:
      | $-.team   |
      | "Hornets" |
      | "Jazz"    |
      | "Spurs"   |
    When executing query:
      """
      GO FROM hash("Boris Diaw") OVER serve YIELD $^.player.name as name, serve.start_year as start, $$.team.name as team | YIELD $-.*
      """
    Then the result should be, in any order:
      | $-.name      | $-.start | $-.team   |
      | "Boris Diaw" | 2003     | "Hawks"   |
      | "Boris Diaw" | 2008     | "Hornets" |
      | "Boris Diaw" | 2016     | "Jazz"    |
      | "Boris Diaw" | 2012     | "Spurs"   |
      | "Boris Diaw" | 2005     | "Suns"    |
    When executing query:
      """
      GO FROM hash("Boris Diaw") OVER serve YIELD $^.player.name as name, serve.start_year as start, $$.team.name as team | YIELD $-.* WHERE $-.start > 2005
      """
    Then the result should be, in any order:
      | $-.name      | $-.start | $-.team   |
      | "Boris Diaw" | 2008     | "Hornets" |
      | "Boris Diaw" | 2016     | "Jazz"    |
      | "Boris Diaw" | 2012     | "Spurs"   |
    When executing query:
      """
      GO FROM hash("Boris Diaw") OVER serve YIELD $^.player.name as name, serve.start_year as start, $$.team.name as team | YIELD $-.*,hash(123) as hash WHERE $-.start > 2005
      """
    Then the result should be, in any order:
      | $-.name      | $-.start | $-.team   | hash |
      | "Boris Diaw" | 2008     | "Hornets" | 123  |
      | "Boris Diaw" | 2016     | "Jazz"    | 123  |
      | "Boris Diaw" | 2012     | "Spurs"   | 123  |

  Scenario: YieldPipeDistinct
    When executing query:
      """
      GO 2 STEPS FROM hash("Tim Duncan") OVER like YIELD like._dst AS dst | YIELD DISTINCT $-.dst AS dst
      """
    Then the result should be, in any order, and the columns 0 should be hashed:
      | dst                 |
      | "Tim Duncan"        |
      | "LaMarcus Aldridge" |
      | "Manu Ginobili"     |
    When executing query:
      """
      GO 2 STEPS FROM hash("Tim Duncan") OVER like YIELD like._dst AS dst | YIELD $-.dst AS dst
      """
    Then the result should be, in any order, and the columns 0 should be hashed:
      | dst                 |
      | "Tim Duncan"        |
      | "Tim Duncan"        |
      | "LaMarcus Aldridge" |
      | "Manu Ginobili"     |
    When executing query:
      """
      GO FROM hash("Tim Duncan") OVER like YIELD like._dst AS dst | YIELD DISTINCT $-.dst AS dst
      """
    Then the result should be, in any order, and the columns 0 should be hashed:
      | dst             |
      | "Tony Parker"   |
      | "Manu Ginobili" |

  Scenario: YieldVar
    When executing query:
      """
      $var = GO FROM hash("Boris Diaw") OVER serve YIELD $^.player.name as name, serve.start_year as start, $$.team.name as team;YIELD $var.team
      """
    Then the result should be, in any order:
      | $var.team |
      | "Hawks"   |
      | "Hornets" |
      | "Jazz"    |
      | "Spurs"   |
      | "Suns"    |
    When executing query:
      """
      $var = GO FROM hash("Boris Diaw") OVER serve YIELD $^.player.name as name, serve.start_year as start, $$.team.name as team;YIELD $var.team WHERE 1 == 1
      """
    Then the result should be, in any order:
      | $var.team |
      | "Hawks"   |
      | "Hornets" |
      | "Jazz"    |
      | "Spurs"   |
      | "Suns"    |
    When executing query:
      """
      $var = GO FROM hash("Boris Diaw") OVER serve YIELD $^.player.name as name, serve.start_year as start, $$.team.name as team;YIELD $var.team WHERE $var.start > 2005
      """
    Then the result should be, in any order:
      | $var.team |
      | "Hornets" |
      | "Jazz"    |
      | "Spurs"   |
    When executing query:
      """
      $var = GO FROM hash("Boris Diaw") OVER serve YIELD $^.player.name as name, serve.start_year as start, $$.team.name as team;YIELD $var.*
      """
    Then the result should be, in any order:
      | $var.name    | $var.start | $var.team |
      | "Boris Diaw" | 2003       | "Hawks"   |
      | "Boris Diaw" | 2008       | "Hornets" |
      | "Boris Diaw" | 2016       | "Jazz"    |
      | "Boris Diaw" | 2012       | "Spurs"   |
      | "Boris Diaw" | 2005       | "Suns"    |
    When executing query:
      """
      $var = GO FROM hash("Boris Diaw") OVER serve YIELD $^.player.name as name, serve.start_year as start, $$.team.name as team;YIELD $var.* WHERE $var.start > 2005
      """
    Then the result should be, in any order:
      | $var.name    | $var.start | $var.team |
      | "Boris Diaw" | 2008       | "Hornets" |
      | "Boris Diaw" | 2016       | "Jazz"    |
      | "Boris Diaw" | 2012       | "Spurs"   |
    When executing query:
      """
      $var = GO FROM hash("Boris Diaw") OVER serve YIELD $^.player.name as name, serve.start_year as start, $$.team.name as team;YIELD $var.*, hash(123) as hash WHERE $var.start > 2005
      """
    Then the result should be, in any order:
      | $var.name    | $var.start | $var.team | hash |
      | "Boris Diaw" | 2008       | "Hornets" | 123  |
      | "Boris Diaw" | 2016       | "Jazz"    | 123  |
      | "Boris Diaw" | 2012       | "Spurs"   | 123  |

  Scenario: YieldVarDistinct
    When executing query:
      """
      $a = GO 2 STEPS FROM hash("Tim Duncan") OVER like YIELD like._dst AS dst;YIELD DISTINCT $a.dst AS dst
      """
    Then the result should be, in any order, and the columns 0 should be hashed:
      | dst                 |
      | "Tim Duncan"        |
      | "LaMarcus Aldridge" |
      | "Manu Ginobili"     |
    When executing query:
      """
      $a = GO 2 STEPS FROM hash("Tim Duncan") OVER like YIELD like._dst AS dst;YIELD $a.dst AS dst
      """
    Then the result should be, in any order, and the columns 0 should be hashed:
      | dst                 |
      | "Tim Duncan"        |
      | "LaMarcus Aldridge" |
      | "Manu Ginobili"     |
      | "Tim Duncan"        |
    When executing query:
      """
      $a = GO FROM hash("Tim Duncan") OVER like YIELD like._dst AS dst;YIELD DISTINCT $a.dst AS dst
      """
    Then the result should be, in any order, and the columns 0 should be hashed:
      | dst             |
      | "Manu Ginobili" |
      | "Tony Parker"   |

  Scenario: Error
    When executing query:
      """
      $var = GO FROM hash("Boris Diaw") OVER serve YIELD $^.player.name AS name, serve.start_year AS start, $$.team.name AS team;YIELD $var.team WHERE $-.start > 2005
      """
    Then a SemanticError should be raised at runtime: Not support both input and variable.
    When executing query:
      """
      $var = GO FROM hash("Boris Diaw") OVER serve YIELD $^.player.name AS name, serve.start_year AS start, $$.team.name AS team;YIELD $var.team WHERE $var1.start > 2005
      """
    Then a SemanticError should be raised at runtime: Only one variable allowed to use.
    When executing query:
      """
      $var = GO FROM hash("Boris Diaw") OVER serve YIELD $^.player.name AS name, serve.start_year AS start, $$.team.name AS team;YIELD $var.abc
      """
    Then a SemanticError should be raised at runtime: `$var.abc', not exist prop `abc'
    When executing query:
      """
      $var = GO FROM hash("Boris Diaw") OVER serve YIELD $^.player.name AS name, serve.start_year AS start, $$.team.name AS team;YIELD $$.a.team
      """
    Then a ExecutionError should be raised at runtime: TagNotFound: TagName `a`
    When executing query:
      """
      $var = GO FROM hash("Boris Diaw") OVER serve YIELD $^.player.name AS name, serve.start_year AS start, $$.team.name AS team;YIELD $^.a.team
      """
    Then a ExecutionError should be raised at runtime: TagNotFound: TagName `a`
    When executing query:
      """
      $var = GO FROM hash("Boris Diaw") OVER serve YIELD $^.player.name AS name, serve.start_year AS start, $$.team.name AS team;YIELD a.team
      """
    Then a SemanticError should be raised at runtime: Invalid label identifiers: a
    When executing query:
      """
      $var = GO FROM hash("Boris Diaw") OVER like YIELD $-.abc
      """
    Then a SemanticError should be raised at runtime: `$-.abc', not exist prop `abc'

  Scenario: CalculateOverflow
    When executing query:
      """
      YIELD 9223372036854775807+1
      """
    Then a SemanticError should be raised at runtime: result of (9223372036854775807+1) cannot be represented as an integer
    When executing query:
      """
      YIELD -9223372036854775807-2
      """
    Then a SemanticError should be raised at runtime: result of (-9223372036854775807-2) cannot be represented as an integer
    When executing query:
      """
      YIELD -9223372036854775807+-2
      """
    Then a SemanticError should be raised at runtime: result of (-9223372036854775807+-2) cannot be represented as an integer
    When executing query:
      """
      YIELD 9223372036854775807*2
      """
    Then a SemanticError should be raised at runtime: result of (9223372036854775807*2) cannot be represented as an integer
    When executing query:
      """
      YIELD -9223372036854775807*-2
      """
    Then a SemanticError should be raised at runtime: result of (-9223372036854775807*-2) cannot be represented as an integer
    When executing query:
      """
      YIELD 9223372036854775807*-2
      """
    Then a SemanticError should be raised at runtime: result of (9223372036854775807*-2) cannot be represented as an integer
    When executing query:
      """
      YIELD 1/0
      """
    Then a SemanticError should be raised at runtime: Divide by 0
    When executing query:
      """
      YIELD 2%0
      """
    Then a SemanticError should be raised at runtime: Divide by 0
    When executing query:
      """
      YIELD -9223372036854775808
      """
    Then the result should be, in any order, with relax comparison:
      | -9223372036854775808 |
      | -9223372036854775808 |
    When executing query:
      """
      YIELD - -9223372036854775808
      """
    Then a SemanticError should be raised at runtime: result of -(-9223372036854775808) cannot be represented as an integer
    When executing query:
      """
      YIELD -9223372036854775809
      """
    Then a SyntaxError should be raised at runtime: Out of range: near `9223372036854775809'
    When executing query:
      """
      YIELD 9223372036854775807
      """
    Then the result should be, in any order, with relax comparison:
      | 9223372036854775807 |
      | 9223372036854775807 |
    When executing query:
      """
      YIELD -2*4611686018427387904
      """
    Then the result should be, in any order, with relax comparison:
      | (-(2)*4611686018427387904) |
      | -9223372036854775808       |

  Scenario: AggCall
    When executing query:
      """
      YIELD COUNT(1), $-.name
      """
    Then a SemanticError should be raised at runtime: `$-.name', not exist prop `name'
    When executing query:
      """
      YIELD COUNT(*), 1+1
      """
    Then the result should be, in any order:
      | COUNT(*) | (1+1) |
      | 1        | 2     |
    When executing query:
      """
      GO FROM hash("Carmelo Anthony") OVER like YIELD $$.player.age as age, like.likeness AS like | YIELD AVG($-.age),SUM($-.like),COUNT(*),1+1
      """
    Then the result should be, in any order:
      | AVG($-.age)        | SUM($-.like) | COUNT(*) | (1+1) |
      | 34.666666666666664 | 270          | 3        | 2     |
    When executing query:
      """
      GO FROM hash("Carmelo Anthony") OVER like YIELD like._dst| YIELD COUNT(*)
      """
    Then the result should be, in any order:
      | COUNT(*) |
      | 3        |
    When executing query:
      """
      GO FROM hash("Carmelo Anthony") OVER like YIELD edge as e| YIELD 1
      """
    Then the result should be, in any order:
      | 1 |
      | 1 |
      | 1 |
      | 1 |
    When executing query:
      """
      GO FROM hash("Nobody") OVER like YIELD like._dst | YIELD 1
      """
    Then the result should be, in any order:
      | 1 |
    When executing query:
      """
      $var=GO FROM hash("Carmelo Anthony") OVER like YIELD $$.player.age as age, like.likeness AS like ; YIELD AVG($var.age),SUM($var.like),COUNT(*)
      """
    Then the result should be, in any order:
      | AVG($var.age)      | SUM($var.like) | COUNT(*) |
      | 34.666666666666664 | 270            | 3        |
    When executing query:
      """
      GO FROM hash("Tim Duncan") OVER like YIELD like._dst as id, $$ as dst | YIELD $-.dst where count($-.id) > 2
      """
    Then a SyntaxError should be raised at runtime: Invalid use of aggregating function in where clause. near `count($-.id) > 2'
    When executing query:
      """
      $var = go from hash("Tim Duncan") over like yield like._dst as id, $$ as dst; yield $var.dst where count($var.id) > 2
      """
    Then a SyntaxError should be raised at runtime: Invalid use of aggregating function in where clause. near `count($var.id) > 2'

  Scenario: EmptyInput
    When executing query:
      """
      GO FROM hash("NON EXIST VERTEX ID") OVER serve YIELD $^.player.name as name, serve.start_year as start, $$.team.name as team | YIELD $-.team
      """
    Then the result should be, in any order:
      | $-.team |
    When executing query:
      """
      $var=GO FROM hash("NON EXIST VERTEX ID") OVER serve YIELD $^.player.name as name, serve.start_year as start, $$.team.name as team ; YIELD $var.team
      """
    Then the result should be, in any order:
      | $var.team |
    When executing query:
      """
      GO FROM hash("NON EXIST VERTEX ID") OVER serve YIELD $^.player.name as name, serve.start_year as start, $$.team.name as team | YIELD $-.name as name WHERE $-.start > 20000 | YIELD $-.name AS name
      """
    Then the result should be, in any order:
      | name |

  Scenario: DuplicateColumn
    When executing query:
      """
      YIELD 1,1
      """
    Then the result should be, in any order:
      | 1 | 1 |
      | 1 | 1 |
    When executing query:
      """
      GO FROM hash("Boris Diaw") OVER serve YIELD $^.player.name as name, serve.start_year as start, $$.team.name as team | YIELD $-.team
      """
    Then the result should be, in any order:
      | $-.team   |
      | "Hawks"   |
      | "Hornets" |
      | "Jazz"    |
      | "Spurs"   |
      | "Suns"    |

  Scenario: PipeYieldGo
    When executing query:
      """
      GO FROM hash("Tim Duncan") OVER serve YIELD serve._src as id | YIELD $-.id as id | GO FROM $-.id OVER serve YIELD $$.team.name AS name
      """
    Then the result should be, in any order:
      | name    |
      | "Spurs" |
    When executing query:
      """
      $var=GO FROM hash("Tim Duncan") OVER serve YIELD serve._src as id;$var2 = YIELD $var.id as id ; GO FROM $var2.id OVER serve YIELD $$.team.name AS name
      """
    Then the result should be, in any order:
      | name    |
      | "Spurs" |

  Scenario: WithComment
    When executing query:
      """
      YIELD 1- -1
      """
    Then the result should be, in any order:
      | (1--(1)) |
      | 2        |

  Scenario: deduce typecase
    When executing query:
      """
      yield split('123,456,789', ',') as l| yield [e in $-.l | (int)(e)] as c;
      """
    Then the result should be, in any order, with relax comparison:
      | c             |
      | [123,456,789] |
    When executing query:
      """
      yield [e in ['123', '456', '789'] | (int)(e)] as c;
      """
    Then the result should be, in any order, with relax comparison:
      | c             |
      | [123,456,789] |
