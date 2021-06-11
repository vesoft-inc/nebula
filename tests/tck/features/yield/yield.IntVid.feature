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
    Then a ExecutionError should be raised at runtime: TagName `a'  is nonexistent
    When executing query:
      """
      $var = GO FROM hash("Boris Diaw") OVER serve YIELD $^.player.name AS name, serve.start_year AS start, $$.team.name AS team;YIELD $^.a.team
      """
    Then a ExecutionError should be raised at runtime: TagName `a'  is nonexistent
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
    Then a ExecutionError should be raised at runtime: result of (9223372036854775807+1) cannot be represented as an integer
    When executing query:
      """
      YIELD -9223372036854775807-2
      """
    Then a ExecutionError should be raised at runtime: result of (-9223372036854775807-2) cannot be represented as an integer
    When executing query:
      """
      YIELD -9223372036854775807+-2
      """
    Then a ExecutionError should be raised at runtime: result of (-9223372036854775807+-2) cannot be represented as an integer
    When executing query:
      """
      YIELD 9223372036854775807*2
      """
    Then a ExecutionError should be raised at runtime: result of (9223372036854775807*2) cannot be represented as an integer
    When executing query:
      """
      YIELD -9223372036854775807*-2
      """
    Then a ExecutionError should be raised at runtime: result of (-9223372036854775807*-2) cannot be represented as an integer
    When executing query:
      """
      YIELD 9223372036854775807*-2
      """
    Then a ExecutionError should be raised at runtime: result of (9223372036854775807*-2) cannot be represented as an integer
    When executing query:
      """
      YIELD 1/0
      """
    Then a ExecutionError should be raised at runtime: / by zero
    When executing query:
      """
      YIELD 2%0
      """
    Then a ExecutionError should be raised at runtime: / by zero
    When executing query:
      """
      YIELD -9223372036854775808
      """
    Then the result should be, in any order, with relax comparison:
      | -9223372036854775808 |
      | -9223372036854775808 |
    When executing query:
      """
      YIELD --9223372036854775808
      """
    Then a ExecutionError should be raised at runtime: result of -(-9223372036854775808) cannot be represented as an integer
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
      GO FROM hash("Carmelo Anthony") OVER like | YIELD COUNT(*)
      """
    Then the result should be, in any order:
      | COUNT(*) |
      | 3        |
    When executing query:
      """
      GO FROM hash("Carmelo Anthony") OVER like | YIELD 1
      """
    Then the result should be, in any order:
      | 1 |
      | 1 |
      | 1 |
      | 1 |
    When executing query:
      """
      GO FROM hash("Nobody") OVER like | YIELD 1
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
      YIELD 1--1
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
