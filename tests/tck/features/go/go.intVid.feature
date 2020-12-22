@skip
Feature: IntegerVid Go  Sentence

  Background: Prepare space
    Given a graph with space named "nba_int_vid"

  Scenario: Integer Vid one step
    When executing query:
      """
      GO FROM hash("Tim Duncan") OVER serve
      """
    Then the result should be, in any order, with relax comparison:
      | serve._dst    |
      | hash("Spurs") |
    When executing query:
      """
      GO FROM hash("Tim Duncan"), hash("Tim Duncan") OVER serve
      """
    Then the result should be, in any order, with relax comparison:
      | serve._dst    |
      | hash("Spurs") |
      | hash("Spurs") |
    When executing query:
      """
      YIELD hash("Tim Duncan") as vid | GO FROM $-.vid OVER serve
      """
    Then the result should be, in any order, with relax comparison:
      | serve._dst    |
      | hash("Spurs") |
    When executing query:
      """
      GO FROM hash("Boris Diaw") OVER serve YIELD $^.player.name, serve.start_year, serve.end_year, $$.team.name
      """
    Then the result should be, in any order, with relax comparison:
      | $^.player.name | serve.start_year | serve.end_year | $$.team.name |
      | "Boris Diaw"   | 2003             | 2005           | "Hawks"      |
      | "Boris Diaw"   | 2005             | 2008           | "Suns"       |
      | "Boris Diaw"   | 2008             | 2012           | "Hornets"    |
      | "Boris Diaw"   | 2012             | 2016           | "Spurs"      |
      | "Boris Diaw"   | 2016             | 2017           | "Jazz"       |
    When executing query:
      """
      GO FROM hash("Rajon Rondo") OVER serve WHERE serve.start_year >= 2013 AND serve.end_year <= 2018
      YIELD $^.player.name, serve.start_year, serve.end_year, $$.team.name
      """
    Then the result should be, in any order, with relax comparison:
      | $^.player.name | serve.start_year | serve.end_year | $$.team.name |
      | "Rajon Rondo"  | 2014             | 2015           | "Mavericks"  |
      | "Rajon Rondo"  | 2015             | 2016           | "Kings"      |
      | "Rajon Rondo"  | 2016             | 2017           | "Bulls"      |
      | "Rajon Rondo"  | 2017             | 2018           | "Pelicans"   |
    When executing query:
      """
      GO FROM hash("Boris Diaw") OVER like YIELD like._dst as id
      | GO FROM $-.id OVER like YIELD like._dst as id | GO FROM $-.id OVER serve
      """
    Then the result should be, in any order, with relax comparison:
      | serve._dst            |
      | hash("Spurs")         |
      | hash("Spurs")         |
      | hash("Spurs")         |
      | hash("Spurs")         |
      | hash("Spurs")         |
      | hash("Hornets")       |
      | hash("Trail Blazers") |
    When executing query:
      """
      GO FROM hash('Thunders') OVER serve REVERSELY
      """
    Then the result should be, in any order, with relax comparison:
      | serve._dst                |
      | hash("Russell Westbrook") |
      | hash("Kevin Durant")      |
      | hash("James Harden")      |
      | hash("Carmelo Anthony")   |
      | hash("Paul George")       |
      | hash("Ray Allen")         |
    When executing query:
      """
      GO FROM hash("Boris Diaw") OVER like YIELD like._dst as id
      |(GO FROM $-.id OVER like YIELD like._dst as id | GO FROM $-.id OVER serve)
      """
    Then the result should be, in any order, with relax comparison:
      | serve._dst            |
      | hash("Spurs")         |
      | hash("Spurs")         |
      | hash("Spurs")         |
      | hash("Spurs")         |
      | hash("Spurs")         |
      | hash("Hornets")       |
      | hash("Trail Blazers") |
    When executing query:
      """
      GO FROM hash("No Exist Vertex Id") OVER like YIELD like._dst as id
      |(GO FROM $-.id OVER like YIELD like._dst as id | GO FROM $-.id OVER serve)
      """
    Then the result should be, in any order, with relax comparison:
      | serve._dst |
    When executing query:
      """
      GO FROM hash("Boris Diaw") OVER like, serve YIELD like._dst as id
      |(GO FROM $-.id OVER like YIELD like._dst as id | GO FROM $-.id OVER serve)
      """
    Then the result should be, in any order, with relax comparison:
      | serve._dst            |
      | hash("Spurs")         |
      | hash("Spurs")         |
      | hash("Spurs")         |
      | hash("Spurs")         |
      | hash("Spurs")         |
      | hash("Hornets")       |
      | hash("Trail Blazers") |

  Scenario: Integer Vid assignment simple
    When executing query:
      """
      $var = GO FROM hash("Tracy McGrady") OVER like YIELD like._dst as id; GO FROM $var.id OVER like
      """
    Then the result should be, in any order, with relax comparison:
      | like._dst                 |
      | hash("Tracy McGrady")     |
      | hash("LaMarcus Aldridge") |

  Scenario: Integer Vid assignment pipe
    When executing query:
      """
      $var = (GO FROM hash("Tracy McGrady") OVER like YIELD like._dst as id | GO FROM $-.id OVER like YIELD like._dst as id);
      GO FROM $var.id OVER like
      """
    Then the result should be, in any order, with relax comparison:
      | like._dst           |
      | hash("Kobe Bryant") |
      | hash("Grant Hill")  |
      | hash("Rudy Gay")    |
      | hash("Tony Parker") |
      | hash("Tim Duncan")  |

  Scenario: Integer Vid pipe only yield input prop
    When executing query:
      """
      GO FROM hash("Tracy McGrady") OVER like YIELD like._dst as vid | GO FROM $-.vid OVER like YIELD $-.vid as id
      """
    Then the result should be, in any order, with relax comparison:
      | id                  |
      | hash("Kobe Bryant") |
      | hash("Grant Hill")  |
      | hash("Rudy Gay")    |

  Scenario: Integer Vid pipe only yield constant
    When executing query:
      """
      GO FROM hash("Tracy McGrady") OVER like YIELD like._dst as vid | GO FROM $-.vid OVER like YIELD 3
      """
    Then the result should be, in any order, with relax comparison:
      | 3 |
      | 3 |
      | 3 |
      | 3 |

  Scenario: Integer Vid assignment empty result
    When executing query:
      """
      $var = GO FROM -1 OVER like YIELD like._dst as id; GO FROM $var.id OVER like
      """
    Then the result should be, in any order, with relax comparison:
      | like._dst |

  Scenario: Integer Vid variable undefined
    When executing query:
      """
      GO FROM $var OVER like
      """
    Then a SyntaxError should be raised at runtime: syntax error near `OVER'

  Scenario: Integer Vid distinct
    When executing query:
      """
      GO FROM hash("Nobody") OVER serve YIELD DISTINCT $^.player.name as name, $$.team.name as name
      """
    Then the result should be, in any order, with relax comparison:
      | name | name |
    When executing query:
      """
      GO FROM hash("Boris Diaw") OVER like YIELD like._dst as id
      | GO FROM $-.id OVER like YIELD like._dst as id
      | GO FROM $-.id OVER serve
      YIELD DISTINCT serve._dst, $$.team.name
      """
    Then the result should be, in any order, with relax comparison:
      | serve._dst            | $$.team.name    |
      | hash("Spurs")         | "Spurs"         |
      | hash("Hornets")       | "Hornets"       |
      | hash("Trail Blazers") | "Trail Blazers" |
    When executing query:
      """
      GO 2 STEPS FROM hash("Tony Parker") OVER like YIELD DISTINCT like._dst
      """
    Then the result should be, in any order, with relax comparison:
      | like._dst             |
      | hash("Tim Duncan")    |
      | hash("Tony Parker")   |
      | hash("Manu Ginobili") |

  Scenario: Integer Vid vertex noexist
    When executing query:
      """
      GO FROM hash("NON EXIST VERTEX ID") OVER serve
      """
    Then the result should be, in any order, with relax comparison:
      | serve._dst |
    When executing query:
      """
      GO FROM hash("NON EXIST VERTEX ID") OVER serve YIELD $^.player.name, serve.start_year, serve.end_year, $$.team.name
      """
    Then the result should be, in any order, with relax comparison:
      | $^.player.name | serve.start_year | serve.end_year | $$.team.name |
    When executing query:
      """
      GO FROM hash("NON EXIST VERTEX ID") OVER serve YIELD DISTINCT $^.player.name, serve.start_year, serve.end_year, $$.team.name
      """
    Then the result should be, in any order, with relax comparison:
      | $^.player.name | serve.start_year | serve.end_year | $$.team.name |

  Scenario: Integer Vid empty input
    When executing query:
      """
      GO FROM hash("NON EXIST VERTEX ID") OVER like YIELD like._dst as id
      | GO FROM $-.id OVER like YIELD like._dst as id
      | GO FROM $-.id OVER serve
      """
    Then the result should be, in any order, with relax comparison:
      | serve._dst |
    When executing query:
      """
      GO FROM hash("NON EXIST VERTEX ID") OVER serve YIELD serve._dst as id, serve.start_year as start
      | YIELD $-.id as id WHERE $-.start > 20000
      | Go FROM $-.id over serve
      """
    Then the result should be, in any order, with relax comparison:
      | serve._dst |

  Scenario: Integer Vid multi edges over all
    When executing query:
      """
      GO FROM hash("Russell Westbrook") OVER * REVERSELY YIELD serve._dst, like._dst
      """
    Then the result should be, in any order, with relax comparison:
      | serve._dst | like._dst               |
      | EMPTY      | hash("James Harden")    |
      | EMPTY      | hash("Dejounte Murray") |
      | EMPTY      | hash("Paul George")     |
    When executing query:
      """
      GO FROM hash("Russell Westbrook") OVER * REVERSELY YIELD serve._src, like._src
      """
    Then the result should be, in any order, with relax comparison:
      | serve._src | like._src                 |
      | EMPTY      | hash("Russell Westbrook") |
      | EMPTY      | hash("Russell Westbrook") |
      | EMPTY      | hash("Russell Westbrook") |
    When executing query:
      """
      GO FROM hash("Russell Westbrook") OVER * REVERSELY
      """
    Then the result should be, in any order, with relax comparison:
      | like._dst               | serve._dst | teammate._dst |
      | hash("James Harden")    | EMPTY      | EMPTY         |
      | hash("Dejounte Murray") | EMPTY      | EMPTY         |
      | hash("Paul George")     | EMPTY      | EMPTY         |
    When executing query:
      """
      GO FROM hash("Dirk Nowitzki") OVER * YIELD serve._dst, like._dst
      """
    Then the result should be, in any order, with relax comparison:
      | serve._dst        | like._dst           |
      | hash("Mavericks") | EMPTY               |
      | EMPTY             | hash("Steve Nash")  |
      | EMPTY             | hash("Jason Kidd")  |
      | EMPTY             | hash("Dwyane Wade") |
    When executing query:
      """
      GO FROM hash("Paul Gasol") OVER *
      """
    Then the result should be, in any order, with relax comparison:
      | like._dst           | serve._dst        | teammate._dst |
      | EMPTY               | hash("Grizzlies") | EMPTY         |
      | EMPTY               | hash("Lakers")    | EMPTY         |
      | EMPTY               | hash("Bulls")     | EMPTY         |
      | EMPTY               | hash("Spurs")     | EMPTY         |
      | EMPTY               | hash("Bucks")     | EMPTY         |
      | hash("Kobe Bryant") | EMPTY             | EMPTY         |
      | hash("Marc Gasol")  | EMPTY             | EMPTY         |
    When executing query:
      """
      GO FROM hash("Manu Ginobili") OVER * REVERSELY YIELD like.likeness, teammate.start_year, serve.start_year, $$.player.name
      """
    Then the result should be, in any order, with relax comparison:
      | like.likeness | teammate.start_year | serve.start_year | $$.player.name    |
      | 95            | EMPTY               | EMPTY            | "Tim Duncan"      |
      | 95            | EMPTY               | EMPTY            | "Tony Parker"     |
      | 90            | EMPTY               | EMPTY            | "Tiago Splitter"  |
      | 99            | EMPTY               | EMPTY            | "Dejounte Murray" |
      | EMPTY         | 2002                | EMPTY            | "Tim Duncan"      |
      | EMPTY         | 2002                | EMPTY            | "Tony Parker"     |
    When executing query:
      """
      GO FROM hash("LaMarcus Aldridge") OVER * YIELD $$.team.name, $$.player.name
      """
    Then the result should be, in any order, with relax comparison:
      | $$.team.name    | $$.player.name |
      | "Trail Blazers" | EMPTY          |
      | EMPTY           | "Tim Duncan"   |
      | EMPTY           | "Tony Parker"  |
      | "Spurs"         | EMPTY          |
    When executing query:
      """
      GO FROM hash("Boris Diaw") OVER * YIELD like._dst as id
      | ( GO FROM $-.id OVER like YIELD like._dst as id | GO FROM $-.id OVER serve )
      """
    Then the result should be, in any order, with relax comparison:
      | serve._dst            |
      | hash("Spurs")         |
      | hash("Spurs")         |
      | hash("Spurs")         |
      | hash("Spurs")         |
      | hash("Spurs")         |
      | hash("Hornets")       |
      | hash("Trail Blazers") |

  @skip
  Scenario: Integer Vid edge type
    When executing query:
      """
      YIELD serve.start_year, like.likeness, serve._type, like._type
      """
    Then the result should be, in any order, with relax comparison:
      | serve.start_year | like.likeness | serve._type | like._type |
      | 2008             | EMPTY         | 6           | EMPTY      |
      | EMPTY            | 90            | EMPTY       | 5          |
      | EMPTY            | 90            | EMPTY       | 5          |
    When executing query:
      """
      GO FROM hash("Russell Westbrook") OVER serve, like REVERSELY
      YIELD serve._dst, like._dst, serve._type, like._type
      """
    Then the result should be, in any order, with relax comparison:
      | serve._dst | like._dst               | serve._type | like._type |
      | EMPTY      | hash("James Harden")    | EMPTY       | -5         |
      | EMPTY      | hash("Dejounte Murray") | EMPTY       | -5         |
      | EMPTY      | hash("Paul George")     | EMPTY       | -5         |

  Scenario: Integer Vid multi edges
    When executing query:
      """
      GO FROM hash("Russell Westbrook") OVER serve, like  YIELD serve.start_year, like.likeness
      """
    Then the result should be, in any order, with relax comparison:
      | serve.start_year | like.likeness |
      | 2008             | EMPTY         |
      | EMPTY            | 90            |
      | EMPTY            | 90            |
    When executing query:
      """
      GO FROM hash("Shaquile O\'Neal") OVER serve, like
      """
    Then the result should be, in any order, with relax comparison:
      | serve._dst        | like._dst            |
      | hash("Magic")     | EMPTY                |
      | hash("Lakers")    | EMPTY                |
      | hash("Heat")      | EMPTY                |
      | hash("Suns")      | EMPTY                |
      | hash("Cavaliers") | EMPTY                |
      | hash("Celtics")   | EMPTY                |
      | EMPTY             | hash("JaVale McGee") |
      | EMPTY             | hash("Tim Duncan")   |
    When executing query:
      """
      GO FROM hash('Russell Westbrook') OVER serve, like
      """
    Then the result should be, in any order, with relax comparison:
      | serve._dst       | like._dst            |
      | hash("Thunders") | EMPTY                |
      | EMPTY            | hash("Paul George")  |
      | EMPTY            | hash("James Harden") |
    When executing query:
      """
      GO FROM hash("Russell Westbrook") OVER serve, like REVERSELY
      YIELD serve._dst, like._dst, serve.start_year, like.likeness
      """
    Then the result should be, in any order, with relax comparison:
      | serve._dst | like._dst               | serve.start_year | like.likeness |
      | EMPTY      | hash("James Harden")    | EMPTY            | 80            |
      | EMPTY      | hash("Dejounte Murray") | EMPTY            | 99            |
      | EMPTY      | hash("Paul George")     | EMPTY            | 95            |
    When executing query:
      """
      GO FROM hash("Russell Westbrook") OVER serve, like REVERSELY YIELD serve._src, like._src
      """
    Then the result should be, in any order, with relax comparison:
      | serve._src | like._src                 |
      | EMPTY      | hash("Russell Westbrook") |
      | EMPTY      | hash("Russell Westbrook") |
      | EMPTY      | hash("Russell Westbrook") |
    When executing query:
      """
      GO FROM hash("Russell Westbrook") OVER serve, like REVERSELY
      """
    Then the result should be, in any order, with relax comparison:
      | serve._dst | like._dst               |
      | EMPTY      | hash("James Harden")    |
      | EMPTY      | hash("Dejounte Murray") |
      | EMPTY      | hash("Paul George")     |
    When executing query:
      """
      GO FROM hash("Manu Ginobili") OVER like, teammate REVERSELY YIELD like.likeness, teammate.start_year, $$.player.name
      """
    Then the result should be, in any order, with relax comparison:
      | like.likeness | teammate.start_year | $$.player.name    |
      | 95            | EMPTY               | "Tim Duncan"      |
      | 95            | EMPTY               | "Tony Parker"     |
      | 90            | EMPTY               | "Tiago Splitter"  |
      | 99            | EMPTY               | "Dejounte Murray" |
      | EMPTY         | 2002                | "Tim Duncan"      |
      | EMPTY         | 2002                | "Tony Parker"     |

  Scenario: Integer Vid multi edges with filter
    When executing query:
      """
      GO FROM hash("Russell Westbrook") OVER serve, like WHERE serve.start_year > 2000
      YIELD serve.start_year, like.likeness
      """
    Then the result should be, in any order, with relax comparison:
      | serve.start_year | like.likeness |
      | 2008             | EMPTY         |
    When executing query:
      """
      GO FROM hash("Manu Ginobili") OVER like, teammate REVERSELY WHERE like.likeness > 90
      YIELD like.likeness, teammate.start_year, $$.player.name
      """
    Then the result should be, in any order, with relax comparison:
      | like.likeness | teammate.start_year | $$.player.name    |
      | 95            | EMPTY               | "Tim Duncan"      |
      | 95            | EMPTY               | "Tony Parker"     |
      | 99            | EMPTY               | "Dejounte Murray" |
    When executing query:
      """
      GO FROM hash("Manu Ginobili") OVER * WHERE $$.player.age > 30 or $$.team.name not starts with "Rockets"
      YIELD DISTINCT $$.player.age, $$.player.name, $$.team.name
      """
    Then the result should be, in any order, with relax comparison:
      | $$.player.age | $$.player.name | $$.team.name |
      | EMPTY         | EMPTY          | "Spurs"      |
      | 42            | "Tim Duncan"   | EMPTY        |
      | 36            | "Tony Parker"  | EMPTY        |
    When executing query:
      """
      GO FROM hash("Manu Ginobili") OVER like, teammate REVERSELY WHERE $$.player.age > 30 and $$.player.age < 40
      YIELD DISTINCT $$.player.age, $$.player.name
      """
    Then the result should be, in any order, with relax comparison:
      | $$.player.age | $$.player.name   |
      | 34            | "Tiago Splitter" |
      | 36            | "Tony Parker"    |

  Scenario: Integer Vid reference pipe in yield and where
    When executing query:
      """
      GO FROM hash('Tim Duncan'), hash('Chris Paul') OVER like YIELD $^.player.name AS name, like._dst AS id
      | GO FROM $-.id OVER like YIELD $-.name, $^.player.name, $$.player.name
      """
    Then the result should be, in any order, with relax comparison:
      | $-.name      | $^.player.name    | $$.player.name      |
      | "Tim Duncan" | "Manu Ginobili"   | "Tim Duncan"        |
      | "Tim Duncan" | "Tony Parker"     | "LaMarcus Aldridge" |
      | "Tim Duncan" | "Tony Parker"     | "Manu Ginobili"     |
      | "Tim Duncan" | "Tony Parker"     | "Tim Duncan"        |
      | "Chris Paul" | "LeBron James"    | "Ray Allen"         |
      | "Chris Paul" | "Carmelo Anthony" | "Chris Paul"        |
      | "Chris Paul" | "Carmelo Anthony" | "LeBron James"      |
      | "Chris Paul" | "Carmelo Anthony" | "Dwyane Wade"       |
      | "Chris Paul" | "Dwyane Wade"     | "Chris Paul"        |
      | "Chris Paul" | "Dwyane Wade"     | "LeBron James"      |
      | "Chris Paul" | "Dwyane Wade"     | "Carmelo Anthony"   |
    When executing query:
      """
      GO FROM hash('Tim Duncan'), hash('Chris Paul') OVER like YIELD $^.player.name AS name, like._dst AS id
      | GO FROM $-.id OVER like WHERE $-.name != $$.player.name YIELD $-.name, $^.player.name, $$.player.name
      """
    Then the result should be, in any order, with relax comparison:
      | $-.name      | $^.player.name    | $$.player.name      |
      | "Tim Duncan" | "Tony Parker"     | "LaMarcus Aldridge" |
      | "Tim Duncan" | "Tony Parker"     | "Manu Ginobili"     |
      | "Chris Paul" | "LeBron James"    | "Ray Allen"         |
      | "Chris Paul" | "Carmelo Anthony" | "LeBron James"      |
      | "Chris Paul" | "Carmelo Anthony" | "Dwyane Wade"       |
      | "Chris Paul" | "Dwyane Wade"     | "LeBron James"      |
      | "Chris Paul" | "Dwyane Wade"     | "Carmelo Anthony"   |

  @skip
  Scenario: Integer Vid all prop(reson = $-.* over * $var.* not implement)
    When executing query:
      """
      GO FROM hash('Tim Duncan'), hash('Chris Paul') OVER like YIELD $^.player.name AS name, like._dst AS id
      | GO FROM $-.id OVER like YIELD $-.*, $^.player.name, $$.player.name
      """
    Then the result should be, in any order, with relax comparison:
      | $-.*         | $^.player.name    | $$.player.name      |
      | "Tim Duncan" | "Manu Ginobili"   | "Tim Duncan"        |
      | "Tim Duncan" | "Tony Parker"     | "LaMarcus Aldridge" |
      | "Tim Duncan" | "Tony Parker"     | "Manu Ginobili"     |
      | "Tim Duncan" | "Tony Parker"     | "Tim Duncan"        |
      | "Chris Paul" | "LeBron James"    | "Ray Allen"         |
      | "Chris Paul" | "Carmelo Anthony" | "Chris Paul"        |
      | "Chris Paul" | "Carmelo Anthony" | "LeBron James"      |
      | "Chris Paul" | "Carmelo Anthony" | "Dwyane Wade"       |
      | "Chris Paul" | "Dwyane Wade"     | "Chris Paul"        |
      | "Chris Paul" | "Dwyane Wade"     | "LeBron James"      |
      | "Chris Paul" | "Dwyane Wade"     | "Carmelo Anthony"   |
    When executing query:
      """
      $var = GO FROM hash('Tim Duncan'), hash('Chris Paul') OVER like YIELD $^.player.name AS name, like._dst AS id;
      GO FROM $var.id OVER like YIELD $var.*, $^.player.name, $$.player.name
      """
    Then the result should be, in any order, with relax comparison:
      | $var.*       | $^.player.name    | $$.player.name      |
      | "Tim Duncan" | "Manu Ginobili"   | "Tim Duncan"        |
      | "Tim Duncan" | "Tony Parker"     | "LaMarcus Aldridge" |
      | "Tim Duncan" | "Tony Parker"     | "Manu Ginobili"     |
      | "Tim Duncan" | "Tony Parker"     | "Tim Duncan"        |
      | "Chris Paul" | "LeBron James"    | "Ray Allen"         |
      | "Chris Paul" | "Carmelo Anthony" | "Chris Paul"        |
      | "Chris Paul" | "Carmelo Anthony" | "LeBron James"      |
      | "Chris Paul" | "Carmelo Anthony" | "Dwyane Wade"       |
      | "Chris Paul" | "Dwyane Wade"     | "Chris Paul"        |
      | "Chris Paul" | "Dwyane Wade"     | "LeBron James"      |
      | "Chris Paul" | "Dwyane Wade"     | "Carmelo Anthony"   |

  Scenario: Integer Vid reference variable in yield and where
    When executing query:
      """
      $var = GO FROM hash('Tim Duncan'), hash('Chris Paul') OVER like YIELD $^.player.name AS name, like._dst AS id;
      GO FROM $var.id OVER like YIELD $var.name, $^.player.name, $$.player.name
      """
    Then the result should be, in any order, with relax comparison:
      | $var.name    | $^.player.name    | $$.player.name      |
      | "Tim Duncan" | "Manu Ginobili"   | "Tim Duncan"        |
      | "Tim Duncan" | "Tony Parker"     | "LaMarcus Aldridge" |
      | "Tim Duncan" | "Tony Parker"     | "Manu Ginobili"     |
      | "Tim Duncan" | "Tony Parker"     | "Tim Duncan"        |
      | "Chris Paul" | "LeBron James"    | "Ray Allen"         |
      | "Chris Paul" | "Carmelo Anthony" | "Chris Paul"        |
      | "Chris Paul" | "Carmelo Anthony" | "LeBron James"      |
      | "Chris Paul" | "Carmelo Anthony" | "Dwyane Wade"       |
      | "Chris Paul" | "Dwyane Wade"     | "Chris Paul"        |
      | "Chris Paul" | "Dwyane Wade"     | "LeBron James"      |
      | "Chris Paul" | "Dwyane Wade"     | "Carmelo Anthony"   |
    When executing query:
      """
      $var = GO FROM hash('Tim Duncan'), hash('Chris Paul') OVER like YIELD $^.player.name AS name, like._dst AS id;
      GO FROM $var.id OVER like  WHERE $var.name != $$.player.name YIELD $var.name, $^.player.name, $$.player.name
      """
    Then the result should be, in any order, with relax comparison:
      | $var.name    | $^.player.name    | $$.player.name      |
      | "Tim Duncan" | "Tony Parker"     | "LaMarcus Aldridge" |
      | "Tim Duncan" | "Tony Parker"     | "Manu Ginobili"     |
      | "Chris Paul" | "LeBron James"    | "Ray Allen"         |
      | "Chris Paul" | "Carmelo Anthony" | "LeBron James"      |
      | "Chris Paul" | "Carmelo Anthony" | "Dwyane Wade"       |
      | "Chris Paul" | "Dwyane Wade"     | "LeBron James"      |
      | "Chris Paul" | "Dwyane Wade"     | "Carmelo Anthony"   |

  Scenario: Integer Vid no exist prop
    When executing query:
      """
      GO FROM hash('Tim Duncan') OVER serve YIELD $^.player.test
      """
    Then a SemanticError should be raised at runtime: `$^.player.test', not found the property `test'.
    When executing query:
      """
      GO FROM hash('Tim Duncan') OVER serve yield $^.player.test
      """
    Then a SemanticError should be raised at runtime: `$^.player.test', not found the property `test'.
    When executing query:
      """
      GO FROM hash('Tim Duncan') OVER serve YIELD serve.test
      """
    Then a SemanticError should be raised at runtime: `serve.test', not found the property `test'.

  @skip
  Scenario: Integer Vid udf call (reason = "not support udf_is_in now")
    When executing query:
      """
      GO FROM hash('Boris Diaw') OVER serve WHERE udf_is_in($$.team.name, 'Hawks', 'Suns')
      YIELD $^.player.name, serve.start_year, serve.end_year, $$.team.name
      """
    Then the result should be, in any order, with relax comparison:
      | $^.player.name | serve.start_year | serve.end_year | $$.team.name |
      | "Boris Diaw"   | 2003             | 2005           | "Hawks"      |
      | "Boris Diaw"   | 2005             | 2008           | "Suns"       |
    When executing query:
      """
      GO FROM hash('Tim Duncan') OVER like YIELD like._dst AS id
      | GO FROM  $-.id OVER serve WHERE udf_is_in($-.id, 'Tony Parker', 123)
      """
    Then the result should be, in any order, with relax comparison:
      | serve._dst      |
      | hash("Spurs")   |
      | hash("Hornets") |
    When executing query:
      """
      GO FROM hash('Tim Duncan') OVER like YIELD like._dst AS id
      | GO FROM  $-.id OVER serve WHERE udf_is_in($-.id, 'Tony Parker', 123) AND 1 == 1
      """
    Then the result should be, in any order, with relax comparison:
      | serve._dst      |
      | hash("Spurs")   |
      | hash("Hornets") |

  @skip
  Scenario: Integer Vid return test (reason = "return not implement")
    When executing query:
      """
      $A = GO FROM hash('Tim Duncan') OVER like YIELD like._dst AS dst;
      $rA = YIELD $A.* WHERE $A.dst == 123;
      RETURN $rA IF $rA IS NOT NULL;
      GO FROM $A.dst OVER serve
      """
    Then the result should be, in any order, with relax comparison:
      | serve._dst      |
      | hash("Spurs")   |
      | hash("Spurs")   |
      | hash("Hornets") |
    When executing query:
      """
      $A = GO FROM hash('Tim Duncan') OVER like YIELD like._dst AS dst;
      $rA = YIELD $A.* WHERE 1 == 1;
      RETURN $rA IF $rA IS NOT NULL;
      GO FROM $A.dst OVER serve
      """
    Then the result should be, in any order, with relax comparison:
      | serve._dst            |
      | hash("Tony Parker")   |
      | hash("Manu Ginobili") |
    When executing query:
      """
      $A = GO FROM hash('Tim Duncan') OVER like YIELD like._dst AS dstA;
      $rA = YIELD $A.* WHERE $A.dstA == 123;
      RETURN $rA IF $rA IS NOT NULL;
      $B = GO FROM $A.dstA OVER like YIELD like._dst AS dstB;
      $rB = YIELD $B.* WHERE $B.dstB == 456;
      RETURN $rB IF $rB IS NOT NULL;
      GO FROM $B.dstB OVER serve
      """
    Then the result should be, in any order, with relax comparison:
      | serve._dst            |
      | hash("Spurs")         |
      | hash("Spurs")         |
      | hash("Trail Blazers") |
      | hash("Spurs")         |
      | hash("Spurs")         |
    When executing query:
      """
      $A = GO FROM hash('Tim Duncan') OVER like YIELD like._dst AS dst;
      $rA = YIELD $A.* WHERE $A.dst == 123;
      RETURN $rA IF $rA IS NOT NULL;
      """
    Then the result should be, in any order, with relax comparison:
      | serve._dst |
    When executing query:
      """
      $A = GO FROM hash('Tim Duncan') OVER like YIELD like._dst AS dst;
      $rA = YIELD $A.* WHERE 1 == 1;
      RETURN $rA IF $rA IS NOT NULL;
      """
    Then the result should be, in any order, with relax comparison:
      | serve._dst            |
      | hash("Tony Parker")   |
      | hash("Manu Ginobili") |
    When executing query:
      """
      $A = GO FROM hash('Tim Duncan') OVER like YIELD like._dst AS dst;
       $rA = YIELD $A.* WHERE 1 == 1;
        RETURN $B IF $B IS NOT NULL
      """
    Then a SemanticError should be raised at runtime: `$a.id', not exist variable `a'
    When executing query:
      """
      $A = GO FROM hash('Tim Duncan') OVER like YIELD like._dst AS dst;
       $rA = YIELD $A.* WHERE 1 == 1;
        RETURN $B IF $A IS NOT NULL
      """
    Then a SemanticError should be raised at runtime: `$a.id', not exist variable `a'
    When executing query:
      """
      RETURN $rA IF $rA IS NOT NULL;
      """
    Then a SemanticError should be raised at runtime: `$a.id', not exist variable `a'

  Scenario: Integer Vid reverse one step
    When executing query:
      """
      GO FROM hash('Tim Duncan') OVER like REVERSELY YIELD like._dst
      """
    Then the result should be, in any order, with relax comparison:
      | like._dst                 |
      | hash("Tony Parker")       |
      | hash("Manu Ginobili")     |
      | hash("LaMarcus Aldridge") |
      | hash("Marco Belinelli")   |
      | hash("Danny Green")       |
      | hash("Aron Baynes")       |
      | hash("Boris Diaw")        |
      | hash("Tiago Splitter")    |
      | hash("Dejounte Murray")   |
      | hash("Shaquile O'Neal")   |
    When executing query:
      """
      GO FROM hash('Tim Duncan') OVER like REVERSELY YIELD $$.player.name
      """
    Then the result should be, in any order, with relax comparison:
      | $$.player.name      |
      | "Tony Parker"       |
      | "Manu Ginobili"     |
      | "LaMarcus Aldridge" |
      | "Marco Belinelli"   |
      | "Danny Green"       |
      | "Aron Baynes"       |
      | "Boris Diaw"        |
      | "Tiago Splitter"    |
      | "Dejounte Murray"   |
      | "Shaquile O'Neal"   |
    When executing query:
      """
      GO FROM hash('Tim Duncan') OVER like REVERSELY WHERE $$.player.age < 35 YIELD $$.player.name
      """
    Then the result should be, in any order, with relax comparison:
      | $$.player.name      |
      | "LaMarcus Aldridge" |
      | "Marco Belinelli"   |
      | "Danny Green"       |
      | "Aron Baynes"       |
      | "Tiago Splitter"    |
      | "Dejounte Murray"   |
    When executing query:
      """
      GO FROM hash('Tim Duncan') OVER * REVERSELY YIELD like._dst
      """
    Then the result should be, in any order, with relax comparison:
      | like._dst                 |
      | hash("Tony Parker")       |
      | hash("Manu Ginobili")     |
      | hash("LaMarcus Aldridge") |
      | hash("Marco Belinelli")   |
      | hash("Danny Green")       |
      | hash("Aron Baynes")       |
      | hash("Boris Diaw")        |
      | hash(("Tiago Splitter")   |
      | hash("Dejounte Murray")   |
      | hash("Shaquile O'Neal")   |
      | EMPTY                     |
      | EMPTY                     |

  Scenario: Integer Vid only id n steps
    When executing query:
      """
      GO 2 STEPS FROM hash('Tony Parker') OVER like YIELD like._dst
      """
    Then the result should be, in any order, with relax comparison:
      | like._dst             |
      | hash("Tim Duncan")    |
      | hash("Tim Duncan")    |
      | hash("Tony Parker")   |
      | hash("Tony Parker")   |
      | hash("Manu Ginobili") |
    When executing query:
      """
      GO 3 STEPS FROM hash('Tony Parker') OVER like YIELD like._dst
      """
    Then the result should be, in any order, with relax comparison:
      | like._dst                 |
      | hash("Tony Parker")       |
      | hash("Manu Ginobili")     |
      | hash("Manu Ginobili")     |
      | hash("Tim Duncan")        |
      | hash("Tim Duncan")        |
      | hash("LaMarcus Aldridge") |
    When executing query:
      """
      GO 1 STEPS FROM hash('Tony Parker') OVER like YIELD like._dst AS id
      | GO 2 STEPS FROM $-.id OVER like YIELD like._dst
      """
    Then the result should be, in any order, with relax comparison:
      | like._dst                 |
      | hash("LaMarcus Aldridge") |
      | hash("LaMarcus Aldridge") |
      | hash("Manu Ginobili")     |
      | hash("Manu Ginobili")     |
      | hash("Tim Duncan")        |
      | hash("Tim Duncan")        |
      | hash("Tim Duncan")        |
      | hash("Manu Ginobili")     |
      | hash("Manu Ginobili")     |
      | hash("Tony Parker")       |
      | hash("Tony Parker")       |

  Scenario: Integer Vid reverse two steps
    When executing query:
      """
      GO 2 STEPS FROM hash('Kobe Bryant') OVER like REVERSELY YIELD $$.player.name
      """
    Then the result should be, in any order, with relax comparison:
      | $$.player.name |
      | "Marc Gasol"   |
      | "Vince Carter" |
      | "Yao Ming"     |
      | "Grant Hill"   |
    When executing query:
      """
      GO FROM hash('Manu Ginobili') OVER * REVERSELY YIELD like._dst AS id
      | GO FROM $-.id OVER serve
      """
    Then the result should be, in any order, with relax comparison:
      | serve._dst      |
      | hash("Spurs")   |
      | hash("Spurs")   |
      | hash("Hornets") |
      | hash("Spurs")   |
      | hash("Hawks")   |
      | hash("76ers")   |
      | hash("Spurs")   |

  Scenario: Integer Vid reverse with pipe
    When executing query:
      """
      GO FROM hash('LeBron James') OVER serve YIELD serve._dst AS id
      | GO FROM $-.id OVER serve REVERSELY YIELD $^.team.name, $$.player.name
      """
    Then the result should be, in any order, with relax comparison:
      | $^.team.name | $$.player.name      |
      | "Cavaliers"  | "Danny Green"       |
      | "Cavaliers"  | "Danny Green"       |
      | "Cavaliers"  | "Dwyane Wade"       |
      | "Cavaliers"  | "Dwyane Wade"       |
      | "Cavaliers"  | "Kyrie Irving"      |
      | "Cavaliers"  | "Kyrie Irving"      |
      | "Cavaliers"  | "LeBron James"      |
      | "Cavaliers"  | "LeBron James"      |
      | "Cavaliers"  | "Shaquile O'Neal"   |
      | "Cavaliers"  | "Shaquile O'Neal"   |
      | "Cavaliers"  | "LeBron James"      |
      | "Cavaliers"  | "LeBron James"      |
      | "Heat"       | "Amar'e Stoudemire" |
      | "Heat"       | "Dwyane Wade"       |
      | "Heat"       | "LeBron James"      |
      | "Heat"       | "Ray Allen"         |
      | "Heat"       | "Shaquile O'Neal"   |
      | "Heat"       | "Dwyane Wade"       |
      | "Lakers"     | "Dwight Howard"     |
      | "Lakers"     | "JaVale McGee"      |
      | "Lakers"     | "Kobe Bryant"       |
      | "Lakers"     | "LeBron James"      |
      | "Lakers"     | "Paul Gasol"        |
      | "Lakers"     | "Rajon Rondo"       |
      | "Lakers"     | "Shaquile O'Neal"   |
      | "Lakers"     | "Steve Nash"        |
    When executing query:
      """
      GO FROM hash('LeBron James') OVER serve YIELD serve._dst AS id
      | GO FROM $-.id OVER serve REVERSELY WHERE $$.player.name != 'LeBron James'
      YIELD $^.team.name, $$.player.name
      """
    Then the result should be, in any order, with relax comparison:
      | $^.team.name | $$.player.name      |
      | "Cavaliers"  | "Danny Green"       |
      | "Cavaliers"  | "Danny Green"       |
      | "Cavaliers"  | "Dwyane Wade"       |
      | "Cavaliers"  | "Dwyane Wade"       |
      | "Cavaliers"  | "Kyrie Irving"      |
      | "Cavaliers"  | "Kyrie Irving"      |
      | "Cavaliers"  | "Shaquile O'Neal"   |
      | "Cavaliers"  | "Shaquile O'Neal"   |
      | "Heat"       | "Amar'e Stoudemire" |
      | "Heat"       | "Dwyane Wade"       |
      | "Heat"       | "Ray Allen"         |
      | "Heat"       | "Shaquile O'Neal"   |
      | "Heat"       | "Dwyane Wade"       |
      | "Lakers"     | "Dwight Howard"     |
      | "Lakers"     | "JaVale McGee"      |
      | "Lakers"     | "Kobe Bryant"       |
      | "Lakers"     | "Paul Gasol"        |
      | "Lakers"     | "Rajon Rondo"       |
      | "Lakers"     | "Shaquile O'Neal"   |
      | "Lakers"     | "Steve Nash"        |
    When executing query:
      """
      GO FROM hash('Manu Ginobili') OVER like REVERSELY YIELD like._dst AS id
      | GO FROM $-.id OVER serve
      """
    Then the result should be, in any order, with relax comparison:
      | serve._dst      |
      | hash("Spurs")   |
      | hash("Spurs")   |
      | hash("Spurs")   |
      | hash("Spurs")   |
      | hash("Hornets") |
      | hash("Hawks")   |
      | hash("76ers")   |

  Scenario: Integer Vid bidirect
    When executing query:
      """
      GO FROM hash('Tim Duncan') OVER serve bidirect
      """
    Then the result should be, in any order, with relax comparison:
      | serve._dst    |
      | hash("Spurs") |
    When executing query:
      """
      GO FROM hash('Tim Duncan') OVER like bidirect
      """
    Then the result should be, in any order, with relax comparison:
      | like._dst                 |
      | hash("Tony Parker")       |
      | hash("Manu Ginobili")     |
      | hash("Tony Parker")       |
      | hash("Manu Ginobili")     |
      | hash("LaMarcus Aldridge") |
      | hash("Marco Belinelli")   |
      | hash("Danny Green")       |
      | hash("Aron Baynes")       |
      | hash("Boris Diaw")        |
      | hash("Tiago Splitter")    |
      | hash("Dejounte Murray")   |
      | hash("Shaquile O'Neal")   |
    When executing query:
      """
      GO FROM hash('Tim Duncan') OVER serve, like bidirect
      """
    Then the result should be, in any order, with relax comparison:
      | serve._dst    | like._dst                 |
      | hash("Spurs") | EMPTY                     |
      | EMPTY         | hash("Tony Parker")       |
      | EMPTY         | hash("Manu Ginobili")     |
      | EMPTY         | hash("Tony Parker")       |
      | EMPTY         | hash("Manu Ginobili")     |
      | EMPTY         | hash("LaMarcus Aldridge") |
      | EMPTY         | hash("Marco Belinelli")   |
      | EMPTY         | hash("Danny Green")       |
      | EMPTY         | hash("Aron Baynes")       |
      | EMPTY         | hash("Boris Diaw")        |
      | EMPTY         | hash("Tiago Splitter")    |
      | EMPTY         | hash("Dejounte Murray")   |
      | EMPTY         | hash("Shaquile O'Neal")   |
    When executing query:
      """
      GO FROM hash('Tim Duncan') OVER serve bidirect YIELD $$.team.name
      """
    Then the result should be, in any order, with relax comparison:
      | $$.team.name |
      | "Spurs"      |
    When executing query:
      """
      GO FROM hash('Tim Duncan') OVER like bidirect YIELD $$.player.name
      """
    Then the result should be, in any order, with relax comparison:
      | $$.player.name      |
      | "Tony Parker"       |
      | "Manu Ginobili"     |
      | "Tony Parker"       |
      | "Manu Ginobili"     |
      | "LaMarcus Aldridge" |
      | "Marco Belinelli"   |
      | "Danny Green"       |
      | "Aron Baynes"       |
      | "Boris Diaw"        |
      | "Tiago Splitter"    |
      | "Dejounte Murray"   |
      | "Shaquile O'Neal"   |
    When executing query:
      """
      GO FROM hash('Tim Duncan') OVER like bidirect WHERE like.likeness > 90
      YIELD $^.player.name, like._dst, $$.player.name, like.likeness
      """
    Then the result should be, in any order, with relax comparison:
      | $^.player.name | like._dst               | $$.player.name    | like.likeness |
      | "Tim Duncan"   | hash("Tony Parker")     | "Tony Parker"     | 95            |
      | "Tim Duncan"   | hash("Manu Ginobili")   | "Manu Ginobili"   | 95            |
      | "Tim Duncan"   | hash("Tony Parker")     | "Tony Parker"     | 95            |
      | "Tim Duncan"   | hash("Dejounte Murray") | "Dejounte Murray" | 99            |

  Scenario: Integer Vid bidirect_over_all
    When executing query:
      """
      GO FROM hash('Tim Duncan') OVER * bidirect YIELD $^.player.name, serve._dst, $$.team.name, like._dst, $$.player.name
      """
    Then the result should be, in any order, with relax comparison:
      | $^.player.name | serve._dst    | $$.team.name | like._dst                 | $$.player.name      |
      | "Tim Duncan"   | hash("Spurs") | "Spurs"      | EMPTY                     | EMPTY               |
      | "Tim Duncan"   | EMPTY         | EMPTY        | hash("Tony Parker")       | "Tony Parker"       |
      | "Tim Duncan"   | EMPTY         | EMPTY        | hash("Manu Ginobili")     | "Manu Ginobili"     |
      | "Tim Duncan"   | EMPTY         | EMPTY        | hash("Tony Parker")       | "Tony Parker"       |
      | "Tim Duncan"   | EMPTY         | EMPTY        | hash("Manu Ginobili")     | "Manu Ginobili"     |
      | "Tim Duncan"   | EMPTY         | EMPTY        | hash("LaMarcus Aldridge") | "LaMarcus Aldridge" |
      | "Tim Duncan"   | EMPTY         | EMPTY        | hash("Marco Belinelli")   | "Marco Belinelli"   |
      | "Tim Duncan"   | EMPTY         | EMPTY        | hash("Danny Green")       | "Danny Green"       |
      | "Tim Duncan"   | EMPTY         | EMPTY        | hash("Aron Baynes")       | "Aron Baynes"       |
      | "Tim Duncan"   | EMPTY         | EMPTY        | hash("Boris Diaw")        | "Boris Diaw"        |
      | "Tim Duncan"   | EMPTY         | EMPTY        | hash("Tiago Splitter")    | "Tiago Splitter"    |
      | "Tim Duncan"   | EMPTY         | EMPTY        | hash("Dejounte Murray")   | "Dejounte Murray"   |
      | "Tim Duncan"   | EMPTY         | EMPTY        | hash("Shaquile O'Neal")   | "Shaquile O'Neal"   |
      | "Tim Duncan"   | EMPTY         | EMPTY        | EMPTY                     | "Tony Parker"       |
      | "Tim Duncan"   | EMPTY         | EMPTY        | EMPTY                     | "Manu Ginobili"     |
      | "Tim Duncan"   | EMPTY         | EMPTY        | EMPTY                     | "Danny Green"       |
      | "Tim Duncan"   | EMPTY         | EMPTY        | EMPTY                     | "LaMarcus Aldridge" |
      | "Tim Duncan"   | EMPTY         | EMPTY        | EMPTY                     | "Tony Parker"       |
      | "Tim Duncan"   | EMPTY         | EMPTY        | EMPTY                     | "Manu Ginobili"     |
    When executing query:
      """
      GO FROM hash('Tim Duncan') OVER * bidirect
      """
    Then the result should be, in any order, with relax comparison:
      | like._dst                 | serve._dst    | teammate._dst             |
      | EMPTY                     | hash("Spurs") | EMPTY                     |
      | hash("Tony Parker")       | EMPTY         | EMPTY                     |
      | hash("Manu Ginobili")     | EMPTY         | EMPTY                     |
      | hash("Tony Parker")       | EMPTY         | EMPTY                     |
      | hash("Manu Ginobili")     | EMPTY         | EMPTY                     |
      | hash("LaMarcus Aldridge") | EMPTY         | EMPTY                     |
      | hash("Marco Belinelli")   | EMPTY         | EMPTY                     |
      | hash("Danny Green")       | EMPTY         | EMPTY                     |
      | hash("Aron Baynes")       | EMPTY         | EMPTY                     |
      | hash("Boris Diaw")        | EMPTY         | EMPTY                     |
      | hash("Tiago Splitter")    | EMPTY         | EMPTY                     |
      | hash("Dejounte Murray")   | EMPTY         | EMPTY                     |
      | hash("Shaquile O'Neal")   | EMPTY         | EMPTY                     |
      | EMPTY                     | EMPTY         | hash("Tony Parker")       |
      | EMPTY                     | EMPTY         | hash("Manu Ginobili")     |
      | EMPTY                     | EMPTY         | hash("LaMarcus Aldridge") |
      | EMPTY                     | EMPTY         | hash("Danny Green")       |
      | EMPTY                     | EMPTY         | hash("Tony Parker")       |
      | EMPTY                     | EMPTY         | hash("Manu Ginobili")     |

  Scenario: Integer Vid duplicate column name
    When executing query:
      """
      GO FROM hash('Tim Duncan') OVER serve YIELD serve._dst, serve._dst
      """
    Then the result should be, in any order, with relax comparison:
      | serve._dst    | serve._dst    |
      | hash("Spurs") | hash("Spurs") |
    When executing query:
      """
      GO FROM hash('Tim Duncan') OVER like YIELD like._dst AS id, like.likeness AS id
      | GO FROM $-.id OVER serve
      """
    Then a SemanticError should be raised at runtime: Duplicate Column Name : `id'
    When executing query:
      """
      GO FROM hash('Tim Duncan') OVER like, serve YIELD serve.start_year AS year, serve.end_year AS year, serve._dst AS id
      | GO FROM $-.id OVER *
      """
    Then a SemanticError should be raised at runtime: Duplicate Column Name : `year'
    When executing query:
      """
      $a = GO FROM hash('Tim Duncan') OVER *
      YIELD serve.start_year AS year, serve.end_year AS year, serve._dst AS id;
      | GO FROM $-.id OVER serve
      """
    Then a SyntaxError should be raised at runtime: syntax error near `| GO FRO'

  Scenario: Integer Vid contain
    When executing query:
      """
      GO FROM hash('Boris Diaw') OVER serve WHERE $$.team.name CONTAINS "Haw"
      YIELD $^.player.name, serve.start_year, serve.end_year, $$.team.name
      """
    Then the result should be, in any order, with relax comparison:
      | $^.player.name | serve.start_year | serve.end_year | $$.team.name |
      | "Boris Diaw"   | 2003             | 2005           | "Hawks"      |
    When executing query:
      """
      GO FROM hash('Boris Diaw') OVER serve WHERE (string)serve.start_year CONTAINS "05"
      YIELD $^.player.name, serve.start_year, serve.end_year, $$.team.name
      """
    Then the result should be, in any order, with relax comparison:
      | $^.player.name | serve.start_year | serve.end_year | $$.team.name |
      | "Boris Diaw"   | 2005             | 2008           | "Suns"       |
    When executing query:
      """
      GO FROM hash('Boris Diaw') OVER serve WHERE $^.player.name CONTAINS "Boris"
      YIELD $^.player.name, serve.start_year, serve.end_year, $$.team.name
      """
    Then the result should be, in any order, with relax comparison:
      | $^.player.name | serve.start_year | serve.end_year | $$.team.name |
      | "Boris Diaw"   | 2003             | 2005           | "Hawks"      |
      | "Boris Diaw"   | 2005             | 2008           | "Suns"       |
      | "Boris Diaw"   | 2008             | 2012           | "Hornets"    |
      | "Boris Diaw"   | 2012             | 2016           | "Spurs"      |
      | "Boris Diaw"   | 2016             | 2017           | "Jazz"       |
    When executing query:
      """
      GO FROM hash('Boris Diaw') OVER serve WHERE !($^.player.name CONTAINS "Boris")
      YIELD $^.player.name, serve.start_year, serve.end_year, $$.team.name
      """
    Then the result should be, in any order, with relax comparison:
      | $^.player.name | serve.start_year | serve.end_year | $$.team.name |
    When executing query:
      """
      GO FROM hash('Boris Diaw') OVER serve WHERE "Leo" CONTAINS "Boris"
      YIELD $^.player.name, serve.start_year, serve.end_year, $$.team.name
      """
    Then the result should be, in any order, with relax comparison:
      | $^.player.name | serve.start_year | serve.end_year | $$.team.name |

  Scenario: Integer Vid intermediate data
    When executing query:
      """
      GO 0 TO 0 STEPS FROM hash('Tony Parker') OVER like YIELD DISTINCT like._dst
      """
    Then the result should be, in any order, with relax comparison:
      | like._dst |
    When executing query:
      """
      GO 1 TO 2 STEPS FROM hash('Tony Parker') OVER like YIELD DISTINCT like._dst
      """
    Then the result should be, in any order, with relax comparison:
      | like._dst                 |
      | hash("Tony Parker")       |
      | hash("Manu Ginobili")     |
      | hash("LaMarcus Aldridge") |
      | hash("Tim Duncan")        |
    When executing query:
      """
      GO 0 TO 2 STEPS FROM hash('Tony Parker') OVER like YIELD DISTINCT like._dst
      """
    Then the result should be, in any order, with relax comparison:
      | like._dst                 |
      | hash("Tony Parker")       |
      | hash("Manu Ginobili")     |
      | hash("LaMarcus Aldridge") |
      | hash("Tim Duncan")        |
    When executing query:
      """
      GO 1 TO 2 STEPS FROM hash('Tony Parker') OVER like YIELD DISTINCT like._dst, like.likeness, $$.player.name
      """
    Then the result should be, in any order, with relax comparison:
      | like._dst                 | like.likeness | $$.player.name      |
      | hash("Manu Ginobili")     | 95            | "Manu Ginobili"     |
      | hash("LaMarcus Aldridge") | 90            | "LaMarcus Aldridge" |
      | hash("Tim Duncan")        | 95            | "Tim Duncan"        |
      | hash("Tony Parker")       | 95            | "Tony Parker"       |
      | hash("Tony Parker")       | 75            | "Tony Parker"       |
      | hash("Tim Duncan")        | 75            | "Tim Duncan"        |
      | hash("Tim Duncan")        | 90            | "Tim Duncan"        |
    When executing query:
      """
      GO 0 TO 2 STEPS FROM hash('Tony Parker') OVER like YIELD DISTINCT like._dst, like.likeness, $$.player.name
      """
    Then the result should be, in any order, with relax comparison:
      | like._dst                 | like.likeness | $$.player.name      |
      | hash("Manu Ginobili")     | 95            | "Manu Ginobili"     |
      | hash("LaMarcus Aldridge") | 90            | "LaMarcus Aldridge" |
      | hash("Tim Duncan")        | 95            | "Tim Duncan"        |
      | hash("Tony Parker")       | 95            | "Tony Parker"       |
      | hash("Tony Parker")       | 75            | "Tony Parker"       |
      | hash("Tim Duncan")        | 75            | "Tim Duncan"        |
      | hash("Tim Duncan")        | 90            | "Tim Duncan"        |
    When executing query:
      """
      GO 0 TO 3 STEPS FROM hash('Tim Duncan') OVER serve
      """
    Then the result should be, in any order, with relax comparison:
      | serve._dst    |
      | hash("Spurs") |
    When executing query:
      """
      GO 2 TO 3 STEPS FROM hash('Tim Duncan') OVER serve
      """
    Then the result should be, in any order, with relax comparison:
      | serve._dst |
    When executing query:
      """
      GO 1 TO 2 STEPS FROM hash('Tony Parker') OVER like REVERSELY YIELD DISTINCT like._dst
      """
    Then the result should be, in any order, with relax comparison:
      | like._dst                 |
      | hash("Tim Duncan")        |
      | hash("LaMarcus Aldridge") |
      | hash("Marco Belinelli")   |
      | hash("Boris Diaw")        |
      | hash("Dejounte Murray")   |
      | hash("Tony Parker")       |
      | hash("Manu Ginobili")     |
      | hash("Danny Green")       |
      | hash("Aron Baynes")       |
      | hash("Tiago Splitter")    |
      | hash("Shaquile O'Neal")   |
      | hash("Rudy Gay")          |
      | hash("Damian Lillard")    |
    When executing query:
      """
      GO 0 TO 2 STEPS FROM hash('Tony Parker') OVER like REVERSELY YIELD DISTINCT like._dst
      """
    Then the result should be, in any order, with relax comparison:
      | like._dst                 |
      | hash("Tim Duncan")        |
      | hash("LaMarcus Aldridge") |
      | hash("Marco Belinelli")   |
      | hash("Boris Diaw")        |
      | hash("Dejounte Murray")   |
      | hash("Tony Parker")       |
      | hash("Manu Ginobili")     |
      | hash("Danny Green")       |
      | hash("Aron Baynes")       |
      | hash("Tiago Splitter")    |
      | hash("Shaquile O'Neal")   |
      | hash("Rudy Gay")          |
      | hash("Damian Lillard")    |
    When executing query:
      """
      GO 2 TO 2 STEPS FROM hash('Tony Parker') OVER like REVERSELY YIELD DISTINCT like._dst
      """
    Then the result should be, in any order, with relax comparison:
      | like._dst                 |
      | hash("LaMarcus Aldridge") |
      | hash("Marco Belinelli")   |
      | hash("Boris Diaw")        |
      | hash("Dejounte Murray")   |
      | hash("Tony Parker")       |
      | hash("Manu Ginobili")     |
      | hash("Danny Green")       |
      | hash("Aron Baynes")       |
      | hash("Tiago Splitter")    |
      | hash("Shaquile O'Neal")   |
      | hash("Rudy Gay")          |
      | hash("Damian Lillard")    |
    When executing query:
      """
      GO 1 TO 3 STEPS FROM hash('Spurs') OVER serve REVERSELY
      """
    Then the result should be, in any order, with relax comparison:
      | serve._dst                |
      | hash("Tim Duncan")        |
      | hash("Tony Parker")       |
      | hash("Manu Ginobili")     |
      | hash("LaMarcus Aldridge") |
      | hash("Rudy Gay")          |
      | hash("Marco Belinelli")   |
      | hash("Danny Green")       |
      | hash("Kyle Anderson")     |
      | hash("Aron Baynes")       |
      | hash("Boris Diaw")        |
      | hash("Tiago Splitter")    |
      | hash("Cory Joseph")       |
      | hash("David West")        |
      | hash("Jonathon Simmons")  |
      | hash("Dejounte Murray")   |
      | hash("Tracy McGrady")     |
      | hash("Paul Gasol")        |
      | hash("Marco Belinelli")   |
    When executing query:
      """
      GO 0 TO 3 STEPS FROM hash('Spurs') OVER serve REVERSELY
      """
    Then the result should be, in any order, with relax comparison:
      | serve._dst                |
      | hash("Tim Duncan")        |
      | hash("Tony Parker")       |
      | hash("Manu Ginobili")     |
      | hash("LaMarcus Aldridge") |
      | hash("Rudy Gay")          |
      | hash("Marco Belinelli")   |
      | hash("Danny Green")       |
      | hash("Kyle Anderson")     |
      | hash("Aron Baynes")       |
      | hash("Boris Diaw")        |
      | hash("Tiago Splitter")    |
      | hash("Cory Joseph")       |
      | hash("David West")        |
      | hash("Jonathon Simmons")  |
      | hash("Dejounte Murray")   |
      | hash("Tracy McGrady")     |
      | hash("Paul Gasol")        |
      | hash("Marco Belinelli")   |
    When executing query:
      """
      GO 1 TO 2 STEPS FROM hash('Tony Parker') OVER like BIDIRECT YIELD DISTINCT like._dst
      """
    Then the result should be, in any order, with relax comparison:
      | like._dst                 |
      | hash("Tim Duncan")        |
      | hash("LaMarcus Aldridge") |
      | hash("Marco Belinelli")   |
      | hash("Boris Diaw")        |
      | hash("Dejounte Murray")   |
      | hash("Tony Parker")       |
      | hash("Manu Ginobili")     |
      | hash("Danny Green")       |
      | hash("Aron Baynes")       |
      | hash("Tiago Splitter")    |
      | hash("Shaquile O'Neal")   |
      | hash("Rudy Gay")          |
      | hash("Damian Lillard")    |
      | hash("LeBron James")      |
      | hash("Russell Westbrook") |
      | hash("Chris Paul")        |
      | hash("Kyle Anderson")     |
      | hash("Kevin Durant")      |
      | hash("James Harden")      |
    When executing query:
      """
      GO 0 TO 2 STEPS FROM hash('Tony Parker') OVER like BIDIRECT YIELD DISTINCT like._dst
      """
    Then the result should be, in any order, with relax comparison:
      | like._dst                 |
      | hash("Tim Duncan")        |
      | hash("LaMarcus Aldridge") |
      | hash("Marco Belinelli")   |
      | hash("Boris Diaw")        |
      | hash("Dejounte Murray")   |
      | hash("Tony Parker")       |
      | hash("Manu Ginobili")     |
      | hash("Danny Green")       |
      | hash("Aron Baynes")       |
      | hash("Tiago Splitter")    |
      | hash("Shaquile O'Neal")   |
      | hash("Rudy Gay")          |
      | hash("Damian Lillard")    |
      | hash("LeBron James")      |
      | hash("Russell Westbrook") |
      | hash("Chris Paul")        |
      | hash("Kyle Anderson")     |
      | hash("Kevin Durant")      |
      | hash("James Harden")      |
    When executing query:
      """
      GO 1 TO 2 STEPS FROM hash('Russell Westbrook') OVER * YIELD serve._dst, like._dst
      """
    Then the result should be, in any order, with relax comparison:
      | serve._dst       | like._dst                 |
      | hash("Thunders") | EMPTY                     |
      | EMPTY            | hash("Paul George")       |
      | EMPTY            | hash("James Harden")      |
      | hash("Pacers")   | EMPTY                     |
      | hash("Thunders") | EMPTY                     |
      | EMPTY            | hash("Russell Westbrook") |
      | hash("Thunders") | EMPTY                     |
      | hash("Rockets")  | EMPTY                     |
      | EMPTY            | hash("Russell Westbrook") |
    When executing query:
      """
      GO 0 TO 2 STEPS FROM hash('Russell Westbrook') OVER * YIELD serve._dst, like._dst
      """
    Then the result should be, in any order, with relax comparison:
      | serve._dst       | like._dst                 |
      | hash("Thunders") | EMPTY                     |
      | EMPTY            | hash("Paul George")       |
      | EMPTY            | hash("James Harden")      |
      | hash("Pacers")   | EMPTY                     |
      | hash("Thunders") | EMPTY                     |
      | EMPTY            | hash("Russell Westbrook") |
      | hash("Thunders") | EMPTY                     |
      | hash("Rockets")  | EMPTY                     |
      | EMPTY            | hash("Russell Westbrook") |
    When executing query:
      """
      GO 1 TO 2 STEPS FROM hash('Russell Westbrook') OVER *
      YIELD serve._dst, like._dst, serve.start_year, like.likeness, $$.player.name
      """
    Then the result should be, in any order, with relax comparison:
      | serve._dst       | like._dst                 | serve.start_year | like.likeness | $$.player.name      |
      | hash("Thunders") | EMPTY                     | 2008             | EMPTY         | EMPTY               |
      | EMPTY            | hash("Paul George")       | EMPTY            | 90            | "Paul George"       |
      | EMPTY            | hash("James Harden")      | EMPTY            | 90            | "James Harden"      |
      | hash("Pacers")   | EMPTY                     | 2010             | EMPTY         | EMPTY               |
      | hash("Thunders") | EMPTY                     | 2017             | EMPTY         | EMPTY               |
      | EMPTY            | hash("Russell Westbrook") | EMPTY            | 95            | "Russell Westbrook" |
      | hash("Thunders") | EMPTY                     | 2009             | EMPTY         | EMPTY               |
      | hash("Rockets")  | EMPTY                     | 2012             | EMPTY         | EMPTY               |
      | EMPTY            | hash("Russell Westbrook") | EMPTY            | 80            | "Russell Westbrook" |
    When executing query:
      """
      GO 0 TO 2 STEPS FROM hash('Russell Westbrook') OVER *
      YIELD serve._dst, like._dst, serve.start_year, like.likeness, $$.player.name
      """
    Then the result should be, in any order, with relax comparison:
      | serve._dst       | like._dst                 | serve.start_year | like.likeness | $$.player.name      |
      | hash("Thunders") | EMPTY                     | 2008             | EMPTY         | EMPTY               |
      | EMPTY            | hash("Paul George")       | EMPTY            | 90            | "Paul George"       |
      | EMPTY            | hash("James Harden")      | EMPTY            | 90            | "James Harden"      |
      | hash("Pacers")   | EMPTY                     | 2010             | EMPTY         | EMPTY               |
      | hash("Thunders") | EMPTY                     | 2017             | EMPTY         | EMPTY               |
      | EMPTY            | hash("Russell Westbrook") | EMPTY            | 95            | "Russell Westbrook" |
      | hash("Thunders") | EMPTY                     | 2009             | EMPTY         | EMPTY               |
      | hash("Rockets")  | EMPTY                     | 2012             | EMPTY         | EMPTY               |
      | EMPTY            | hash("Russell Westbrook") | EMPTY            | 80            | "Russell Westbrook" |
    When executing query:
      """
      GO 1 TO 2 STEPS FROM hash('Russell Westbrook') OVER * REVERSELY YIELD serve._dst, like._dst
      """
    Then the result should be, in any order, with relax comparison:
      | serve._dst | like._dst                 |
      | EMPTY      | hash("Dejounte Murray")   |
      | EMPTY      | hash("James Harden")      |
      | EMPTY      | hash("Paul George")       |
      | EMPTY      | hash("Dejounte Murray")   |
      | EMPTY      | hash("Russell Westbrook") |
      | EMPTY      | hash("Luka Doncic")       |
      | EMPTY      | hash("Russell Westbrook") |
    When executing query:
      """
      GO 0 TO 2 STEPS FROM hash('Russell Westbrook') OVER * REVERSELY YIELD serve._dst, like._dst
      """
    Then the result should be, in any order, with relax comparison:
      | serve._dst | like._dst                 |
      | EMPTY      | hash("Dejounte Murray")   |
      | EMPTY      | hash("James Harden")      |
      | EMPTY      | hash("Paul George")       |
      | EMPTY      | hash("Dejounte Murray")   |
      | EMPTY      | hash("Russell Westbrook") |
      | EMPTY      | hash("Luka Doncic")       |
      | EMPTY      | hash("Russell Westbrook") |

  Scenario: Integer Vid error message
    When executing query:
      """
      GO FROM hash('Tim Duncan') OVER serve YIELD $$.player.name as name
      """
    Then the result should be, in any order, with relax comparison:
      | name  |
      | EMPTY |

  Scenario: Integer Vid zero step
    When executing query:
      """
      GO 0 STEPS FROM hash('Tim Duncan') OVER serve BIDIRECT
      """
    Then the result should be, in any order, with relax comparison:
      | serve._dst |
    When executing query:
      """
      GO 0 STEPS FROM hash('Tim Duncan') OVER serve
      """
    Then the result should be, in any order, with relax comparison:
      | serve._dst |
    When executing query:
      """
      GO 0 STEPS FROM hash('Tim Duncan') OVER like YIELD like._dst as id | GO FROM $-.id OVER serve
      """
    Then the result should be, in any order, with relax comparison:
      | serve._dst |

  Scenario: Integer Vid go cover input
    When executing query:
      """
      GO FROM hash('Tim Duncan') OVER like YIELD like._src as src, like._dst as dst | GO FROM $-.src OVER like
      YIELD $-.src as src, like._dst as dst, $^.player.name, $$.player.name
      """
    Then the result should be, in any order, with relax comparison:
      | src                | dst                   | $^.player.name | $$.player.name  |
      | hash("Tim Duncan") | hash("Tony Parker")   | "Tim Duncan"   | "Tony Parker"   |
      | hash("Tim Duncan") | hash("Manu Ginobili") | "Tim Duncan"   | "Manu Ginobili" |
      | hash("Tim Duncan") | hash("Tony Parker")   | "Tim Duncan"   | "Tony Parker"   |
      | hash("Tim Duncan") | hash("Manu Ginobili") | "Tim Duncan"   | "Manu Ginobili" |
    When executing query:
      """
      $a = GO FROM hash('Tim Duncan') OVER like YIELD like._src as src, like._dst as dst;
      GO FROM $a.src OVER like YIELD $a.src as src, like._dst as dst
      """
    Then the result should be, in any order, with relax comparison:
      | src                | dst                   |
      | hash("Tim Duncan") | hash("Tony Parker")   |
      | hash("Tim Duncan") | hash("Manu Ginobili") |
      | hash("Tim Duncan") | hash("Tony Parker")   |
      | hash("Tim Duncan") | hash("Manu Ginobili") |
    When executing query:
      """
      GO FROM hash('Tim Duncan') OVER like YIELD like._src as src, like._dst as dst
      | GO 1 TO 2 STEPS FROM $-.src OVER like YIELD $-.src as src, like._dst as dst
      """
    Then the result should be, in any order, with relax comparison:
      | src                | dst                       |
      | hash("Tim Duncan") | hash("Tony Parker")       |
      | hash("Tim Duncan") | hash("Manu Ginobili")     |
      | hash("Tim Duncan") | hash("Tony Parker")       |
      | hash("Tim Duncan") | hash("Manu Ginobili")     |
      | hash("Tim Duncan") | hash("Tim Duncan")        |
      | hash("Tim Duncan") | hash("Tim Duncan")        |
      | hash("Tim Duncan") | hash("Manu Ginobili")     |
      | hash("Tim Duncan") | hash("Manu Ginobili")     |
      | hash("Tim Duncan") | hash("LaMarcus Aldridge") |
      | hash("Tim Duncan") | hash("LaMarcus Aldridge") |
      | hash("Tim Duncan") | hash("Tim Duncan")        |
      | hash("Tim Duncan") | hash("Tim Duncan")        |
    When executing query:
      """
      GO FROM hash('Tim Duncan') OVER like YIELD like._src as src, like._dst as dst
      | GO 1 TO 2 STEPS FROM $-.src OVER like
      YIELD $-.src as src, $-.dst, like._dst as dst, like.likeness
      """
    Then the result should be, in any order, with relax comparison:
      | src                | $-.dst                | dst                       | like.likeness |
      | hash("Tim Duncan") | hash("Tony Parker")   | hash("Tony Parker")       | 95            |
      | hash("Tim Duncan") | hash("Tony Parker")   | hash("Manu Ginobili")     | 95            |
      | hash("Tim Duncan") | hash("Manu Ginobili") | hash("Tony Parker")       | 95            |
      | hash("Tim Duncan") | hash("Manu Ginobili") | hash("Manu Ginobili")     | 95            |
      | hash("Tim Duncan") | hash("Tony Parker")   | hash("Tim Duncan")        | 95            |
      | hash("Tim Duncan") | hash("Tony Parker")   | hash("Manu Ginobili")     | 95            |
      | hash("Tim Duncan") | hash("Tony Parker")   | hash("LaMarcus Aldridge") | 90            |
      | hash("Tim Duncan") | hash("Tony Parker")   | hash("Tim Duncan")        | 90            |
      | hash("Tim Duncan") | hash("Manu Ginobili") | hash("Tim Duncan")        | 95            |
      | hash("Tim Duncan") | hash("Manu Ginobili") | hash("Manu Ginobili")     | 95            |
      | hash("Tim Duncan") | hash("Manu Ginobili") | hash("LaMarcus Aldridge") | 90            |
      | hash("Tim Duncan") | hash("Manu Ginobili") | hash("Tim Duncan")        | 90            |
    When executing query:
      """
      GO FROM hash('Danny Green') OVER like YIELD like._src AS src, like._dst AS dst
      | GO FROM $-.dst OVER teammate YIELD $-.src AS src, $-.dst, teammate._dst AS dst
      """
    Then the result should be, in any order, with relax comparison:
      | src                 | $-.dst             | dst                       |
      | hash("Danny Green") | hash("Tim Duncan") | hash("Tony Parker")       |
      | hash("Danny Green") | hash("Tim Duncan") | hash("Manu Ginobili")     |
      | hash("Danny Green") | hash("Tim Duncan") | hash("LaMarcus Aldridge") |
      | hash("Danny Green") | hash("Tim Duncan") | hash("Danny Green")       |
    When executing query:
      """
      $a = GO FROM hash('Danny Green') OVER like YIELD like._src AS src, like._dst AS dst;
      GO FROM $a.dst OVER teammate YIELD $a.src AS src, $a.dst, teammate._dst AS dst
      """
    Then the result should be, in any order, with relax comparison:
      | src                 | $a.dst             | dst                       |
      | hash("Danny Green") | hash("Tim Duncan") | hash("Tony Parker")       |
      | hash("Danny Green") | hash("Tim Duncan") | hash("Manu Ginobili")     |
      | hash("Danny Green") | hash("Tim Duncan") | hash("LaMarcus Aldridge") |
      | hash("Danny Green") | hash("Tim Duncan") | hash("Danny Green")       |

  Scenario: Integer Vid backtrack overlap
    When executing query:
      """
      GO FROM hash('Tony Parker') OVER like YIELD like._src as src, like._dst as dst
      | GO 2 STEPS FROM $-.src OVER like YIELD $-.src, $-.dst, like._src, like._dst
      """
    Then the result should be, in any order, with relax comparison:
      | $-.src              | $-.dst                     | like._src                  | like._dst              |
      | hash("Tony Parker") | hash("Tim Duncan" )        | hash("Tim Duncan" )        | hash("Manu Ginobili")  |
      | hash("Tony Parker") | hash("Tim Duncan" )        | hash("Tim Duncan" )        | hash("Tony Parker")    |
      | hash("Tony Parker") | hash("Tim Duncan" )        | hash("LaMarcus Aldridge" ) | hash("Tony Parker")    |
      | hash("Tony Parker") | hash("Tim Duncan" )        | hash("Manu Ginobili")      | hash("Tim Duncan" )    |
      | hash("Tony Parker") | hash("Tim Duncan" )        | hash("LaMarcus Aldridge" ) | hash("Tim Duncan" )    |
      | hash("Tony Parker") | hash("Manu Ginobili" )     | hash("Tim Duncan" )        | hash("Manu Ginobili" ) |
      | hash("Tony Parker") | hash("Manu Ginobili" )     | hash("Tim Duncan" )        | hash("Tony Parker")    |
      | hash("Tony Parker") | hash("Manu Ginobili" )     | hash("LaMarcus Aldridge" ) | hash("Tony Parker")    |
      | hash("Tony Parker") | hash("Manu Ginobili" )     | hash("Manu Ginobili" )     | hash("Tim Duncan" )    |
      | hash("Tony Parker") | hash("Manu Ginobili" )     | hash("LaMarcus Aldridge" ) | hash("Tim Duncan" )    |
      | hash("Tony Parker") | hash("LaMarcus Aldridge" ) | hash("Tim Duncan" )        | hash("Manu Ginobili" ) |
      | hash("Tony Parker") | hash("LaMarcus Aldridge" ) | hash("Tim Duncan" )        | hash("Tony Parker")    |
      | hash("Tony Parker") | hash("LaMarcus Aldridge" ) | hash("LaMarcus Aldridge" ) | hash("Tony Parker")    |
      | hash("Tony Parker") | hash("LaMarcus Aldridge" ) | hash("Manu Ginobili" )     | hash("Tim Duncan" )    |
      | hash("Tony Parker") | hash("LaMarcus Aldridge" ) | hash("LaMarcus Aldridge" ) | hash("Tim Duncan" )    |
    When executing query:
      """
      $a = GO FROM hash('Tony Parker') OVER like YIELD like._src as src, like._dst as dst;
      GO 2 STEPS FROM $a.src OVER like YIELD $a.src, $a.dst, like._src, like._dst
      """
    Then the result should be, in any order, with relax comparison:
      | $a.src              | $a.dst                     | like._src                  | like._dst              |
      | hash("Tony Parker") | hash("Tim Duncan" )        | hash("Tim Duncan" )        | hash("Manu Ginobili")  |
      | hash("Tony Parker") | hash("Tim Duncan" )        | hash("Tim Duncan" )        | hash("Tony Parker")    |
      | hash("Tony Parker") | hash("Tim Duncan" )        | hash("LaMarcus Aldridge" ) | hash("Tony Parker")    |
      | hash("Tony Parker") | hash("Tim Duncan" )        | hash("Manu Ginobili")      | hash("Tim Duncan" )    |
      | hash("Tony Parker") | hash("Tim Duncan" )        | hash("LaMarcus Aldridge" ) | hash("Tim Duncan" )    |
      | hash("Tony Parker") | hash("Manu Ginobili" )     | hash("Tim Duncan" )        | hash("Manu Ginobili" ) |
      | hash("Tony Parker") | hash("Manu Ginobili" )     | hash("Tim Duncan" )        | hash("Tony Parker")    |
      | hash("Tony Parker") | hash("Manu Ginobili" )     | hash("LaMarcus Aldridge" ) | hash("Tony Parker")    |
      | hash("Tony Parker") | hash("Manu Ginobili" )     | hash("Manu Ginobili" )     | hash("Tim Duncan" )    |
      | hash("Tony Parker") | hash("Manu Ginobili" )     | hash("LaMarcus Aldridge" ) | hash("Tim Duncan" )    |
      | hash("Tony Parker") | hash("LaMarcus Aldridge" ) | hash("Tim Duncan" )        | hash("Manu Ginobili" ) |
      | hash("Tony Parker") | hash("LaMarcus Aldridge" ) | hash("Tim Duncan" )        | hash("Tony Parker")    |
      | hash("Tony Parker") | hash("LaMarcus Aldridge" ) | hash("LaMarcus Aldridge" ) | hash("Tony Parker")    |
      | hash("Tony Parker") | hash("LaMarcus Aldridge" ) | hash("Manu Ginobili" )     | hash("Tim Duncan" )    |
      | hash("Tony Parker") | hash("LaMarcus Aldridge" ) | hash("LaMarcus Aldridge" ) | hash("Tim Duncan" )    |
