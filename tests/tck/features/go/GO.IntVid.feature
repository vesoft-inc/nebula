# Copyright (c) 2021 vesoft inc. All rights reserved.
#
# This source code is licensed under Apache 2.0 License,
# attached with Common Clause Condition 1.0, found in the LICENSES directory.
Feature: IntegerVid Go  Sentence

  Background:
    Given a graph with space named "nba_int_vid"

  Scenario: Integer Vid one step
    When executing query:
      """
      GO FROM hash("Tim Duncan") OVER serve
      """
    Then the result should be, in any order, with relax comparison, and the columns 0 should be hashed:
      | serve._dst |
      | "Spurs"    |
    When executing query:
      """
      GO FROM hash("Tim Duncan") OVER like YIELD $^.player.name as name, $^.player.age as age
      """
    Then the result should be, in any order, with relax comparison:
      | name         | age |
      | "Tim Duncan" | 42  |
      | "Tim Duncan" | 42  |
    When executing query:
      """
      GO FROM hash("Tim Duncan"), hash("Tony Parker") OVER like YIELD $^.player.name as name, $^.player.age as age
      """
    Then the result should be, in any order, with relax comparison:
      | name          | age |
      | "Tim Duncan"  | 42  |
      | "Tim Duncan"  | 42  |
      | "Tony Parker" | 36  |
      | "Tony Parker" | 36  |
      | "Tony Parker" | 36  |
    When executing query:
      """
      GO FROM hash("Tim Duncan"), hash("Tim Duncan") OVER serve
      """
    Then the result should be, in any order, with relax comparison, and the columns 0 should be hashed:
      | serve._dst |
      | "Spurs"    |
      | "Spurs"    |
    When executing query:
      """
      YIELD hash("Tim Duncan") as vid | GO FROM $-.vid OVER serve
      """
    Then the result should be, in any order, with relax comparison, and the columns 0 should be hashed:
      | serve._dst |
      | "Spurs"    |
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
    Then the result should be, in any order, with relax comparison, and the columns 0 should be hashed:
      | serve._dst      |
      | "Spurs"         |
      | "Spurs"         |
      | "Spurs"         |
      | "Spurs"         |
      | "Spurs"         |
      | "Hornets"       |
      | "Trail Blazers" |
    When executing query:
      """
      GO FROM hash('Thunders') OVER serve REVERSELY
      """
    Then the result should be, in any order, with relax comparison, and the columns 0 should be hashed:
      | serve._dst          |
      | "Russell Westbrook" |
      | "Kevin Durant"      |
      | "James Harden"      |
      | "Carmelo Anthony"   |
      | "Paul George"       |
      | "Ray Allen"         |
    When executing query:
      """
      GO FROM hash("Boris Diaw") OVER like YIELD like._dst as id
      |(GO FROM $-.id OVER like YIELD like._dst as id | GO FROM $-.id OVER serve)
      """
    Then the result should be, in any order, with relax comparison, and the columns 0 should be hashed:
      | serve._dst      |
      | "Spurs"         |
      | "Spurs"         |
      | "Spurs"         |
      | "Spurs"         |
      | "Spurs"         |
      | "Hornets"       |
      | "Trail Blazers" |
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
    Then the result should be, in any order, with relax comparison, and the columns 0 should be hashed:
      | serve._dst      |
      | "Spurs"         |
      | "Spurs"         |
      | "Spurs"         |
      | "Spurs"         |
      | "Spurs"         |
      | "Hornets"       |
      | "Trail Blazers" |

  Scenario: Integer Vid assignment simple
    When executing query:
      """
      $var = GO FROM hash("Tracy McGrady") OVER like YIELD like._dst as id; GO FROM $var.id OVER like
      """
    Then the result should be, in any order, with relax comparison, and the columns 0 should be hashed:
      | like._dst           |
      | "Tracy McGrady"     |
      | "LaMarcus Aldridge" |

  Scenario: Integer Vid assignment pipe
    When executing query:
      """
      $var = (GO FROM hash("Tracy McGrady") OVER like YIELD like._dst as id | GO FROM $-.id OVER like YIELD like._dst as id);
      GO FROM $var.id OVER like
      """
    Then the result should be, in any order, with relax comparison, and the columns 0 should be hashed:
      | like._dst     |
      | "Kobe Bryant" |
      | "Grant Hill"  |
      | "Rudy Gay"    |
      | "Tony Parker" |
      | "Tim Duncan"  |

  Scenario: Integer Vid pipe only yield input prop
    When executing query:
      """
      GO FROM hash("Tracy McGrady") OVER like YIELD like._dst as vid | GO FROM $-.vid OVER like YIELD $-.vid as id
      """
    Then the result should be, in any order, with relax comparison, and the columns 0 should be hashed:
      | id           |
      | "Grant Hill" |
      | "Rudy Gay"   |

  Scenario: Integer Vid pipe only yield constant
    When executing query:
      """
      GO FROM hash("Tracy McGrady") OVER like YIELD like._dst as vid | GO FROM $-.vid OVER like YIELD 3
      """
    Then the result should be, in any order, with relax comparison:
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
    Then the result should be, in any order, with relax comparison, and the columns 0 should be hashed:
      | serve._dst      | $$.team.name    |
      | "Spurs"         | "Spurs"         |
      | "Hornets"       | "Hornets"       |
      | "Trail Blazers" | "Trail Blazers" |
    When executing query:
      """
      GO 2 STEPS FROM hash("Tony Parker") OVER like YIELD DISTINCT like._dst
      """
    Then the result should be, in any order, with relax comparison, and the columns 0 should be hashed:
      | like._dst       |
      | "Tim Duncan"    |
      | "Tony Parker"   |
      | "Manu Ginobili" |

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
    Then the result should be, in any order, with relax comparison, and the columns 0,1 should be hashed:
      | serve._dst | like._dst         |
      | EMPTY      | "James Harden"    |
      | EMPTY      | "Dejounte Murray" |
      | EMPTY      | "Paul George"     |
    When executing query:
      """
      GO FROM hash("Russell Westbrook") OVER * REVERSELY YIELD serve._src, like._src
      """
    Then the result should be, in any order, with relax comparison, and the columns 0,1 should be hashed:
      | serve._src | like._src           |
      | EMPTY      | "Russell Westbrook" |
      | EMPTY      | "Russell Westbrook" |
      | EMPTY      | "Russell Westbrook" |
    When executing query:
      """
      GO FROM hash("Russell Westbrook") OVER * REVERSELY
      """
    Then the result should be, in any order, with relax comparison, and the columns 0,1,2 should be hashed:
      | like._dst         | serve._dst | teammate._dst |
      | "James Harden"    | EMPTY      | EMPTY         |
      | "Dejounte Murray" | EMPTY      | EMPTY         |
      | "Paul George"     | EMPTY      | EMPTY         |
    When executing query:
      """
      GO FROM hash("Dirk Nowitzki") OVER * YIELD serve._dst, like._dst
      """
    Then the result should be, in any order, with relax comparison, and the columns 0,1 should be hashed:
      | serve._dst  | like._dst     |
      | "Mavericks" | EMPTY         |
      | EMPTY       | "Steve Nash"  |
      | EMPTY       | "Jason Kidd"  |
      | EMPTY       | "Dwyane Wade" |
    When executing query:
      """
      GO FROM hash("Paul Gasol") OVER *
      """
    Then the result should be, in any order, with relax comparison, and the columns 0,1,2 should be hashed:
      | like._dst     | serve._dst  | teammate._dst |
      | EMPTY         | "Grizzlies" | EMPTY         |
      | EMPTY         | "Lakers"    | EMPTY         |
      | EMPTY         | "Bulls"     | EMPTY         |
      | EMPTY         | "Spurs"     | EMPTY         |
      | EMPTY         | "Bucks"     | EMPTY         |
      | "Kobe Bryant" | EMPTY       | EMPTY         |
      | "Marc Gasol"  | EMPTY       | EMPTY         |
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
    Then the result should be, in any order, with relax comparison, and the columns 0 should be hashed:
      | serve._dst      |
      | "Spurs"         |
      | "Spurs"         |
      | "Spurs"         |
      | "Spurs"         |
      | "Spurs"         |
      | "Hornets"       |
      | "Trail Blazers" |

  Scenario: Integer Vid edge type
    When executing query:
      """
      YIELD serve.start_year, like.likeness, serve._type, like._type
      """
    Then a SemanticError should be raised at runtime: Invalid label identifiers: serve
    When executing query:
      """
      GO FROM hash("Russell Westbrook") OVER serve, like REVERSELY
      YIELD serve._dst, like._dst, serve._type, like._type
      """
    Then the result should be, in any order, with relax comparison, and the columns 0,1 should be hashed:
      | serve._dst | like._dst         | serve._type | like._type |
      | EMPTY      | "James Harden"    | EMPTY       | /-?\d+/    |
      | EMPTY      | "Dejounte Murray" | EMPTY       | /-?\d+/    |
      | EMPTY      | "Paul George"     | EMPTY       | /-?\d+/    |

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
    Then the result should be, in any order, with relax comparison, and the columns 0,1 should be hashed:
      | serve._dst  | like._dst      |
      | "Magic"     | EMPTY          |
      | "Lakers"    | EMPTY          |
      | "Heat"      | EMPTY          |
      | "Suns"      | EMPTY          |
      | "Cavaliers" | EMPTY          |
      | "Celtics"   | EMPTY          |
      | EMPTY       | "JaVale McGee" |
      | EMPTY       | "Tim Duncan"   |
    When executing query:
      """
      GO FROM hash('Russell Westbrook') OVER serve, like
      """
    Then the result should be, in any order, with relax comparison, and the columns 0,1 should be hashed:
      | serve._dst | like._dst      |
      | "Thunders" | EMPTY          |
      | EMPTY      | "Paul George"  |
      | EMPTY      | "James Harden" |
    When executing query:
      """
      GO FROM hash("Russell Westbrook") OVER serve, like REVERSELY
      YIELD serve._dst, like._dst, serve.start_year, like.likeness
      """
    Then the result should be, in any order, with relax comparison, and the columns 0,1 should be hashed:
      | serve._dst | like._dst         | serve.start_year | like.likeness |
      | EMPTY      | "James Harden"    | EMPTY            | 80            |
      | EMPTY      | "Dejounte Murray" | EMPTY            | 99            |
      | EMPTY      | "Paul George"     | EMPTY            | 95            |
    When executing query:
      """
      GO FROM hash("Russell Westbrook") OVER serve, like REVERSELY YIELD serve._src, like._src
      """
    Then the result should be, in any order, with relax comparison, and the columns 0,1 should be hashed:
      | serve._src | like._src           |
      | EMPTY      | "Russell Westbrook" |
      | EMPTY      | "Russell Westbrook" |
      | EMPTY      | "Russell Westbrook" |
    When executing query:
      """
      GO FROM hash("Russell Westbrook") OVER serve, like REVERSELY
      """
    Then the result should be, in any order, with relax comparison, and the columns 0,1 should be hashed:
      | serve._dst | like._dst         |
      | EMPTY      | "James Harden"    |
      | EMPTY      | "Dejounte Murray" |
      | EMPTY      | "Paul George"     |
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

  Scenario: Integer Vid udf call
    When executing query:
      """
      GO FROM hash('Boris Diaw') OVER serve WHERE $$.team.name IN ['Hawks', 'Suns']
      YIELD $^.player.name, serve.start_year, serve.end_year, $$.team.name
      """
    Then the result should be, in any order, with relax comparison:
      | $^.player.name | serve.start_year | serve.end_year | $$.team.name |
      | "Boris Diaw"   | 2003             | 2005           | "Hawks"      |
      | "Boris Diaw"   | 2005             | 2008           | "Suns"       |
    When executing query:
      """
      GO FROM hash('Tim Duncan') OVER like YIELD like._dst AS id |
      GO FROM  $-.id OVER serve WHERE $-.id IN [hash('Tony Parker'), 123]
      """
    Then the result should be, in any order, with relax comparison, and the columns 0 should be hashed:
      | serve._dst |
      | "Spurs"    |
      | "Hornets"  |
    When executing query:
      """
      GO FROM hash('Tim Duncan') OVER like YIELD like._dst AS id |
      GO FROM  $-.id OVER serve WHERE $-.id IN [hash('Tony Parker'), 123] AND 1 == 1
      """
    Then the result should be, in any order, with relax comparison, and the columns 0 should be hashed:
      | serve._dst |
      | "Spurs"    |
      | "Hornets"  |

  @skip
  Scenario: Integer Vid return test (reason = "return not implement")
    When executing query:
      """
      $A = GO FROM hash('Tim Duncan') OVER like YIELD like._dst AS dst;
      $rA = YIELD $A.* WHERE $A.dst == 123;
      RETURN $rA IF $rA IS NOT NULL;
      GO FROM $A.dst OVER serve
      """
    Then the result should be, in any order, with relax comparison, and the columns 0 should be hashed:
      | serve._dst |
      | "Spurs"    |
      | "Spurs"    |
      | "Hornets"  |
    When executing query:
      """
      $A = GO FROM hash('Tim Duncan') OVER like YIELD like._dst AS dst;
      $rA = YIELD $A.* WHERE 1 == 1;
      RETURN $rA IF $rA IS NOT NULL;
      GO FROM $A.dst OVER serve
      """
    Then the result should be, in any order, with relax comparison, and the columns 0 should be hashed:
      | serve._dst      |
      | "Tony Parker"   |
      | "Manu Ginobili" |
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
    Then the result should be, in any order, with relax comparison, and the columns 0 should be hashed:
      | serve._dst      |
      | "Spurs"         |
      | "Spurs"         |
      | "Trail Blazers" |
      | "Spurs"         |
      | "Spurs"         |
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
    Then the result should be, in any order, with relax comparison, and the columns 0 should be hashed:
      | serve._dst      |
      | "Tony Parker"   |
      | "Manu Ginobili" |
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
    Then the result should be, in any order, with relax comparison, and the columns 0 should be hashed:
      | like._dst           |
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
    Then the result should be, in any order, with relax comparison, and the columns 0 should be hashed:
      | like._dst           |
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
      | EMPTY               |
      | EMPTY               |

  Scenario: Integer Vid only id n steps
    When executing query:
      """
      GO 2 STEPS FROM hash('Tony Parker') OVER like YIELD like._dst
      """
    Then the result should be, in any order, with relax comparison, and the columns 0 should be hashed:
      | like._dst       |
      | "Tim Duncan"    |
      | "Tim Duncan"    |
      | "Tony Parker"   |
      | "Tony Parker"   |
      | "Manu Ginobili" |
    When executing query:
      """
      GO 3 STEPS FROM hash('Tony Parker') OVER like YIELD like._dst
      """
    Then the result should be, in any order, with relax comparison, and the columns 0 should be hashed:
      | like._dst           |
      | "Tony Parker"       |
      | "Manu Ginobili"     |
      | "Manu Ginobili"     |
      | "Tim Duncan"        |
      | "Tim Duncan"        |
      | "LaMarcus Aldridge" |
    When executing query:
      """
      GO 1 STEPS FROM hash('Tony Parker') OVER like YIELD like._dst AS id
      | GO 2 STEPS FROM $-.id OVER like YIELD like._dst
      """
    Then the result should be, in any order, with relax comparison, and the columns 0 should be hashed:
      | like._dst           |
      | "LaMarcus Aldridge" |
      | "LaMarcus Aldridge" |
      | "Manu Ginobili"     |
      | "Manu Ginobili"     |
      | "Tim Duncan"        |
      | "Tim Duncan"        |
      | "Tim Duncan"        |
      | "Manu Ginobili"     |
      | "Manu Ginobili"     |
      | "Tony Parker"       |
      | "Tony Parker"       |

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
    Then the result should be, in any order, with relax comparison, and the columns 0 should be hashed:
      | serve._dst |
      | "Spurs"    |
      | "Spurs"    |
      | "Hornets"  |
      | "Spurs"    |
      | "Hawks"    |
      | "76ers"    |
      | "Spurs"    |

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
    Then the result should be, in any order, with relax comparison, and the columns 0 should be hashed:
      | serve._dst |
      | "Spurs"    |
      | "Spurs"    |
      | "Spurs"    |
      | "Spurs"    |
      | "Hornets"  |
      | "Hawks"    |
      | "76ers"    |

  Scenario: Integer Vid bidirect
    When executing query:
      """
      GO FROM hash('Tim Duncan') OVER serve bidirect
      """
    Then the result should be, in any order, with relax comparison, and the columns 0 should be hashed:
      | serve._dst |
      | "Spurs"    |
    When executing query:
      """
      GO FROM hash('Tim Duncan') OVER like bidirect
      """
    Then the result should be, in any order, with relax comparison, and the columns 0 should be hashed:
      | like._dst           |
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
      GO FROM hash('Tim Duncan') OVER serve, like bidirect
      """
    Then the result should be, in any order, with relax comparison, and the columns 0,1 should be hashed:
      | serve._dst | like._dst           |
      | "Spurs"    | EMPTY               |
      | EMPTY      | "Tony Parker"       |
      | EMPTY      | "Manu Ginobili"     |
      | EMPTY      | "Tony Parker"       |
      | EMPTY      | "Manu Ginobili"     |
      | EMPTY      | "LaMarcus Aldridge" |
      | EMPTY      | "Marco Belinelli"   |
      | EMPTY      | "Danny Green"       |
      | EMPTY      | "Aron Baynes"       |
      | EMPTY      | "Boris Diaw"        |
      | EMPTY      | "Tiago Splitter"    |
      | EMPTY      | "Dejounte Murray"   |
      | EMPTY      | "Shaquile O'Neal"   |
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
    Then the result should be, in any order, with relax comparison, and the columns 1 should be hashed:
      | $^.player.name | like._dst         | $$.player.name    | like.likeness |
      | "Tim Duncan"   | "Tony Parker"     | "Tony Parker"     | 95            |
      | "Tim Duncan"   | "Manu Ginobili"   | "Manu Ginobili"   | 95            |
      | "Tim Duncan"   | "Tony Parker"     | "Tony Parker"     | 95            |
      | "Tim Duncan"   | "Dejounte Murray" | "Dejounte Murray" | 99            |

  Scenario: Integer Vid bidirect_over_all
    When executing query:
      """
      GO FROM hash('Tim Duncan') OVER * bidirect YIELD $^.player.name, serve._dst, $$.team.name, like._dst, $$.player.name
      """
    Then the result should be, in any order, with relax comparison, and the columns 1,3 should be hashed:
      | $^.player.name | serve._dst | $$.team.name | like._dst           | $$.player.name      |
      | "Tim Duncan"   | "Spurs"    | "Spurs"      | EMPTY               | EMPTY               |
      | "Tim Duncan"   | EMPTY      | EMPTY        | "Tony Parker"       | "Tony Parker"       |
      | "Tim Duncan"   | EMPTY      | EMPTY        | "Manu Ginobili"     | "Manu Ginobili"     |
      | "Tim Duncan"   | EMPTY      | EMPTY        | "Tony Parker"       | "Tony Parker"       |
      | "Tim Duncan"   | EMPTY      | EMPTY        | "Manu Ginobili"     | "Manu Ginobili"     |
      | "Tim Duncan"   | EMPTY      | EMPTY        | "LaMarcus Aldridge" | "LaMarcus Aldridge" |
      | "Tim Duncan"   | EMPTY      | EMPTY        | "Marco Belinelli"   | "Marco Belinelli"   |
      | "Tim Duncan"   | EMPTY      | EMPTY        | "Danny Green"       | "Danny Green"       |
      | "Tim Duncan"   | EMPTY      | EMPTY        | "Aron Baynes"       | "Aron Baynes"       |
      | "Tim Duncan"   | EMPTY      | EMPTY        | "Boris Diaw"        | "Boris Diaw"        |
      | "Tim Duncan"   | EMPTY      | EMPTY        | "Tiago Splitter"    | "Tiago Splitter"    |
      | "Tim Duncan"   | EMPTY      | EMPTY        | "Dejounte Murray"   | "Dejounte Murray"   |
      | "Tim Duncan"   | EMPTY      | EMPTY        | "Shaquile O'Neal"   | "Shaquile O'Neal"   |
      | "Tim Duncan"   | EMPTY      | EMPTY        | EMPTY               | "Tony Parker"       |
      | "Tim Duncan"   | EMPTY      | EMPTY        | EMPTY               | "Manu Ginobili"     |
      | "Tim Duncan"   | EMPTY      | EMPTY        | EMPTY               | "Danny Green"       |
      | "Tim Duncan"   | EMPTY      | EMPTY        | EMPTY               | "LaMarcus Aldridge" |
      | "Tim Duncan"   | EMPTY      | EMPTY        | EMPTY               | "Tony Parker"       |
      | "Tim Duncan"   | EMPTY      | EMPTY        | EMPTY               | "Manu Ginobili"     |
    When executing query:
      """
      GO FROM hash('Tim Duncan') OVER * bidirect
      """
    Then the result should be, in any order, with relax comparison, and the columns 0,1,2 should be hashed:
      | like._dst           | serve._dst | teammate._dst       |
      | EMPTY               | "Spurs"    | EMPTY               |
      | "Tony Parker"       | EMPTY      | EMPTY               |
      | "Manu Ginobili"     | EMPTY      | EMPTY               |
      | "Tony Parker"       | EMPTY      | EMPTY               |
      | "Manu Ginobili"     | EMPTY      | EMPTY               |
      | "LaMarcus Aldridge" | EMPTY      | EMPTY               |
      | "Marco Belinelli"   | EMPTY      | EMPTY               |
      | "Danny Green"       | EMPTY      | EMPTY               |
      | "Aron Baynes"       | EMPTY      | EMPTY               |
      | "Boris Diaw"        | EMPTY      | EMPTY               |
      | "Tiago Splitter"    | EMPTY      | EMPTY               |
      | "Dejounte Murray"   | EMPTY      | EMPTY               |
      | "Shaquile O'Neal"   | EMPTY      | EMPTY               |
      | EMPTY               | EMPTY      | "Tony Parker"       |
      | EMPTY               | EMPTY      | "Manu Ginobili"     |
      | EMPTY               | EMPTY      | "LaMarcus Aldridge" |
      | EMPTY               | EMPTY      | "Danny Green"       |
      | EMPTY               | EMPTY      | "Tony Parker"       |
      | EMPTY               | EMPTY      | "Manu Ginobili"     |

  Scenario: Integer Vid duplicate column name
    When executing query:
      """
      GO FROM hash('Tim Duncan') OVER serve YIELD serve._dst, serve._dst
      """
    Then the result should be, in any order, with relax comparison, and the columns 0,1 should be hashed:
      | serve._dst | serve._dst |
      | "Spurs"    | "Spurs"    |
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
    Then the result should be, in any order, with relax comparison, and the columns 0 should be hashed:
      | like._dst           |
      | "Tony Parker"       |
      | "Manu Ginobili"     |
      | "LaMarcus Aldridge" |
      | "Tim Duncan"        |
    When executing query:
      """
      GO 0 TO 2 STEPS FROM hash('Tony Parker') OVER like YIELD DISTINCT like._dst
      """
    Then the result should be, in any order, with relax comparison, and the columns 0 should be hashed:
      | like._dst           |
      | "Tony Parker"       |
      | "Manu Ginobili"     |
      | "LaMarcus Aldridge" |
      | "Tim Duncan"        |
    When executing query:
      """
      GO 1 TO 2 STEPS FROM hash('Tony Parker') OVER like YIELD DISTINCT like._dst, like.likeness, $$.player.name
      """
    Then the result should be, in any order, with relax comparison, and the columns 0 should be hashed:
      | like._dst           | like.likeness | $$.player.name      |
      | "Manu Ginobili"     | 95            | "Manu Ginobili"     |
      | "LaMarcus Aldridge" | 90            | "LaMarcus Aldridge" |
      | "Tim Duncan"        | 95            | "Tim Duncan"        |
      | "Tony Parker"       | 95            | "Tony Parker"       |
      | "Tony Parker"       | 75            | "Tony Parker"       |
      | "Tim Duncan"        | 75            | "Tim Duncan"        |
      | "Tim Duncan"        | 90            | "Tim Duncan"        |
    When executing query:
      """
      GO 0 TO 2 STEPS FROM hash('Tony Parker') OVER like YIELD DISTINCT like._dst, like.likeness, $$.player.name
      """
    Then the result should be, in any order, with relax comparison, and the columns 0 should be hashed:
      | like._dst           | like.likeness | $$.player.name      |
      | "Manu Ginobili"     | 95            | "Manu Ginobili"     |
      | "LaMarcus Aldridge" | 90            | "LaMarcus Aldridge" |
      | "Tim Duncan"        | 95            | "Tim Duncan"        |
      | "Tony Parker"       | 95            | "Tony Parker"       |
      | "Tony Parker"       | 75            | "Tony Parker"       |
      | "Tim Duncan"        | 75            | "Tim Duncan"        |
      | "Tim Duncan"        | 90            | "Tim Duncan"        |
    When executing query:
      """
      GO 0 TO 3 STEPS FROM hash('Tim Duncan') OVER serve
      """
    Then the result should be, in any order, with relax comparison, and the columns 0 should be hashed:
      | serve._dst |
      | "Spurs"    |
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
    Then the result should be, in any order, with relax comparison, and the columns 0 should be hashed:
      | like._dst           |
      | "Tim Duncan"        |
      | "LaMarcus Aldridge" |
      | "Marco Belinelli"   |
      | "Boris Diaw"        |
      | "Dejounte Murray"   |
      | "Tony Parker"       |
      | "Manu Ginobili"     |
      | "Danny Green"       |
      | "Aron Baynes"       |
      | "Tiago Splitter"    |
      | "Shaquile O'Neal"   |
      | "Rudy Gay"          |
      | "Damian Lillard"    |
    When executing query:
      """
      GO 0 TO 2 STEPS FROM hash('Tony Parker') OVER like REVERSELY YIELD DISTINCT like._dst
      """
    Then the result should be, in any order, with relax comparison, and the columns 0 should be hashed:
      | like._dst           |
      | "Tim Duncan"        |
      | "LaMarcus Aldridge" |
      | "Marco Belinelli"   |
      | "Boris Diaw"        |
      | "Dejounte Murray"   |
      | "Tony Parker"       |
      | "Manu Ginobili"     |
      | "Danny Green"       |
      | "Aron Baynes"       |
      | "Tiago Splitter"    |
      | "Shaquile O'Neal"   |
      | "Rudy Gay"          |
      | "Damian Lillard"    |
    When executing query:
      """
      GO 2 TO 2 STEPS FROM hash('Tony Parker') OVER like REVERSELY YIELD DISTINCT like._dst
      """
    Then the result should be, in any order, with relax comparison, and the columns 0 should be hashed:
      | like._dst           |
      | "LaMarcus Aldridge" |
      | "Marco Belinelli"   |
      | "Boris Diaw"        |
      | "Dejounte Murray"   |
      | "Tony Parker"       |
      | "Manu Ginobili"     |
      | "Danny Green"       |
      | "Aron Baynes"       |
      | "Tiago Splitter"    |
      | "Shaquile O'Neal"   |
      | "Rudy Gay"          |
      | "Damian Lillard"    |
    When executing query:
      """
      GO 1 TO 3 STEPS FROM hash('Spurs') OVER serve REVERSELY
      """
    Then the result should be, in any order, with relax comparison, and the columns 0 should be hashed:
      | serve._dst          |
      | "Tim Duncan"        |
      | "Tony Parker"       |
      | "Manu Ginobili"     |
      | "LaMarcus Aldridge" |
      | "Rudy Gay"          |
      | "Marco Belinelli"   |
      | "Danny Green"       |
      | "Kyle Anderson"     |
      | "Aron Baynes"       |
      | "Boris Diaw"        |
      | "Tiago Splitter"    |
      | "Cory Joseph"       |
      | "David West"        |
      | "Jonathon Simmons"  |
      | "Dejounte Murray"   |
      | "Tracy McGrady"     |
      | "Paul Gasol"        |
      | "Marco Belinelli"   |
    When executing query:
      """
      GO 0 TO 3 STEPS FROM hash('Spurs') OVER serve REVERSELY
      """
    Then the result should be, in any order, with relax comparison, and the columns 0 should be hashed:
      | serve._dst          |
      | "Tim Duncan"        |
      | "Tony Parker"       |
      | "Manu Ginobili"     |
      | "LaMarcus Aldridge" |
      | "Rudy Gay"          |
      | "Marco Belinelli"   |
      | "Danny Green"       |
      | "Kyle Anderson"     |
      | "Aron Baynes"       |
      | "Boris Diaw"        |
      | "Tiago Splitter"    |
      | "Cory Joseph"       |
      | "David West"        |
      | "Jonathon Simmons"  |
      | "Dejounte Murray"   |
      | "Tracy McGrady"     |
      | "Paul Gasol"        |
      | "Marco Belinelli"   |
    When executing query:
      """
      GO 1 TO 2 STEPS FROM hash('Tony Parker') OVER like BIDIRECT YIELD DISTINCT like._dst
      """
    Then the result should be, in any order, with relax comparison, and the columns 0 should be hashed:
      | like._dst           |
      | "Tim Duncan"        |
      | "LaMarcus Aldridge" |
      | "Marco Belinelli"   |
      | "Boris Diaw"        |
      | "Dejounte Murray"   |
      | "Tony Parker"       |
      | "Manu Ginobili"     |
      | "Danny Green"       |
      | "Aron Baynes"       |
      | "Tiago Splitter"    |
      | "Shaquile O'Neal"   |
      | "Rudy Gay"          |
      | "Damian Lillard"    |
      | "LeBron James"      |
      | "Russell Westbrook" |
      | "Chris Paul"        |
      | "Kyle Anderson"     |
      | "Kevin Durant"      |
      | "James Harden"      |
    When executing query:
      """
      GO 0 TO 2 STEPS FROM hash('Tony Parker') OVER like BIDIRECT YIELD DISTINCT like._dst
      """
    Then the result should be, in any order, with relax comparison, and the columns 0 should be hashed:
      | like._dst           |
      | "Tim Duncan"        |
      | "LaMarcus Aldridge" |
      | "Marco Belinelli"   |
      | "Boris Diaw"        |
      | "Dejounte Murray"   |
      | "Tony Parker"       |
      | "Manu Ginobili"     |
      | "Danny Green"       |
      | "Aron Baynes"       |
      | "Tiago Splitter"    |
      | "Shaquile O'Neal"   |
      | "Rudy Gay"          |
      | "Damian Lillard"    |
      | "LeBron James"      |
      | "Russell Westbrook" |
      | "Chris Paul"        |
      | "Kyle Anderson"     |
      | "Kevin Durant"      |
      | "James Harden"      |
    When executing query:
      """
      GO 1 TO 2 STEPS FROM hash('Russell Westbrook') OVER * YIELD serve._dst, like._dst
      """
    Then the result should be, in any order, with relax comparison, and the columns 0,1 should be hashed:
      | serve._dst | like._dst           |
      | "Thunders" | EMPTY               |
      | EMPTY      | "Paul George"       |
      | EMPTY      | "James Harden"      |
      | "Pacers"   | EMPTY               |
      | "Thunders" | EMPTY               |
      | EMPTY      | "Russell Westbrook" |
      | "Thunders" | EMPTY               |
      | "Rockets"  | EMPTY               |
      | EMPTY      | "Russell Westbrook" |
    When executing query:
      """
      GO 0 TO 2 STEPS FROM hash('Russell Westbrook') OVER * YIELD serve._dst, like._dst
      """
    Then the result should be, in any order, with relax comparison, and the columns 0,1 should be hashed:
      | serve._dst | like._dst           |
      | "Thunders" | EMPTY               |
      | EMPTY      | "Paul George"       |
      | EMPTY      | "James Harden"      |
      | "Pacers"   | EMPTY               |
      | "Thunders" | EMPTY               |
      | EMPTY      | "Russell Westbrook" |
      | "Thunders" | EMPTY               |
      | "Rockets"  | EMPTY               |
      | EMPTY      | "Russell Westbrook" |
    When executing query:
      """
      GO 1 TO 2 STEPS FROM hash('Russell Westbrook') OVER *
      YIELD serve._dst, like._dst, serve.start_year, like.likeness, $$.player.name
      """
    Then the result should be, in any order, with relax comparison, and the columns 0,1 should be hashed:
      | serve._dst | like._dst           | serve.start_year | like.likeness | $$.player.name      |
      | "Thunders" | EMPTY               | 2008             | EMPTY         | EMPTY               |
      | EMPTY      | "Paul George"       | EMPTY            | 90            | "Paul George"       |
      | EMPTY      | "James Harden"      | EMPTY            | 90            | "James Harden"      |
      | "Pacers"   | EMPTY               | 2010             | EMPTY         | EMPTY               |
      | "Thunders" | EMPTY               | 2017             | EMPTY         | EMPTY               |
      | EMPTY      | "Russell Westbrook" | EMPTY            | 95            | "Russell Westbrook" |
      | "Thunders" | EMPTY               | 2009             | EMPTY         | EMPTY               |
      | "Rockets"  | EMPTY               | 2012             | EMPTY         | EMPTY               |
      | EMPTY      | "Russell Westbrook" | EMPTY            | 80            | "Russell Westbrook" |
    When executing query:
      """
      GO 0 TO 2 STEPS FROM hash('Russell Westbrook') OVER *
      YIELD serve._dst, like._dst, serve.start_year, like.likeness, $$.player.name
      """
    Then the result should be, in any order, with relax comparison, and the columns 0,1 should be hashed:
      | serve._dst | like._dst           | serve.start_year | like.likeness | $$.player.name      |
      | "Thunders" | EMPTY               | 2008             | EMPTY         | EMPTY               |
      | EMPTY      | "Paul George"       | EMPTY            | 90            | "Paul George"       |
      | EMPTY      | "James Harden"      | EMPTY            | 90            | "James Harden"      |
      | "Pacers"   | EMPTY               | 2010             | EMPTY         | EMPTY               |
      | "Thunders" | EMPTY               | 2017             | EMPTY         | EMPTY               |
      | EMPTY      | "Russell Westbrook" | EMPTY            | 95            | "Russell Westbrook" |
      | "Thunders" | EMPTY               | 2009             | EMPTY         | EMPTY               |
      | "Rockets"  | EMPTY               | 2012             | EMPTY         | EMPTY               |
      | EMPTY      | "Russell Westbrook" | EMPTY            | 80            | "Russell Westbrook" |
    When executing query:
      """
      GO 1 TO 2 STEPS FROM hash('Russell Westbrook') OVER * REVERSELY YIELD serve._dst, like._dst
      """
    Then the result should be, in any order, with relax comparison, and the columns 0,1 should be hashed:
      | serve._dst | like._dst           |
      | EMPTY      | "Dejounte Murray"   |
      | EMPTY      | "James Harden"      |
      | EMPTY      | "Paul George"       |
      | EMPTY      | "Dejounte Murray"   |
      | EMPTY      | "Russell Westbrook" |
      | EMPTY      | "Luka Doncic"       |
      | EMPTY      | "Russell Westbrook" |
    When executing query:
      """
      GO 0 TO 2 STEPS FROM hash('Russell Westbrook') OVER * REVERSELY YIELD serve._dst, like._dst
      """
    Then the result should be, in any order, with relax comparison, and the columns 0,1 should be hashed:
      | serve._dst | like._dst           |
      | EMPTY      | "Dejounte Murray"   |
      | EMPTY      | "James Harden"      |
      | EMPTY      | "Paul George"       |
      | EMPTY      | "Dejounte Murray"   |
      | EMPTY      | "Russell Westbrook" |
      | EMPTY      | "Luka Doncic"       |
      | EMPTY      | "Russell Westbrook" |

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
    Then the result should be, in any order, with relax comparison, and the columns 0,1 should be hashed:
      | src          | dst             | $^.player.name | $$.player.name  |
      | "Tim Duncan" | "Tony Parker"   | "Tim Duncan"   | "Tony Parker"   |
      | "Tim Duncan" | "Manu Ginobili" | "Tim Duncan"   | "Manu Ginobili" |
      | "Tim Duncan" | "Tony Parker"   | "Tim Duncan"   | "Tony Parker"   |
      | "Tim Duncan" | "Manu Ginobili" | "Tim Duncan"   | "Manu Ginobili" |
    When executing query:
      """
      $a = GO FROM hash('Tim Duncan') OVER like YIELD like._src as src, like._dst as dst;
      GO FROM $a.src OVER like YIELD $a.src as src, like._dst as dst
      """
    Then the result should be, in any order, with relax comparison, and the columns 0,1 should be hashed:
      | src          | dst             |
      | "Tim Duncan" | "Tony Parker"   |
      | "Tim Duncan" | "Manu Ginobili" |
      | "Tim Duncan" | "Tony Parker"   |
      | "Tim Duncan" | "Manu Ginobili" |
    When executing query:
      """
      GO FROM hash('Tim Duncan') OVER like YIELD like._src as src, like._dst as dst
      | GO 1 TO 2 STEPS FROM $-.src OVER like YIELD $-.src as src, like._dst as dst
      """
    Then the result should be, in any order, with relax comparison, and the columns 0,1 should be hashed:
      | src          | dst                 |
      | "Tim Duncan" | "Tony Parker"       |
      | "Tim Duncan" | "Manu Ginobili"     |
      | "Tim Duncan" | "Tony Parker"       |
      | "Tim Duncan" | "Manu Ginobili"     |
      | "Tim Duncan" | "Tim Duncan"        |
      | "Tim Duncan" | "Tim Duncan"        |
      | "Tim Duncan" | "Manu Ginobili"     |
      | "Tim Duncan" | "Manu Ginobili"     |
      | "Tim Duncan" | "LaMarcus Aldridge" |
      | "Tim Duncan" | "LaMarcus Aldridge" |
      | "Tim Duncan" | "Tim Duncan"        |
      | "Tim Duncan" | "Tim Duncan"        |
    When executing query:
      """
      GO FROM hash('Tim Duncan') OVER like YIELD like._src as src, like._dst as dst
      | GO 1 TO 2 STEPS FROM $-.src OVER like
      YIELD $-.src as src, $-.dst, like._dst as dst, like.likeness
      """
    Then the result should be, in any order, with relax comparison, and the columns 0,1,2 should be hashed:
      | src          | $-.dst          | dst                 | like.likeness |
      | "Tim Duncan" | "Tony Parker"   | "Tony Parker"       | 95            |
      | "Tim Duncan" | "Tony Parker"   | "Manu Ginobili"     | 95            |
      | "Tim Duncan" | "Manu Ginobili" | "Tony Parker"       | 95            |
      | "Tim Duncan" | "Manu Ginobili" | "Manu Ginobili"     | 95            |
      | "Tim Duncan" | "Tony Parker"   | "Tim Duncan"        | 95            |
      | "Tim Duncan" | "Tony Parker"   | "Manu Ginobili"     | 95            |
      | "Tim Duncan" | "Tony Parker"   | "LaMarcus Aldridge" | 90            |
      | "Tim Duncan" | "Tony Parker"   | "Tim Duncan"        | 90            |
      | "Tim Duncan" | "Manu Ginobili" | "Tim Duncan"        | 95            |
      | "Tim Duncan" | "Manu Ginobili" | "Manu Ginobili"     | 95            |
      | "Tim Duncan" | "Manu Ginobili" | "LaMarcus Aldridge" | 90            |
      | "Tim Duncan" | "Manu Ginobili" | "Tim Duncan"        | 90            |
    When executing query:
      """
      GO FROM hash('Danny Green') OVER like YIELD like._src AS src, like._dst AS dst
      | GO FROM $-.dst OVER teammate YIELD $-.src AS src, $-.dst, teammate._dst AS dst
      """
    Then the result should be, in any order, with relax comparison, and the columns 0,1,2 should be hashed:
      | src           | $-.dst       | dst                 |
      | "Danny Green" | "Tim Duncan" | "Tony Parker"       |
      | "Danny Green" | "Tim Duncan" | "Manu Ginobili"     |
      | "Danny Green" | "Tim Duncan" | "LaMarcus Aldridge" |
      | "Danny Green" | "Tim Duncan" | "Danny Green"       |
    When executing query:
      """
      $a = GO FROM hash('Danny Green') OVER like YIELD like._src AS src, like._dst AS dst;
      GO FROM $a.dst OVER teammate YIELD $a.src AS src, $a.dst, teammate._dst AS dst
      """
    Then the result should be, in any order, with relax comparison, and the columns 0,1,2 should be hashed:
      | src           | $a.dst       | dst                 |
      | "Danny Green" | "Tim Duncan" | "Tony Parker"       |
      | "Danny Green" | "Tim Duncan" | "Manu Ginobili"     |
      | "Danny Green" | "Tim Duncan" | "LaMarcus Aldridge" |
      | "Danny Green" | "Tim Duncan" | "Danny Green"       |

  Scenario: Integer Vid backtrack overlap
    When executing query:
      """
      GO FROM hash('Tony Parker') OVER like YIELD like._src as src, like._dst as dst
      | GO 2 STEPS FROM $-.src OVER like YIELD $-.src, $-.dst, like._src, like._dst
      """
    Then the result should be, in any order, with relax comparison, and the columns 0,1,2,3 should be hashed:
      | $-.src        | $-.dst              | like._src           | like._dst       |
      | "Tony Parker" | "Tim Duncan"        | "Tim Duncan"        | "Manu Ginobili" |
      | "Tony Parker" | "Tim Duncan"        | "Tim Duncan"        | "Tony Parker"   |
      | "Tony Parker" | "Tim Duncan"        | "LaMarcus Aldridge" | "Tony Parker"   |
      | "Tony Parker" | "Tim Duncan"        | "Manu Ginobili"     | "Tim Duncan"    |
      | "Tony Parker" | "Tim Duncan"        | "LaMarcus Aldridge" | "Tim Duncan"    |
      | "Tony Parker" | "Manu Ginobili"     | "Tim Duncan"        | "Manu Ginobili" |
      | "Tony Parker" | "Manu Ginobili"     | "Tim Duncan"        | "Tony Parker"   |
      | "Tony Parker" | "Manu Ginobili"     | "LaMarcus Aldridge" | "Tony Parker"   |
      | "Tony Parker" | "Manu Ginobili"     | "Manu Ginobili"     | "Tim Duncan"    |
      | "Tony Parker" | "Manu Ginobili"     | "LaMarcus Aldridge" | "Tim Duncan"    |
      | "Tony Parker" | "LaMarcus Aldridge" | "Tim Duncan"        | "Manu Ginobili" |
      | "Tony Parker" | "LaMarcus Aldridge" | "Tim Duncan"        | "Tony Parker"   |
      | "Tony Parker" | "LaMarcus Aldridge" | "LaMarcus Aldridge" | "Tony Parker"   |
      | "Tony Parker" | "LaMarcus Aldridge" | "Manu Ginobili"     | "Tim Duncan"    |
      | "Tony Parker" | "LaMarcus Aldridge" | "LaMarcus Aldridge" | "Tim Duncan"    |
    When executing query:
      """
      $a = GO FROM hash('Tony Parker') OVER like YIELD like._src as src, like._dst as dst;
      GO 2 STEPS FROM $a.src OVER like YIELD $a.src, $a.dst, like._src, like._dst
      """
    Then the result should be, in any order, with relax comparison, and the columns 0,1,2,3 should be hashed:
      | $a.src        | $a.dst              | like._src           | like._dst       |
      | "Tony Parker" | "Tim Duncan"        | "Tim Duncan"        | "Manu Ginobili" |
      | "Tony Parker" | "Tim Duncan"        | "Tim Duncan"        | "Tony Parker"   |
      | "Tony Parker" | "Tim Duncan"        | "LaMarcus Aldridge" | "Tony Parker"   |
      | "Tony Parker" | "Tim Duncan"        | "Manu Ginobili"     | "Tim Duncan"    |
      | "Tony Parker" | "Tim Duncan"        | "LaMarcus Aldridge" | "Tim Duncan"    |
      | "Tony Parker" | "Manu Ginobili"     | "Tim Duncan"        | "Manu Ginobili" |
      | "Tony Parker" | "Manu Ginobili"     | "Tim Duncan"        | "Tony Parker"   |
      | "Tony Parker" | "Manu Ginobili"     | "LaMarcus Aldridge" | "Tony Parker"   |
      | "Tony Parker" | "Manu Ginobili"     | "Manu Ginobili"     | "Tim Duncan"    |
      | "Tony Parker" | "Manu Ginobili"     | "LaMarcus Aldridge" | "Tim Duncan"    |
      | "Tony Parker" | "LaMarcus Aldridge" | "Tim Duncan"        | "Manu Ginobili" |
      | "Tony Parker" | "LaMarcus Aldridge" | "Tim Duncan"        | "Tony Parker"   |
      | "Tony Parker" | "LaMarcus Aldridge" | "LaMarcus Aldridge" | "Tony Parker"   |
      | "Tony Parker" | "LaMarcus Aldridge" | "Manu Ginobili"     | "Tim Duncan"    |
      | "Tony Parker" | "LaMarcus Aldridge" | "LaMarcus Aldridge" | "Tim Duncan"    |

  Scenario: Integer Vid Go and Limit
    When executing query:
      """
      $a = GO FROM hash('Tony Parker') OVER like YIELD like._src as src, like._dst as dst;
      GO 2 STEPS FROM $a.src OVER like YIELD $a.src as src, $a.dst, like._src AS like_src, like._dst AS like_dst
      | ORDER BY $-.src,$-.like_src,$-.like_dst | OFFSET 1 LIMIT 2
      """
    Then the result should be, in any order, with relax comparison, and the columns 0,1,2,3 should be hashed:
      | src           | $a.dst          | like_src            | like_dst      |
      | "Tony Parker" | "Manu Ginobili" | "LaMarcus Aldridge" | "Tony Parker" |
      | "Tony Parker" | "Tim Duncan"    | "LaMarcus Aldridge" | "Tony Parker" |
    When executing query:
      """
      $a = GO FROM hash('Tony Parker') OVER like YIELD like._src as src, like._dst as dst;
      GO 2 STEPS FROM $a.src OVER like YIELD $a.src as src, $a.dst, like._src AS like_src, like._dst AS like_dst
      | ORDER BY $-.src,$-.like_src,$-.like_dst | LIMIT 2 OFFSET 1
      """
    Then the result should be, in any order, with relax comparison, and the columns 0,1,2,3 should be hashed:
      | src           | $a.dst          | like_src            | like_dst      |
      | "Tony Parker" | "Manu Ginobili" | "LaMarcus Aldridge" | "Tony Parker" |
      | "Tony Parker" | "Tim Duncan"    | "LaMarcus Aldridge" | "Tony Parker" |

  Scenario: Integer Vid GroupBy and Count
    When executing query:
      """
      GO 1 TO 2 STEPS FROM hash("Tim Duncan") OVER like WHERE like._dst != hash("YAO MING") YIELD like._dst AS vid
      | GROUP BY $-.vid YIELD 1 AS id | GROUP BY $-.id YIELD COUNT($-.id);
      """
    Then the result should be, in any order, with relax comparison:
      | COUNT($-.id) |
      | 4            |

  Scenario: Integer Vid Bugfix filter not pushdown
    When executing query:
      """
      GO FROM hash("Tim Duncan") OVER like WHERE like._dst == hash("Tony Parker") | limit 10;
      """
    Then the result should be, in any order, with relax comparison, and the columns 0 should be hashed:
      | like._dst     |
      | "Tony Parker" |

  Scenario: Step over end
    When executing query:
      """
      GO 2 STEPS FROM hash("Tim Duncan") OVER serve;
      """
    Then the result should be, in any order:
      | serve._dst |
    When executing query:
      """
      GO 10 STEPS FROM hash("Tim Duncan") OVER serve;
      """
    Then the result should be, in any order:
      | serve._dst |
    When executing query:
      """
      GO 10000000000000 STEPS FROM hash("Tim Duncan") OVER serve;
      """
    Then the result should be, in any order:
      | serve._dst |
    When executing query:
      """
      GO 1 TO 10 STEPS FROM hash("Tim Duncan") OVER serve;
      """
    Then the result should be, in any order, and the columns 0 should be hashed:
      | serve._dst |
      | "Spurs"    |
    When executing query:
      """
      GO 2 TO 10 STEPS FROM hash("Tim Duncan") OVER serve;
      """
    Then the result should be, in any order, and the columns 0 should be hashed:
      | serve._dst |
    When executing query:
      """
      GO 10000000000 TO 10000000002 STEPS FROM hash("Tim Duncan") OVER serve;
      """
    Then the result should be, in any order:
      | serve._dst |

  Scenario: go from vertices looked up by index
    When executing query:
      """
      LOOKUP ON player WHERE player.age == 40 YIELD player.name AS name |
      YIELD hash($-.name) AS name |
      GO 1 TO 2 STEPS FROM $-.name OVER like REVERSELY YIELD like._dst AS dst, $$.player.name AS name
      """
    Then the result should be, in any order, with relax comparison, and the columns 0 should be hashed:
      | dst                  | name                 |
      | "Jason Kidd"         | "Jason Kidd"         |
      | "Luka Doncic"        | "Luka Doncic"        |
      | "Steve Nash"         | "Steve Nash"         |
      | "Paul Gasol"         | "Paul Gasol"         |
      | "Tracy McGrady"      | "Tracy McGrady"      |
      | "Kristaps Porzingis" | "Kristaps Porzingis" |
      | "Dirk Nowitzki"      | "Dirk Nowitzki"      |
      | "Steve Nash"         | "Steve Nash"         |
      | "Vince Carter"       | "Vince Carter"       |
      | "Marc Gasol"         | "Marc Gasol"         |
      | "Amar'e Stoudemire"  | "Amar'e Stoudemire"  |
      | "Dirk Nowitzki"      | "Dirk Nowitzki"      |
      | "Jason Kidd"         | "Jason Kidd"         |
      | "Grant Hill"         | "Grant Hill"         |
      | "Vince Carter"       | "Vince Carter"       |
      | "Yao Ming"           | "Yao Ming"           |
