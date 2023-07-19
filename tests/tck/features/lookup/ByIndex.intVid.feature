# Copyright (c) 2021 vesoft inc. All rights reserved.
#
# This source code is licensed under Apache 2.0 License.
Feature: Lookup by index itself in integer vid

  Scenario: [1] tag index
    Given a graph with space named "nba_int_vid"
    When executing query:
      """
      LOOKUP ON team YIELD id(vertex) as name
      """
    Then the result should be, in any order, and the columns 0 should be hashed:
      | name            |
      | 'Nets'          |
      | 'Pistons'       |
      | 'Bucks'         |
      | 'Mavericks'     |
      | 'Clippers'      |
      | 'Thunders'      |
      | 'Lakers'        |
      | 'Jazz'          |
      | 'Nuggets'       |
      | 'Wizards'       |
      | 'Pacers'        |
      | 'Timberwolves'  |
      | 'Hawks'         |
      | 'Warriors'      |
      | 'Magic'         |
      | 'Rockets'       |
      | 'Pelicans'      |
      | 'Raptors'       |
      | 'Spurs'         |
      | 'Heat'          |
      | 'Grizzlies'     |
      | 'Knicks'        |
      | 'Suns'          |
      | 'Hornets'       |
      | 'Cavaliers'     |
      | 'Kings'         |
      | 'Celtics'       |
      | '76ers'         |
      | 'Trail Blazers' |
      | 'Bulls'         |
    When executing query:
      """
      LOOKUP ON team YIELD id(vertex) as id, team.name AS Name
      """
    Then the result should be, in any order, and the columns 0 should be hashed:
      | id              | Name            |
      | 'Nets'          | 'Nets'          |
      | 'Pistons'       | 'Pistons'       |
      | 'Bucks'         | 'Bucks'         |
      | 'Mavericks'     | 'Mavericks'     |
      | 'Clippers'      | 'Clippers'      |
      | 'Thunders'      | 'Thunders'      |
      | 'Lakers'        | 'Lakers'        |
      | 'Jazz'          | 'Jazz'          |
      | 'Nuggets'       | 'Nuggets'       |
      | 'Wizards'       | 'Wizards'       |
      | 'Pacers'        | 'Pacers'        |
      | 'Timberwolves'  | 'Timberwolves'  |
      | 'Hawks'         | 'Hawks'         |
      | 'Warriors'      | 'Warriors'      |
      | 'Magic'         | 'Magic'         |
      | 'Rockets'       | 'Rockets'       |
      | 'Pelicans'      | 'Pelicans'      |
      | 'Raptors'       | 'Raptors'       |
      | 'Spurs'         | 'Spurs'         |
      | 'Heat'          | 'Heat'          |
      | 'Grizzlies'     | 'Grizzlies'     |
      | 'Knicks'        | 'Knicks'        |
      | 'Suns'          | 'Suns'          |
      | 'Hornets'       | 'Hornets'       |
      | 'Cavaliers'     | 'Cavaliers'     |
      | 'Kings'         | 'Kings'         |
      | 'Celtics'       | 'Celtics'       |
      | '76ers'         | '76ers'         |
      | 'Trail Blazers' | 'Trail Blazers' |
      | 'Bulls'         | 'Bulls'         |

  Scenario: [1] Tag TODO
    Given a graph with space named "nba_int_vid"
    When executing query:
      """
      LOOKUP ON team WHERE 1 + 1 == 2 YIELD vertex as node
      """
    Then a SemanticError should be raised at runtime:
    When executing query:
      """
      LOOKUP ON team WHERE 1 + 1 == 2 YIELD team.name AS Name
      """
    Then a SemanticError should be raised at runtime:
    When executing query:
      """
      LOOKUP ON player WHERE player.age > 9223372036854775807+1 YIELD vertex as node
      """
    Then a SemanticError should be raised at runtime: result of (9223372036854775807+1) cannot be represented as an integer
    When executing query:
      """
      LOOKUP ON player WHERE player.age > -9223372036854775808-1 YIELD vertex as node
      """
    Then a SemanticError should be raised at runtime: result of (-9223372036854775808-1) cannot be represented as an integer

  Scenario: [2] edge index
    Given a graph with space named "nba_int_vid"
    When executing query:
      """
      LOOKUP ON serve YIELD src(edge) as src, dst(edge) as dst, rank(edge) as rank
      """
    Then the result should be, in any order, and the columns 0,1 should be hashed:
      | src                     | dst             | rank |
      | "Amar'e Stoudemire"     | 'Suns'          | 0    |
      | "Amar'e Stoudemire"     | 'Knicks'        | 0    |
      | "Amar'e Stoudemire"     | 'Heat'          | 0    |
      | 'Russell Westbrook'     | 'Thunders'      | 0    |
      | 'James Harden'          | 'Thunders'      | 0    |
      | 'James Harden'          | 'Rockets'       | 0    |
      | 'Kobe Bryant'           | 'Lakers'        | 0    |
      | 'Tracy McGrady'         | 'Raptors'       | 0    |
      | 'Tracy McGrady'         | 'Magic'         | 0    |
      | 'Tracy McGrady'         | 'Rockets'       | 0    |
      | 'Tracy McGrady'         | 'Spurs'         | 0    |
      | 'Chris Paul'            | 'Hornets'       | 0    |
      | 'Chris Paul'            | 'Clippers'      | 0    |
      | 'Chris Paul'            | 'Rockets'       | 0    |
      | 'Boris Diaw'            | 'Hawks'         | 0    |
      | 'Boris Diaw'            | 'Suns'          | 0    |
      | 'Boris Diaw'            | 'Hornets'       | 0    |
      | 'Boris Diaw'            | 'Spurs'         | 0    |
      | 'Boris Diaw'            | 'Jazz'          | 0    |
      | 'LeBron James'          | 'Cavaliers'     | 0    |
      | 'LeBron James'          | 'Heat'          | 0    |
      | 'LeBron James'          | 'Cavaliers'     | 1    |
      | 'LeBron James'          | 'Lakers'        | 0    |
      | 'Klay Thompson'         | 'Warriors'      | 0    |
      | 'Kristaps Porzingis'    | 'Knicks'        | 0    |
      | 'Kristaps Porzingis'    | 'Mavericks'     | 0    |
      | 'Jonathon Simmons'      | 'Spurs'         | 0    |
      | 'Jonathon Simmons'      | 'Magic'         | 0    |
      | 'Jonathon Simmons'      | '76ers'         | 0    |
      | 'Marco Belinelli'       | 'Warriors'      | 0    |
      | 'Marco Belinelli'       | 'Raptors'       | 0    |
      | 'Marco Belinelli'       | 'Hornets'       | 0    |
      | 'Marco Belinelli'       | 'Bulls'         | 0    |
      | 'Marco Belinelli'       | 'Spurs'         | 0    |
      | 'Marco Belinelli'       | 'Kings'         | 0    |
      | 'Marco Belinelli'       | 'Hornets'       | 1    |
      | 'Marco Belinelli'       | 'Hawks'         | 0    |
      | 'Marco Belinelli'       | '76ers'         | 0    |
      | 'Marco Belinelli'       | 'Spurs'         | 1    |
      | 'Luka Doncic'           | 'Mavericks'     | 0    |
      | 'David West'            | 'Hornets'       | 0    |
      | 'David West'            | 'Pacers'        | 0    |
      | 'David West'            | 'Spurs'         | 0    |
      | 'David West'            | 'Warriors'      | 0    |
      | 'Tony Parker'           | 'Spurs'         | 0    |
      | 'Tony Parker'           | 'Hornets'       | 0    |
      | 'Danny Green'           | 'Cavaliers'     | 0    |
      | 'Danny Green'           | 'Spurs'         | 0    |
      | 'Danny Green'           | 'Raptors'       | 0    |
      | 'Rudy Gay'              | 'Grizzlies'     | 0    |
      | 'Rudy Gay'              | 'Raptors'       | 0    |
      | 'Rudy Gay'              | 'Kings'         | 0    |
      | 'Rudy Gay'              | 'Spurs'         | 0    |
      | 'LaMarcus Aldridge'     | 'Trail Blazers' | 0    |
      | 'LaMarcus Aldridge'     | 'Spurs'         | 0    |
      | 'Tim Duncan'            | 'Spurs'         | 0    |
      | 'Kevin Durant'          | 'Thunders'      | 0    |
      | 'Kevin Durant'          | 'Warriors'      | 0    |
      | 'Stephen Curry'         | 'Warriors'      | 0    |
      | 'Ray Allen'             | 'Bucks'         | 0    |
      | 'Ray Allen'             | 'Thunders'      | 0    |
      | 'Ray Allen'             | 'Celtics'       | 0    |
      | 'Ray Allen'             | 'Heat'          | 0    |
      | 'Tiago Splitter'        | 'Spurs'         | 0    |
      | 'Tiago Splitter'        | 'Hawks'         | 0    |
      | 'Tiago Splitter'        | '76ers'         | 0    |
      | 'DeAndre Jordan'        | 'Clippers'      | 0    |
      | 'DeAndre Jordan'        | 'Mavericks'     | 0    |
      | 'DeAndre Jordan'        | 'Knicks'        | 0    |
      | 'Paul Gasol'            | 'Grizzlies'     | 0    |
      | 'Paul Gasol'            | 'Lakers'        | 0    |
      | 'Paul Gasol'            | 'Bulls'         | 0    |
      | 'Paul Gasol'            | 'Spurs'         | 0    |
      | 'Paul Gasol'            | 'Bucks'         | 0    |
      | 'Aron Baynes'           | 'Spurs'         | 0    |
      | 'Aron Baynes'           | 'Pistons'       | 0    |
      | 'Aron Baynes'           | 'Celtics'       | 0    |
      | 'Cory Joseph'           | 'Spurs'         | 0    |
      | 'Cory Joseph'           | 'Raptors'       | 0    |
      | 'Cory Joseph'           | 'Pacers'        | 0    |
      | 'Vince Carter'          | 'Raptors'       | 0    |
      | 'Vince Carter'          | 'Nets'          | 0    |
      | 'Vince Carter'          | 'Magic'         | 0    |
      | 'Vince Carter'          | 'Suns'          | 0    |
      | 'Vince Carter'          | 'Mavericks'     | 0    |
      | 'Vince Carter'          | 'Grizzlies'     | 0    |
      | 'Vince Carter'          | 'Kings'         | 0    |
      | 'Vince Carter'          | 'Hawks'         | 0    |
      | 'Marc Gasol'            | 'Grizzlies'     | 0    |
      | 'Marc Gasol'            | 'Raptors'       | 0    |
      | 'Ricky Rubio'           | 'Timberwolves'  | 0    |
      | 'Ricky Rubio'           | 'Jazz'          | 0    |
      | 'Ben Simmons'           | '76ers'         | 0    |
      | 'Giannis Antetokounmpo' | 'Bucks'         | 0    |
      | 'Rajon Rondo'           | 'Celtics'       | 0    |
      | 'Rajon Rondo'           | 'Mavericks'     | 0    |
      | 'Rajon Rondo'           | 'Kings'         | 0    |
      | 'Rajon Rondo'           | 'Bulls'         | 0    |
      | 'Rajon Rondo'           | 'Pelicans'      | 0    |
      | 'Rajon Rondo'           | 'Lakers'        | 0    |
      | 'Manu Ginobili'         | 'Spurs'         | 0    |
      | 'Kyrie Irving'          | 'Cavaliers'     | 0    |
      | 'Kyrie Irving'          | 'Celtics'       | 0    |
      | 'Carmelo Anthony'       | 'Nuggets'       | 0    |
      | 'Carmelo Anthony'       | 'Knicks'        | 0    |
      | 'Carmelo Anthony'       | 'Thunders'      | 0    |
      | 'Carmelo Anthony'       | 'Rockets'       | 0    |
      | 'Dwyane Wade'           | 'Heat'          | 0    |
      | 'Dwyane Wade'           | 'Bulls'         | 0    |
      | 'Dwyane Wade'           | 'Cavaliers'     | 0    |
      | 'Dwyane Wade'           | 'Heat'          | 1    |
      | 'Joel Embiid'           | '76ers'         | 0    |
      | 'Damian Lillard'        | 'Trail Blazers' | 0    |
      | 'Yao Ming'              | 'Rockets'       | 0    |
      | 'Kyle Anderson'         | 'Spurs'         | 0    |
      | 'Kyle Anderson'         | 'Grizzlies'     | 0    |
      | 'Dejounte Murray'       | 'Spurs'         | 0    |
      | 'Blake Griffin'         | 'Clippers'      | 0    |
      | 'Blake Griffin'         | 'Pistons'       | 0    |
      | 'Steve Nash'            | 'Suns'          | 0    |
      | 'Steve Nash'            | 'Mavericks'     | 0    |
      | 'Steve Nash'            | 'Suns'          | 1    |
      | 'Steve Nash'            | 'Lakers'        | 0    |
      | 'Jason Kidd'            | 'Mavericks'     | 0    |
      | 'Jason Kidd'            | 'Suns'          | 0    |
      | 'Jason Kidd'            | 'Nets'          | 0    |
      | 'Jason Kidd'            | 'Mavericks'     | 1    |
      | 'Jason Kidd'            | 'Knicks'        | 0    |
      | 'Dirk Nowitzki'         | 'Mavericks'     | 0    |
      | 'Paul George'           | 'Pacers'        | 0    |
      | 'Paul George'           | 'Thunders'      | 0    |
      | 'Grant Hill'            | 'Pistons'       | 0    |
      | 'Grant Hill'            | 'Magic'         | 0    |
      | 'Grant Hill'            | 'Suns'          | 0    |
      | 'Grant Hill'            | 'Clippers'      | 0    |
      | "Shaquille O'Neal"      | 'Magic'         | 0    |
      | "Shaquille O'Neal"      | 'Lakers'        | 0    |
      | "Shaquille O'Neal"      | 'Heat'          | 0    |
      | "Shaquille O'Neal"      | 'Suns'          | 0    |
      | "Shaquille O'Neal"      | 'Cavaliers'     | 0    |
      | "Shaquille O'Neal"      | 'Celtics'       | 0    |
      | 'JaVale McGee'          | 'Wizards'       | 0    |
      | 'JaVale McGee'          | 'Nuggets'       | 0    |
      | 'JaVale McGee'          | 'Mavericks'     | 0    |
      | 'JaVale McGee'          | 'Warriors'      | 0    |
      | 'JaVale McGee'          | 'Lakers'        | 0    |
      | 'Dwight Howard'         | 'Magic'         | 0    |
      | 'Dwight Howard'         | 'Lakers'        | 0    |
      | 'Dwight Howard'         | 'Rockets'       | 0    |
      | 'Dwight Howard'         | 'Hawks'         | 0    |
      | 'Dwight Howard'         | 'Hornets'       | 0    |
      | 'Dwight Howard'         | 'Wizards'       | 0    |
    When executing query:
      """
      LOOKUP ON serve YIELD serve.start_year AS startYear
      """
    Then the result should be, in any order:
      | startYear |
      | 2002      |
      | 2010      |
      | 2015      |
      | 2008      |
      | 2009      |
      | 2012      |
      | 1996      |
      | 1997      |
      | 2000      |
      | 2004      |
      | 2013      |
      | 2005      |
      | 2011      |
      | 2017      |
      | 2003      |
      | 2005      |
      | 2008      |
      | 2012      |
      | 2016      |
      | 2003      |
      | 2010      |
      | 2014      |
      | 2018      |
      | 2011      |
      | 2015      |
      | 2019      |
      | 2015      |
      | 2017      |
      | 2019      |
      | 2007      |
      | 2009      |
      | 2010      |
      | 2012      |
      | 2013      |
      | 2015      |
      | 2016      |
      | 2017      |
      | 2018      |
      | 2018      |
      | 2018      |
      | 2003      |
      | 2011      |
      | 2015      |
      | 2016      |
      | 1999      |
      | 2018      |
      | 2009      |
      | 2010      |
      | 2018      |
      | 2006      |
      | 2013      |
      | 2013      |
      | 2017      |
      | 2006      |
      | 2015      |
      | 1997      |
      | 2007      |
      | 2016      |
      | 2009      |
      | 1996      |
      | 2003      |
      | 2007      |
      | 2012      |
      | 2010      |
      | 2015      |
      | 2017      |
      | 2008      |
      | 2018      |
      | 2019      |
      | 2001      |
      | 2008      |
      | 2014      |
      | 2016      |
      | 2019      |
      | 2013      |
      | 2015      |
      | 2017      |
      | 2011      |
      | 2015      |
      | 2017      |
      | 1998      |
      | 2004      |
      | 2009      |
      | 2010      |
      | 2011      |
      | 2014      |
      | 2017      |
      | 2018      |
      | 2008      |
      | 2019      |
      | 2011      |
      | 2017      |
      | 2016      |
      | 2013      |
      | 2006      |
      | 2014      |
      | 2015      |
      | 2016      |
      | 2017      |
      | 2018      |
      | 2002      |
      | 2011      |
      | 2017      |
      | 2003      |
      | 2011      |
      | 2017      |
      | 2018      |
      | 2003      |
      | 2016      |
      | 2017      |
      | 2018      |
      | 2014      |
      | 2012      |
      | 2002      |
      | 2014      |
      | 2018      |
      | 2016      |
      | 2009      |
      | 2018      |
      | 1996      |
      | 1998      |
      | 2004      |
      | 2012      |
      | 1994      |
      | 1996      |
      | 2001      |
      | 2008      |
      | 2012      |
      | 1998      |
      | 2010      |
      | 2017      |
      | 1994      |
      | 2000      |
      | 2007      |
      | 2012      |
      | 1992      |
      | 1996      |
      | 2004      |
      | 2008      |
      | 2009      |
      | 2010      |
      | 2008      |
      | 2012      |
      | 2015      |
      | 2016      |
      | 2018      |
      | 2004      |
      | 2012      |
      | 2013      |
      | 2016      |
      | 2017      |
      | 2018      |

  Scenario: [2] Edge TODO
    Given a graph with space named "nba_int_vid"
    When executing query:
      """
      LOOKUP ON serve WHERE 1 + 1 == 2 YIELD serve.start_year
      """
    Then a SemanticError should be raised at runtime:
    When executing query:
      """
      LOOKUP ON serve WHERE 1 + 1 == 2 YIELD serve.start_year AS startYear
      """
    Then a SemanticError should be raised at runtime:
    When executing query:
      """
      LOOKUP ON serve WHERE serve.start_year == serve.end_year YIELD serve.start_year
      """
    Then a SemanticError should be raised at runtime:
    When executing query:
      """
      LOOKUP ON serve WHERE serve.start_year == serve.end_year YIELD serve.start_year AS startYear
      """
    Then a SemanticError should be raised at runtime:

  Scenario: [1] Compare INT and FLOAT during IndexScan
    Given a graph with space named "nba_int_vid"
    When executing query:
      """
      LOOKUP ON player WHERE player.age == 40 YIELD id(vertex) as name, player.age AS Age
      """
    Then the result should be, in any order, and the columns 0 should be hashed:
      | name            | Age |
      | "Dirk Nowitzki" | 40  |
      | "Kobe Bryant"   | 40  |
    When executing query:
      """
      LOOKUP ON player WHERE player.age > 40 YIELD id(vertex) as name, player.age AS Age
      """
    Then the result should be, in any order, and the columns 0 should be hashed:
      | name               | Age |
      | "Grant Hill"       | 46  |
      | "Jason Kidd"       | 45  |
      | "Manu Ginobili"    | 41  |
      | "Ray Allen"        | 43  |
      | "Shaquille O'Neal" | 47  |
      | "Steve Nash"       | 45  |
      | "Tim Duncan"       | 42  |
      | "Vince Carter"     | 42  |
    When executing query:
      """
      LOOKUP ON player WHERE player.age >= 40.0 YIELD id(vertex) as name, player.age AS Age
      """
    Then the result should be, in any order, and the columns 0 should be hashed:
      | name               | Age |
      | "Grant Hill"       | 46  |
      | "Jason Kidd"       | 45  |
      | "Manu Ginobili"    | 41  |
      | "Ray Allen"        | 43  |
      | "Shaquille O'Neal" | 47  |
      | "Steve Nash"       | 45  |
      | "Tim Duncan"       | 42  |
      | "Vince Carter"     | 42  |
      | "Dirk Nowitzki"    | 40  |
      | "Kobe Bryant"      | 40  |
    When executing query:
      """
      LOOKUP ON player WHERE player.age > 40.5 YIELD id(vertex) as name, player.age AS Age
      """
    Then the result should be, in any order, and the columns 0 should be hashed:
      | name               | Age |
      | "Grant Hill"       | 46  |
      | "Jason Kidd"       | 45  |
      | "Manu Ginobili"    | 41  |
      | "Ray Allen"        | 43  |
      | "Shaquille O'Neal" | 47  |
      | "Steve Nash"       | 45  |
      | "Tim Duncan"       | 42  |
      | "Vince Carter"     | 42  |
    When executing query:
      """
      LOOKUP ON player WHERE player.age >= 40.5 YIELD id(vertex) as name, player.age AS Age
      """
    Then the result should be, in any order, and the columns 0 should be hashed:
      | name               | Age |
      | "Grant Hill"       | 46  |
      | "Jason Kidd"       | 45  |
      | "Manu Ginobili"    | 41  |
      | "Ray Allen"        | 43  |
      | "Shaquille O'Neal" | 47  |
      | "Steve Nash"       | 45  |
      | "Tim Duncan"       | 42  |
      | "Vince Carter"     | 42  |
    When executing query:
      """
      LOOKUP ON player WHERE player.age < 40
      YIELD player.age AS Age, player.name AS Name | order by $-.Age DESC, $-.Name| limit 10
      """
    Then the result should be, in order, with relax comparison:
      | Age | Name                |
      | 39  | "Tracy McGrady"     |
      | 38  | "David West"        |
      | 38  | "Paul Gasol"        |
      | 38  | "Yao Ming"          |
      | 37  | "Dwyane Wade"       |
      | 36  | "Amar'e Stoudemire" |
      | 36  | "Boris Diaw"        |
      | 36  | "Tony Parker"       |
      | 34  | "Carmelo Anthony"   |
      | 34  | "LeBron James"      |
    When executing query:
      """
      LOOKUP ON player WHERE player.age <= 40
      YIELD player.age AS Age, player.name AS Name | order by $-.Age DESC, $-.Name| limit 10
      """
    Then the result should be, in order, with relax comparison:
      | Age | Name                |
      | 40  | "Dirk Nowitzki"     |
      | 40  | "Kobe Bryant"       |
      | 39  | "Tracy McGrady"     |
      | 38  | "David West"        |
      | 38  | "Paul Gasol"        |
      | 38  | "Yao Ming"          |
      | 37  | "Dwyane Wade"       |
      | 36  | "Amar'e Stoudemire" |
      | 36  | "Boris Diaw"        |
      | 36  | "Tony Parker"       |

  Scenario: [2] Compare INT and FLOAT during IndexScan
    Given an empty graph
    And load "nba_int_vid" csv data to a new space
    And having executed:
      """
      CREATE TAG weight (WEIGHT double);
      CREATE TAG INDEX weight_index ON weight(WEIGHT);
      """
    And wait 6 seconds
    When executing query:
      """
      INSERT VERTEX weight(WEIGHT) VALUES hash("Tim Duncan") : (70.5);
      INSERT VERTEX weight(WEIGHT) VALUES hash("Tony Parker") : (80.0);
      """
    When executing query:
      """
      LOOKUP ON weight WHERE weight.WEIGHT > 70 YIELD id(vertex) as name;
      """
    Then the result should be, in any order, and the columns 0 should be hashed:
      | name          |
      | "Tim Duncan"  |
      | "Tony Parker" |
    When executing query:
      """
      LOOKUP ON weight WHERE weight.WEIGHT > 70.4 YIELD id(vertex) as name;
      """
    Then the result should be, in any order, and the columns 0 should be hashed:
      | name          |
      | "Tim Duncan"  |
      | "Tony Parker" |
    When executing query:
      """
      LOOKUP ON weight WHERE weight.WEIGHT >= 70.5 YIELD id(vertex) as name;
      """
    Then the result should be, in any order, and the columns 0 should be hashed:
      | name          |
      | "Tim Duncan"  |
      | "Tony Parker" |
    # (TODO) Unsupported cases due to the lack of float precision
    When executing query:
      """
      LOOKUP ON weight WHERE weight.WEIGHT > 70.5 YIELD id(vertex) as name;
      """
    Then the result should be, in any order, and the columns 0 should be hashed:
      | name          |
      | "Tony Parker" |
    When executing query:
      """
      LOOKUP ON weight WHERE weight.WEIGHT <= 80.0 YIELD id(vertex) as name;
      """
    Then the result should be, in any order, and the columns 0 should be hashed:
      | name          |
      | "Tim Duncan"  |
      | "Tony Parker" |
    Then drop the used space
