# Copyright (c) 2021 vesoft inc. All rights reserved.
#
# This source code is licensed under Apache 2.0 License,
# attached with Common Clause Condition 1.0, found in the LICENSES directory.
Feature: Lookup by index itself

  Background:
    Given an empty graph
    And load "nba" csv data to a new space

  Scenario: [1] tag index
    When executing query:
      """
      LOOKUP ON team
      """
    Then the result should be, in any order:
      | VertexID        |
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
      LOOKUP ON team YIELD team.name AS Name
      """
    Then the result should be, in any order:
      | VertexID        | Name            |
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
    And drop the used space

  Scenario: [1] Tag TODO
    When executing query:
      """
      LOOKUP ON team WHERE 1 + 1 == 2
      """
    Then a SemanticError should be raised at runtime:
    When executing query:
      """
      LOOKUP ON team WHERE 1 + 1 == 2 YIELD team.name AS Name
      """
    Then a SemanticError should be raised at runtime:
    When executing query:
      """
      LOOKUP ON player WHERE player.age > 9223372036854775807+1
      """
    Then a ExecutionError should be raised at runtime: result of (9223372036854775807+1) cannot be represented as an integer
    When executing query:
      """
      LOOKUP ON player WHERE player.age > -9223372036854775808-1
      """
    Then a ExecutionError should be raised at runtime: result of (-9223372036854775808-1) cannot be represented as an integer
    And drop the used space

  Scenario: [2] edge index
    When executing query:
      """
      LOOKUP ON serve
      """
    Then the result should be, in any order:
      | SrcVID                  | DstVID          | Ranking |
      | "Amar'e Stoudemire"     | 'Suns'          | 0       |
      | "Amar'e Stoudemire"     | 'Knicks'        | 0       |
      | "Amar'e Stoudemire"     | 'Heat'          | 0       |
      | 'Russell Westbrook'     | 'Thunders'      | 0       |
      | 'James Harden'          | 'Thunders'      | 0       |
      | 'James Harden'          | 'Rockets'       | 0       |
      | 'Kobe Bryant'           | 'Lakers'        | 0       |
      | 'Tracy McGrady'         | 'Raptors'       | 0       |
      | 'Tracy McGrady'         | 'Magic'         | 0       |
      | 'Tracy McGrady'         | 'Rockets'       | 0       |
      | 'Tracy McGrady'         | 'Spurs'         | 0       |
      | 'Chris Paul'            | 'Hornets'       | 0       |
      | 'Chris Paul'            | 'Clippers'      | 0       |
      | 'Chris Paul'            | 'Rockets'       | 0       |
      | 'Boris Diaw'            | 'Hawks'         | 0       |
      | 'Boris Diaw'            | 'Suns'          | 0       |
      | 'Boris Diaw'            | 'Hornets'       | 0       |
      | 'Boris Diaw'            | 'Spurs'         | 0       |
      | 'Boris Diaw'            | 'Jazz'          | 0       |
      | 'LeBron James'          | 'Cavaliers'     | 0       |
      | 'LeBron James'          | 'Heat'          | 0       |
      | 'LeBron James'          | 'Cavaliers'     | 1       |
      | 'LeBron James'          | 'Lakers'        | 0       |
      | 'Klay Thompson'         | 'Warriors'      | 0       |
      | 'Kristaps Porzingis'    | 'Knicks'        | 0       |
      | 'Kristaps Porzingis'    | 'Mavericks'     | 0       |
      | 'Jonathon Simmons'      | 'Spurs'         | 0       |
      | 'Jonathon Simmons'      | 'Magic'         | 0       |
      | 'Jonathon Simmons'      | '76ers'         | 0       |
      | 'Marco Belinelli'       | 'Warriors'      | 0       |
      | 'Marco Belinelli'       | 'Raptors'       | 0       |
      | 'Marco Belinelli'       | 'Hornets'       | 0       |
      | 'Marco Belinelli'       | 'Bulls'         | 0       |
      | 'Marco Belinelli'       | 'Spurs'         | 0       |
      | 'Marco Belinelli'       | 'Kings'         | 0       |
      | 'Marco Belinelli'       | 'Hornets'       | 1       |
      | 'Marco Belinelli'       | 'Hawks'         | 0       |
      | 'Marco Belinelli'       | '76ers'         | 0       |
      | 'Marco Belinelli'       | 'Spurs'         | 1       |
      | 'Luka Doncic'           | 'Mavericks'     | 0       |
      | 'David West'            | 'Hornets'       | 0       |
      | 'David West'            | 'Pacers'        | 0       |
      | 'David West'            | 'Spurs'         | 0       |
      | 'David West'            | 'Warriors'      | 0       |
      | 'Tony Parker'           | 'Spurs'         | 0       |
      | 'Tony Parker'           | 'Hornets'       | 0       |
      | 'Danny Green'           | 'Cavaliers'     | 0       |
      | 'Danny Green'           | 'Spurs'         | 0       |
      | 'Danny Green'           | 'Raptors'       | 0       |
      | 'Rudy Gay'              | 'Grizzlies'     | 0       |
      | 'Rudy Gay'              | 'Raptors'       | 0       |
      | 'Rudy Gay'              | 'Kings'         | 0       |
      | 'Rudy Gay'              | 'Spurs'         | 0       |
      | 'LaMarcus Aldridge'     | 'Trail Blazers' | 0       |
      | 'LaMarcus Aldridge'     | 'Spurs'         | 0       |
      | 'Tim Duncan'            | 'Spurs'         | 0       |
      | 'Kevin Durant'          | 'Thunders'      | 0       |
      | 'Kevin Durant'          | 'Warriors'      | 0       |
      | 'Stephen Curry'         | 'Warriors'      | 0       |
      | 'Ray Allen'             | 'Bucks'         | 0       |
      | 'Ray Allen'             | 'Thunders'      | 0       |
      | 'Ray Allen'             | 'Celtics'       | 0       |
      | 'Ray Allen'             | 'Heat'          | 0       |
      | 'Tiago Splitter'        | 'Spurs'         | 0       |
      | 'Tiago Splitter'        | 'Hawks'         | 0       |
      | 'Tiago Splitter'        | '76ers'         | 0       |
      | 'DeAndre Jordan'        | 'Clippers'      | 0       |
      | 'DeAndre Jordan'        | 'Mavericks'     | 0       |
      | 'DeAndre Jordan'        | 'Knicks'        | 0       |
      | 'Paul Gasol'            | 'Grizzlies'     | 0       |
      | 'Paul Gasol'            | 'Lakers'        | 0       |
      | 'Paul Gasol'            | 'Bulls'         | 0       |
      | 'Paul Gasol'            | 'Spurs'         | 0       |
      | 'Paul Gasol'            | 'Bucks'         | 0       |
      | 'Aron Baynes'           | 'Spurs'         | 0       |
      | 'Aron Baynes'           | 'Pistons'       | 0       |
      | 'Aron Baynes'           | 'Celtics'       | 0       |
      | 'Cory Joseph'           | 'Spurs'         | 0       |
      | 'Cory Joseph'           | 'Raptors'       | 0       |
      | 'Cory Joseph'           | 'Pacers'        | 0       |
      | 'Vince Carter'          | 'Raptors'       | 0       |
      | 'Vince Carter'          | 'Nets'          | 0       |
      | 'Vince Carter'          | 'Magic'         | 0       |
      | 'Vince Carter'          | 'Suns'          | 0       |
      | 'Vince Carter'          | 'Mavericks'     | 0       |
      | 'Vince Carter'          | 'Grizzlies'     | 0       |
      | 'Vince Carter'          | 'Kings'         | 0       |
      | 'Vince Carter'          | 'Hawks'         | 0       |
      | 'Marc Gasol'            | 'Grizzlies'     | 0       |
      | 'Marc Gasol'            | 'Raptors'       | 0       |
      | 'Ricky Rubio'           | 'Timberwolves'  | 0       |
      | 'Ricky Rubio'           | 'Jazz'          | 0       |
      | 'Ben Simmons'           | '76ers'         | 0       |
      | 'Giannis Antetokounmpo' | 'Bucks'         | 0       |
      | 'Rajon Rondo'           | 'Celtics'       | 0       |
      | 'Rajon Rondo'           | 'Mavericks'     | 0       |
      | 'Rajon Rondo'           | 'Kings'         | 0       |
      | 'Rajon Rondo'           | 'Bulls'         | 0       |
      | 'Rajon Rondo'           | 'Pelicans'      | 0       |
      | 'Rajon Rondo'           | 'Lakers'        | 0       |
      | 'Manu Ginobili'         | 'Spurs'         | 0       |
      | 'Kyrie Irving'          | 'Cavaliers'     | 0       |
      | 'Kyrie Irving'          | 'Celtics'       | 0       |
      | 'Carmelo Anthony'       | 'Nuggets'       | 0       |
      | 'Carmelo Anthony'       | 'Knicks'        | 0       |
      | 'Carmelo Anthony'       | 'Thunders'      | 0       |
      | 'Carmelo Anthony'       | 'Rockets'       | 0       |
      | 'Dwyane Wade'           | 'Heat'          | 0       |
      | 'Dwyane Wade'           | 'Bulls'         | 0       |
      | 'Dwyane Wade'           | 'Cavaliers'     | 0       |
      | 'Dwyane Wade'           | 'Heat'          | 1       |
      | 'Joel Embiid'           | '76ers'         | 0       |
      | 'Damian Lillard'        | 'Trail Blazers' | 0       |
      | 'Yao Ming'              | 'Rockets'       | 0       |
      | 'Kyle Anderson'         | 'Spurs'         | 0       |
      | 'Kyle Anderson'         | 'Grizzlies'     | 0       |
      | 'Dejounte Murray'       | 'Spurs'         | 0       |
      | 'Blake Griffin'         | 'Clippers'      | 0       |
      | 'Blake Griffin'         | 'Pistons'       | 0       |
      | 'Steve Nash'            | 'Suns'          | 0       |
      | 'Steve Nash'            | 'Mavericks'     | 0       |
      | 'Steve Nash'            | 'Suns'          | 1       |
      | 'Steve Nash'            | 'Lakers'        | 0       |
      | 'Jason Kidd'            | 'Mavericks'     | 0       |
      | 'Jason Kidd'            | 'Suns'          | 0       |
      | 'Jason Kidd'            | 'Nets'          | 0       |
      | 'Jason Kidd'            | 'Mavericks'     | 1       |
      | 'Jason Kidd'            | 'Knicks'        | 0       |
      | 'Dirk Nowitzki'         | 'Mavericks'     | 0       |
      | 'Paul George'           | 'Pacers'        | 0       |
      | 'Paul George'           | 'Thunders'      | 0       |
      | 'Grant Hill'            | 'Pistons'       | 0       |
      | 'Grant Hill'            | 'Magic'         | 0       |
      | 'Grant Hill'            | 'Suns'          | 0       |
      | 'Grant Hill'            | 'Clippers'      | 0       |
      | "Shaquile O'Neal"       | 'Magic'         | 0       |
      | "Shaquile O'Neal"       | 'Lakers'        | 0       |
      | "Shaquile O'Neal"       | 'Heat'          | 0       |
      | "Shaquile O'Neal"       | 'Suns'          | 0       |
      | "Shaquile O'Neal"       | 'Cavaliers'     | 0       |
      | "Shaquile O'Neal"       | 'Celtics'       | 0       |
      | 'JaVale McGee'          | 'Wizards'       | 0       |
      | 'JaVale McGee'          | 'Nuggets'       | 0       |
      | 'JaVale McGee'          | 'Mavericks'     | 0       |
      | 'JaVale McGee'          | 'Warriors'      | 0       |
      | 'JaVale McGee'          | 'Lakers'        | 0       |
      | 'Dwight Howard'         | 'Magic'         | 0       |
      | 'Dwight Howard'         | 'Lakers'        | 0       |
      | 'Dwight Howard'         | 'Rockets'       | 0       |
      | 'Dwight Howard'         | 'Hawks'         | 0       |
      | 'Dwight Howard'         | 'Hornets'       | 0       |
      | 'Dwight Howard'         | 'Wizards'       | 0       |
    When executing query:
      """
      LOOKUP ON serve YIELD serve.start_year AS startYear
      """
    Then the result should be, in any order:
      | SrcVID                  | DstVID          | Ranking | startYear |
      | "Amar'e Stoudemire"     | 'Suns'          | 0       | 2002      |
      | "Amar'e Stoudemire"     | 'Knicks'        | 0       | 2010      |
      | "Amar'e Stoudemire"     | 'Heat'          | 0       | 2015      |
      | 'Russell Westbrook'     | 'Thunders'      | 0       | 2008      |
      | 'James Harden'          | 'Thunders'      | 0       | 2009      |
      | 'James Harden'          | 'Rockets'       | 0       | 2012      |
      | 'Kobe Bryant'           | 'Lakers'        | 0       | 1996      |
      | 'Tracy McGrady'         | 'Raptors'       | 0       | 1997      |
      | 'Tracy McGrady'         | 'Magic'         | 0       | 2000      |
      | 'Tracy McGrady'         | 'Rockets'       | 0       | 2004      |
      | 'Tracy McGrady'         | 'Spurs'         | 0       | 2013      |
      | 'Chris Paul'            | 'Hornets'       | 0       | 2005      |
      | 'Chris Paul'            | 'Clippers'      | 0       | 2011      |
      | 'Chris Paul'            | 'Rockets'       | 0       | 2017      |
      | 'Boris Diaw'            | 'Hawks'         | 0       | 2003      |
      | 'Boris Diaw'            | 'Suns'          | 0       | 2005      |
      | 'Boris Diaw'            | 'Hornets'       | 0       | 2008      |
      | 'Boris Diaw'            | 'Spurs'         | 0       | 2012      |
      | 'Boris Diaw'            | 'Jazz'          | 0       | 2016      |
      | 'LeBron James'          | 'Cavaliers'     | 0       | 2003      |
      | 'LeBron James'          | 'Heat'          | 0       | 2010      |
      | 'LeBron James'          | 'Cavaliers'     | 1       | 2014      |
      | 'LeBron James'          | 'Lakers'        | 0       | 2018      |
      | 'Klay Thompson'         | 'Warriors'      | 0       | 2011      |
      | 'Kristaps Porzingis'    | 'Knicks'        | 0       | 2015      |
      | 'Kristaps Porzingis'    | 'Mavericks'     | 0       | 2019      |
      | 'Jonathon Simmons'      | 'Spurs'         | 0       | 2015      |
      | 'Jonathon Simmons'      | 'Magic'         | 0       | 2017      |
      | 'Jonathon Simmons'      | '76ers'         | 0       | 2019      |
      | 'Marco Belinelli'       | 'Warriors'      | 0       | 2007      |
      | 'Marco Belinelli'       | 'Raptors'       | 0       | 2009      |
      | 'Marco Belinelli'       | 'Hornets'       | 0       | 2010      |
      | 'Marco Belinelli'       | 'Bulls'         | 0       | 2012      |
      | 'Marco Belinelli'       | 'Spurs'         | 0       | 2013      |
      | 'Marco Belinelli'       | 'Kings'         | 0       | 2015      |
      | 'Marco Belinelli'       | 'Hornets'       | 1       | 2016      |
      | 'Marco Belinelli'       | 'Hawks'         | 0       | 2017      |
      | 'Marco Belinelli'       | '76ers'         | 0       | 2018      |
      | 'Marco Belinelli'       | 'Spurs'         | 1       | 2018      |
      | 'Luka Doncic'           | 'Mavericks'     | 0       | 2018      |
      | 'David West'            | 'Hornets'       | 0       | 2003      |
      | 'David West'            | 'Pacers'        | 0       | 2011      |
      | 'David West'            | 'Spurs'         | 0       | 2015      |
      | 'David West'            | 'Warriors'      | 0       | 2016      |
      | 'Tony Parker'           | 'Spurs'         | 0       | 1999      |
      | 'Tony Parker'           | 'Hornets'       | 0       | 2018      |
      | 'Danny Green'           | 'Cavaliers'     | 0       | 2009      |
      | 'Danny Green'           | 'Spurs'         | 0       | 2010      |
      | 'Danny Green'           | 'Raptors'       | 0       | 2018      |
      | 'Rudy Gay'              | 'Grizzlies'     | 0       | 2006      |
      | 'Rudy Gay'              | 'Raptors'       | 0       | 2013      |
      | 'Rudy Gay'              | 'Kings'         | 0       | 2013      |
      | 'Rudy Gay'              | 'Spurs'         | 0       | 2017      |
      | 'LaMarcus Aldridge'     | 'Trail Blazers' | 0       | 2006      |
      | 'LaMarcus Aldridge'     | 'Spurs'         | 0       | 2015      |
      | 'Tim Duncan'            | 'Spurs'         | 0       | 1997      |
      | 'Kevin Durant'          | 'Thunders'      | 0       | 2007      |
      | 'Kevin Durant'          | 'Warriors'      | 0       | 2016      |
      | 'Stephen Curry'         | 'Warriors'      | 0       | 2009      |
      | 'Ray Allen'             | 'Bucks'         | 0       | 1996      |
      | 'Ray Allen'             | 'Thunders'      | 0       | 2003      |
      | 'Ray Allen'             | 'Celtics'       | 0       | 2007      |
      | 'Ray Allen'             | 'Heat'          | 0       | 2012      |
      | 'Tiago Splitter'        | 'Spurs'         | 0       | 2010      |
      | 'Tiago Splitter'        | 'Hawks'         | 0       | 2015      |
      | 'Tiago Splitter'        | '76ers'         | 0       | 2017      |
      | 'DeAndre Jordan'        | 'Clippers'      | 0       | 2008      |
      | 'DeAndre Jordan'        | 'Mavericks'     | 0       | 2018      |
      | 'DeAndre Jordan'        | 'Knicks'        | 0       | 2019      |
      | 'Paul Gasol'            | 'Grizzlies'     | 0       | 2001      |
      | 'Paul Gasol'            | 'Lakers'        | 0       | 2008      |
      | 'Paul Gasol'            | 'Bulls'         | 0       | 2014      |
      | 'Paul Gasol'            | 'Spurs'         | 0       | 2016      |
      | 'Paul Gasol'            | 'Bucks'         | 0       | 2019      |
      | 'Aron Baynes'           | 'Spurs'         | 0       | 2013      |
      | 'Aron Baynes'           | 'Pistons'       | 0       | 2015      |
      | 'Aron Baynes'           | 'Celtics'       | 0       | 2017      |
      | 'Cory Joseph'           | 'Spurs'         | 0       | 2011      |
      | 'Cory Joseph'           | 'Raptors'       | 0       | 2015      |
      | 'Cory Joseph'           | 'Pacers'        | 0       | 2017      |
      | 'Vince Carter'          | 'Raptors'       | 0       | 1998      |
      | 'Vince Carter'          | 'Nets'          | 0       | 2004      |
      | 'Vince Carter'          | 'Magic'         | 0       | 2009      |
      | 'Vince Carter'          | 'Suns'          | 0       | 2010      |
      | 'Vince Carter'          | 'Mavericks'     | 0       | 2011      |
      | 'Vince Carter'          | 'Grizzlies'     | 0       | 2014      |
      | 'Vince Carter'          | 'Kings'         | 0       | 2017      |
      | 'Vince Carter'          | 'Hawks'         | 0       | 2018      |
      | 'Marc Gasol'            | 'Grizzlies'     | 0       | 2008      |
      | 'Marc Gasol'            | 'Raptors'       | 0       | 2019      |
      | 'Ricky Rubio'           | 'Timberwolves'  | 0       | 2011      |
      | 'Ricky Rubio'           | 'Jazz'          | 0       | 2017      |
      | 'Ben Simmons'           | '76ers'         | 0       | 2016      |
      | 'Giannis Antetokounmpo' | 'Bucks'         | 0       | 2013      |
      | 'Rajon Rondo'           | 'Celtics'       | 0       | 2006      |
      | 'Rajon Rondo'           | 'Mavericks'     | 0       | 2014      |
      | 'Rajon Rondo'           | 'Kings'         | 0       | 2015      |
      | 'Rajon Rondo'           | 'Bulls'         | 0       | 2016      |
      | 'Rajon Rondo'           | 'Pelicans'      | 0       | 2017      |
      | 'Rajon Rondo'           | 'Lakers'        | 0       | 2018      |
      | 'Manu Ginobili'         | 'Spurs'         | 0       | 2002      |
      | 'Kyrie Irving'          | 'Cavaliers'     | 0       | 2011      |
      | 'Kyrie Irving'          | 'Celtics'       | 0       | 2017      |
      | 'Carmelo Anthony'       | 'Nuggets'       | 0       | 2003      |
      | 'Carmelo Anthony'       | 'Knicks'        | 0       | 2011      |
      | 'Carmelo Anthony'       | 'Thunders'      | 0       | 2017      |
      | 'Carmelo Anthony'       | 'Rockets'       | 0       | 2018      |
      | 'Dwyane Wade'           | 'Heat'          | 0       | 2003      |
      | 'Dwyane Wade'           | 'Bulls'         | 0       | 2016      |
      | 'Dwyane Wade'           | 'Cavaliers'     | 0       | 2017      |
      | 'Dwyane Wade'           | 'Heat'          | 1       | 2018      |
      | 'Joel Embiid'           | '76ers'         | 0       | 2014      |
      | 'Damian Lillard'        | 'Trail Blazers' | 0       | 2012      |
      | 'Yao Ming'              | 'Rockets'       | 0       | 2002      |
      | 'Kyle Anderson'         | 'Spurs'         | 0       | 2014      |
      | 'Kyle Anderson'         | 'Grizzlies'     | 0       | 2018      |
      | 'Dejounte Murray'       | 'Spurs'         | 0       | 2016      |
      | 'Blake Griffin'         | 'Clippers'      | 0       | 2009      |
      | 'Blake Griffin'         | 'Pistons'       | 0       | 2018      |
      | 'Steve Nash'            | 'Suns'          | 0       | 1996      |
      | 'Steve Nash'            | 'Mavericks'     | 0       | 1998      |
      | 'Steve Nash'            | 'Suns'          | 1       | 2004      |
      | 'Steve Nash'            | 'Lakers'        | 0       | 2012      |
      | 'Jason Kidd'            | 'Mavericks'     | 0       | 1994      |
      | 'Jason Kidd'            | 'Suns'          | 0       | 1996      |
      | 'Jason Kidd'            | 'Nets'          | 0       | 2001      |
      | 'Jason Kidd'            | 'Mavericks'     | 1       | 2008      |
      | 'Jason Kidd'            | 'Knicks'        | 0       | 2012      |
      | 'Dirk Nowitzki'         | 'Mavericks'     | 0       | 1998      |
      | 'Paul George'           | 'Pacers'        | 0       | 2010      |
      | 'Paul George'           | 'Thunders'      | 0       | 2017      |
      | 'Grant Hill'            | 'Pistons'       | 0       | 1994      |
      | 'Grant Hill'            | 'Magic'         | 0       | 2000      |
      | 'Grant Hill'            | 'Suns'          | 0       | 2007      |
      | 'Grant Hill'            | 'Clippers'      | 0       | 2012      |
      | "Shaquile O'Neal"       | 'Magic'         | 0       | 1992      |
      | "Shaquile O'Neal"       | 'Lakers'        | 0       | 1996      |
      | "Shaquile O'Neal"       | 'Heat'          | 0       | 2004      |
      | "Shaquile O'Neal"       | 'Suns'          | 0       | 2008      |
      | "Shaquile O'Neal"       | 'Cavaliers'     | 0       | 2009      |
      | "Shaquile O'Neal"       | 'Celtics'       | 0       | 2010      |
      | 'JaVale McGee'          | 'Wizards'       | 0       | 2008      |
      | 'JaVale McGee'          | 'Nuggets'       | 0       | 2012      |
      | 'JaVale McGee'          | 'Mavericks'     | 0       | 2015      |
      | 'JaVale McGee'          | 'Warriors'      | 0       | 2016      |
      | 'JaVale McGee'          | 'Lakers'        | 0       | 2018      |
      | 'Dwight Howard'         | 'Magic'         | 0       | 2004      |
      | 'Dwight Howard'         | 'Lakers'        | 0       | 2012      |
      | 'Dwight Howard'         | 'Rockets'       | 0       | 2013      |
      | 'Dwight Howard'         | 'Hawks'         | 0       | 2016      |
      | 'Dwight Howard'         | 'Hornets'       | 0       | 2017      |
      | 'Dwight Howard'         | 'Wizards'       | 0       | 2018      |
    And drop the used space

  Scenario: [2] Edge TODO
    When executing query:
      """
      LOOKUP ON serve WHERE 1 + 1 == 2
      """
    Then a SemanticError should be raised at runtime:
    When executing query:
      """
      LOOKUP ON serve WHERE 1 + 1 == 2 YIELD serve.start_year AS startYear
      """
    Then a SemanticError should be raised at runtime:
    When executing query:
      """
      LOOKUP ON serve WHERE serve.start_year == serve.end_year
      """
    Then a SemanticError should be raised at runtime:
    When executing query:
      """
      LOOKUP ON serve WHERE serve.start_year == serve.end_year YIELD serve.start_year AS startYear
      """
    Then a SemanticError should be raised at runtime:
    And drop the used space

  Scenario: [1] Compare INT and FLOAT during IndexScan
    When executing query:
      """
      LOOKUP ON player WHERE player.age == 40 YIELD player.age AS Age
      """
    Then the result should be, in any order:
      | VertexID        | Age |
      | "Dirk Nowitzki" | 40  |
      | "Kobe Bryant"   | 40  |
    When executing query:
      """
      LOOKUP ON player WHERE player.age > 40 YIELD player.age AS Age
      """
    Then the result should be, in any order:
      | VertexID          | Age |
      | "Grant Hill"      | 46  |
      | "Jason Kidd"      | 45  |
      | "Manu Ginobili"   | 41  |
      | "Ray Allen"       | 43  |
      | "Shaquile O'Neal" | 47  |
      | "Steve Nash"      | 45  |
      | "Tim Duncan"      | 42  |
      | "Vince Carter"    | 42  |
    When executing query:
      """
      LOOKUP ON player WHERE player.age >= 40.0 YIELD player.age AS Age
      """
    Then the result should be, in any order:
      | VertexID          | Age |
      | "Grant Hill"      | 46  |
      | "Jason Kidd"      | 45  |
      | "Manu Ginobili"   | 41  |
      | "Ray Allen"       | 43  |
      | "Shaquile O'Neal" | 47  |
      | "Steve Nash"      | 45  |
      | "Tim Duncan"      | 42  |
      | "Vince Carter"    | 42  |
      | "Dirk Nowitzki"   | 40  |
      | "Kobe Bryant"     | 40  |
    When executing query:
      """
      LOOKUP ON player WHERE player.age > 40.5 YIELD player.age AS Age
      """
    Then the result should be, in any order:
      | VertexID          | Age |
      | "Grant Hill"      | 46  |
      | "Jason Kidd"      | 45  |
      | "Manu Ginobili"   | 41  |
      | "Ray Allen"       | 43  |
      | "Shaquile O'Neal" | 47  |
      | "Steve Nash"      | 45  |
      | "Tim Duncan"      | 42  |
      | "Vince Carter"    | 42  |
    When executing query:
      """
      LOOKUP ON player WHERE player.age >= 40.5 YIELD player.age AS Age
      """
    Then the result should be, in any order:
      | VertexID          | Age |
      | "Grant Hill"      | 46  |
      | "Jason Kidd"      | 45  |
      | "Manu Ginobili"   | 41  |
      | "Ray Allen"       | 43  |
      | "Shaquile O'Neal" | 47  |
      | "Steve Nash"      | 45  |
      | "Tim Duncan"      | 42  |
      | "Vince Carter"    | 42  |
    When executing query:
      """
      LOOKUP ON player WHERE player.age < 40
      YIELD player.age AS Age, player.name AS Name | order by Age DESC, Name| limit 10
      """
    Then the result should be, in order, with relax comparison:
      | VertexID            | Age | Name                |
      | "Tracy McGrady"     | 39  | "Tracy McGrady"     |
      | "David West"        | 38  | "David West"        |
      | "Paul Gasol"        | 38  | "Paul Gasol"        |
      | "Yao Ming"          | 38  | "Yao Ming"          |
      | "Dwyane Wade"       | 37  | "Dwyane Wade"       |
      | "Amar'e Stoudemire" | 36  | "Amar'e Stoudemire" |
      | "Boris Diaw"        | 36  | "Boris Diaw"        |
      | "Tony Parker"       | 36  | "Tony Parker"       |
      | "Carmelo Anthony"   | 34  | "Carmelo Anthony"   |
      | "LeBron James"      | 34  | "LeBron James"      |
    When executing query:
      """
      LOOKUP ON player WHERE player.age <= 40
      YIELD player.age AS Age, player.name AS Name | order by Age DESC, Name| limit 10
      """
    Then the result should be, in order, with relax comparison:
      | VertexID            | Age | Name                |
      | "Dirk Nowitzki"     | 40  | "Dirk Nowitzki"     |
      | "Kobe Bryant"       | 40  | "Kobe Bryant"       |
      | "Tracy McGrady"     | 39  | "Tracy McGrady"     |
      | "David West"        | 38  | "David West"        |
      | "Paul Gasol"        | 38  | "Paul Gasol"        |
      | "Yao Ming"          | 38  | "Yao Ming"          |
      | "Dwyane Wade"       | 37  | "Dwyane Wade"       |
      | "Amar'e Stoudemire" | 36  | "Amar'e Stoudemire" |
      | "Boris Diaw"        | 36  | "Boris Diaw"        |
      | "Tony Parker"       | 36  | "Tony Parker"       |
    And drop the used space

  Scenario: [2] Compare INT and FLOAT during IndexScan
    Given having executed:
      """
      CREATE TAG weight (WEIGHT double);
      CREATE TAG INDEX weight_index ON weight(WEIGHT);
      """
    And wait 6 seconds
    When executing query:
      """
      INSERT VERTEX weight(WEIGHT) VALUES "Tim Duncan" : (70.5);
      INSERT VERTEX weight(WEIGHT) VALUES "Tony Parker" : (80.0);
      """
    Then the execution should be successful
    When executing query:
      """
      LOOKUP ON weight WHERE weight.WEIGHT > 70;
      """
    Then the result should be, in any order:
      | VertexID      |
      | "Tim Duncan"  |
      | "Tony Parker" |
    When executing query:
      """
      LOOKUP ON weight WHERE weight.WEIGHT > 70.4;
      """
    Then the result should be, in any order:
      | VertexID      |
      | "Tim Duncan"  |
      | "Tony Parker" |
    When executing query:
      """
      LOOKUP ON weight WHERE weight.WEIGHT >= 70.5;
      """
    Then the result should be, in any order:
      | VertexID      |
      | "Tim Duncan"  |
      | "Tony Parker" |
    Then drop the used space

# (TODO) Unsupported cases due to the lack of float precision
# When executing query:
# """
# LOOKUP ON weight
# WHERE weight.WEIGHT > 70.5;
# """
# Then the result should be, in any order:
# | VertexID      |
# | "Tony Parker" |
# When executing query:
# """
# LOOKUP ON weight
# WHERE weight.WEIGHT <= 80.0;
# """
# Then the result should be, in any order:
# | VertexID      |
# | "Tim Duncan"  |
# | "Tony Parker" |
