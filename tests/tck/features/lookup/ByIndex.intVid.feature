Feature: Lookup by index itself in integer vid

  Background:
    Given a graph with space named "nba_int_vid"

  Scenario: [1] tag index
    When executing query:
      """
      LOOKUP ON team
      """
    Then the result should be, in any order:
      | VertexID              |
      | hash('Nets')          |
      | hash('Pistons')       |
      | hash('Bucks')         |
      | hash('Mavericks')     |
      | hash('Clippers')      |
      | hash('Thunders')      |
      | hash('Lakers')        |
      | hash('Jazz')          |
      | hash('Nuggets')       |
      | hash('Wizards')       |
      | hash('Pacers')        |
      | hash('Timberwolves')  |
      | hash('Hawks')         |
      | hash('Warriors')      |
      | hash('Magic')         |
      | hash('Rockets')       |
      | hash('Pelicans')      |
      | hash('Raptors')       |
      | hash('Spurs')         |
      | hash('Heat')          |
      | hash('Grizzlies')     |
      | hash('Knicks')        |
      | hash('Suns')          |
      | hash('Hornets')       |
      | hash('Cavaliers')     |
      | hash('Kings')         |
      | hash('Celtics')       |
      | hash('76ers')         |
      | hash('Trail Blazers') |
      | hash('Bulls')         |
    When executing query:
      """
      LOOKUP ON team YIELD team.name AS Name
      """
    Then the result should be, in any order:
      | VertexID              | Name            |
      | hash('Nets')          | 'Nets'          |
      | hash('Pistons')       | 'Pistons'       |
      | hash('Bucks')         | 'Bucks'         |
      | hash('Mavericks')     | 'Mavericks'     |
      | hash('Clippers')      | 'Clippers'      |
      | hash('Thunders')      | 'Thunders'      |
      | hash('Lakers')        | 'Lakers'        |
      | hash('Jazz')          | 'Jazz'          |
      | hash('Nuggets')       | 'Nuggets'       |
      | hash('Wizards')       | 'Wizards'       |
      | hash('Pacers')        | 'Pacers'        |
      | hash('Timberwolves')  | 'Timberwolves'  |
      | hash('Hawks')         | 'Hawks'         |
      | hash('Warriors')      | 'Warriors'      |
      | hash('Magic')         | 'Magic'         |
      | hash('Rockets')       | 'Rockets'       |
      | hash('Pelicans')      | 'Pelicans'      |
      | hash('Raptors')       | 'Raptors'       |
      | hash('Spurs')         | 'Spurs'         |
      | hash('Heat')          | 'Heat'          |
      | hash('Grizzlies')     | 'Grizzlies'     |
      | hash('Knicks')        | 'Knicks'        |
      | hash('Suns')          | 'Suns'          |
      | hash('Hornets')       | 'Hornets'       |
      | hash('Cavaliers')     | 'Cavaliers'     |
      | hash('Kings')         | 'Kings'         |
      | hash('Celtics')       | 'Celtics'       |
      | hash('76ers')         | '76ers'         |
      | hash('Trail Blazers') | 'Trail Blazers' |
      | hash('Bulls')         | 'Bulls'         |

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
      LOOKUP ON team WHERE team.name CONTAINS 'Jazz'
      """
    Then a SemanticError should be raised at runtime:
    When executing query:
      """
      LOOKUP ON team WHERE team.name CONTAINS 'Jazz' YIELD team.name AS Name
      """
    Then a SemanticError should be raised at runtime:

  Scenario: [2] edge index
    When executing query:
      """
      LOOKUP ON serve
      """
    Then the result should be, in any order:
      | SrcVID                        | DstVID                | Ranking |
      | hash("Amar'e Stoudemire")     | hash('Suns')          | 0       |
      | hash("Amar'e Stoudemire")     | hash('Knicks')        | 0       |
      | hash("Amar'e Stoudemire")     | hash('Heat')          | 0       |
      | hash('Russell Westbrook')     | hash('Thunders')      | 0       |
      | hash('James Harden')          | hash('Thunders')      | 0       |
      | hash('James Harden')          | hash('Rockets')       | 0       |
      | hash('Kobe Bryant')           | hash('Lakers')        | 0       |
      | hash('Tracy McGrady')         | hash('Raptors')       | 0       |
      | hash('Tracy McGrady')         | hash('Magic')         | 0       |
      | hash('Tracy McGrady')         | hash('Rockets')       | 0       |
      | hash('Tracy McGrady')         | hash('Spurs')         | 0       |
      | hash('Chris Paul')            | hash('Hornets')       | 0       |
      | hash('Chris Paul')            | hash('Clippers')      | 0       |
      | hash('Chris Paul')            | hash('Rockets')       | 0       |
      | hash('Boris Diaw')            | hash('Hawks')         | 0       |
      | hash('Boris Diaw')            | hash('Suns')          | 0       |
      | hash('Boris Diaw')            | hash('Hornets')       | 0       |
      | hash('Boris Diaw')            | hash('Spurs')         | 0       |
      | hash('Boris Diaw')            | hash('Jazz')          | 0       |
      | hash('LeBron James')          | hash('Cavaliers')     | 0       |
      | hash('LeBron James')          | hash('Heat')          | 0       |
      | hash('LeBron James')          | hash('Cavaliers')     | 1       |
      | hash('LeBron James')          | hash('Lakers')        | 0       |
      | hash('Klay Thompson')         | hash('Warriors')      | 0       |
      | hash('Kristaps Porzingis')    | hash('Knicks')        | 0       |
      | hash('Kristaps Porzingis')    | hash('Mavericks')     | 0       |
      | hash('Jonathon Simmons')      | hash('Spurs')         | 0       |
      | hash('Jonathon Simmons')      | hash('Magic')         | 0       |
      | hash('Jonathon Simmons')      | hash('76ers')         | 0       |
      | hash('Marco Belinelli')       | hash('Warriors')      | 0       |
      | hash('Marco Belinelli')       | hash('Raptors')       | 0       |
      | hash('Marco Belinelli')       | hash('Hornets')       | 0       |
      | hash('Marco Belinelli')       | hash('Bulls')         | 0       |
      | hash('Marco Belinelli')       | hash('Spurs')         | 0       |
      | hash('Marco Belinelli')       | hash('Kings')         | 0       |
      | hash('Marco Belinelli')       | hash('Hornets')       | 1       |
      | hash('Marco Belinelli')       | hash('Hawks')         | 0       |
      | hash('Marco Belinelli')       | hash('76ers')         | 0       |
      | hash('Marco Belinelli')       | hash('Spurs')         | 1       |
      | hash('Luka Doncic')           | hash('Mavericks')     | 0       |
      | hash('David West')            | hash('Hornets')       | 0       |
      | hash('David West')            | hash('Pacers')        | 0       |
      | hash('David West')            | hash('Spurs')         | 0       |
      | hash('David West')            | hash('Warriors')      | 0       |
      | hash('Tony Parker')           | hash('Spurs')         | 0       |
      | hash('Tony Parker')           | hash('Hornets')       | 0       |
      | hash('Danny Green')           | hash('Cavaliers')     | 0       |
      | hash('Danny Green')           | hash('Spurs')         | 0       |
      | hash('Danny Green')           | hash('Raptors')       | 0       |
      | hash('Rudy Gay')              | hash('Grizzlies')     | 0       |
      | hash('Rudy Gay')              | hash('Raptors')       | 0       |
      | hash('Rudy Gay')              | hash('Kings')         | 0       |
      | hash('Rudy Gay')              | hash('Spurs')         | 0       |
      | hash('LaMarcus Aldridge')     | hash('Trail Blazers') | 0       |
      | hash('LaMarcus Aldridge')     | hash('Spurs')         | 0       |
      | hash('Tim Duncan')            | hash('Spurs')         | 0       |
      | hash('Kevin Durant')          | hash('Thunders')      | 0       |
      | hash('Kevin Durant')          | hash('Warriors')      | 0       |
      | hash('Stephen Curry')         | hash('Warriors')      | 0       |
      | hash('Ray Allen')             | hash('Bucks')         | 0       |
      | hash('Ray Allen')             | hash('Thunders')      | 0       |
      | hash('Ray Allen')             | hash('Celtics')       | 0       |
      | hash('Ray Allen')             | hash('Heat')          | 0       |
      | hash('Tiago Splitter')        | hash('Spurs')         | 0       |
      | hash('Tiago Splitter')        | hash('Hawks')         | 0       |
      | hash('Tiago Splitter')        | hash('76ers')         | 0       |
      | hash('DeAndre Jordan')        | hash('Clippers')      | 0       |
      | hash('DeAndre Jordan')        | hash('Mavericks')     | 0       |
      | hash('DeAndre Jordan')        | hash('Knicks')        | 0       |
      | hash('Paul Gasol')            | hash('Grizzlies')     | 0       |
      | hash('Paul Gasol')            | hash('Lakers')        | 0       |
      | hash('Paul Gasol')            | hash('Bulls')         | 0       |
      | hash('Paul Gasol')            | hash('Spurs')         | 0       |
      | hash('Paul Gasol')            | hash('Bucks')         | 0       |
      | hash('Aron Baynes')           | hash('Spurs')         | 0       |
      | hash('Aron Baynes')           | hash('Pistons')       | 0       |
      | hash('Aron Baynes')           | hash('Celtics')       | 0       |
      | hash('Cory Joseph')           | hash('Spurs')         | 0       |
      | hash('Cory Joseph')           | hash('Raptors')       | 0       |
      | hash('Cory Joseph')           | hash('Pacers')        | 0       |
      | hash('Vince Carter')          | hash('Raptors')       | 0       |
      | hash('Vince Carter')          | hash('Nets')          | 0       |
      | hash('Vince Carter')          | hash('Magic')         | 0       |
      | hash('Vince Carter')          | hash('Suns')          | 0       |
      | hash('Vince Carter')          | hash('Mavericks')     | 0       |
      | hash('Vince Carter')          | hash('Grizzlies')     | 0       |
      | hash('Vince Carter')          | hash('Kings')         | 0       |
      | hash('Vince Carter')          | hash('Hawks')         | 0       |
      | hash('Marc Gasol')            | hash('Grizzlies')     | 0       |
      | hash('Marc Gasol')            | hash('Raptors')       | 0       |
      | hash('Ricky Rubio')           | hash('Timberwolves')  | 0       |
      | hash('Ricky Rubio')           | hash('Jazz')          | 0       |
      | hash('Ben Simmons')           | hash('76ers')         | 0       |
      | hash('Giannis Antetokounmpo') | hash('Bucks')         | 0       |
      | hash('Rajon Rondo')           | hash('Celtics')       | 0       |
      | hash('Rajon Rondo')           | hash('Mavericks')     | 0       |
      | hash('Rajon Rondo')           | hash('Kings')         | 0       |
      | hash('Rajon Rondo')           | hash('Bulls')         | 0       |
      | hash('Rajon Rondo')           | hash('Pelicans')      | 0       |
      | hash('Rajon Rondo')           | hash('Lakers')        | 0       |
      | hash('Manu Ginobili')         | hash('Spurs')         | 0       |
      | hash('Kyrie Irving')          | hash('Cavaliers')     | 0       |
      | hash('Kyrie Irving')          | hash('Celtics')       | 0       |
      | hash('Carmelo Anthony')       | hash('Nuggets')       | 0       |
      | hash('Carmelo Anthony')       | hash('Knicks')        | 0       |
      | hash('Carmelo Anthony')       | hash('Thunders')      | 0       |
      | hash('Carmelo Anthony')       | hash('Rockets')       | 0       |
      | hash('Dwyane Wade')           | hash('Heat')          | 0       |
      | hash('Dwyane Wade')           | hash('Bulls')         | 0       |
      | hash('Dwyane Wade')           | hash('Cavaliers')     | 0       |
      | hash('Dwyane Wade')           | hash('Heat')          | 1       |
      | hash('Joel Embiid')           | hash('76ers')         | 0       |
      | hash('Damian Lillard')        | hash('Trail Blazers') | 0       |
      | hash('Yao Ming')              | hash('Rockets')       | 0       |
      | hash('Kyle Anderson')         | hash('Spurs')         | 0       |
      | hash('Kyle Anderson')         | hash('Grizzlies')     | 0       |
      | hash('Dejounte Murray')       | hash('Spurs')         | 0       |
      | hash('Blake Griffin')         | hash('Clippers')      | 0       |
      | hash('Blake Griffin')         | hash('Pistons')       | 0       |
      | hash('Steve Nash')            | hash('Suns')          | 0       |
      | hash('Steve Nash')            | hash('Mavericks')     | 0       |
      | hash('Steve Nash')            | hash('Suns')          | 1       |
      | hash('Steve Nash')            | hash('Lakers')        | 0       |
      | hash('Jason Kidd')            | hash('Mavericks')     | 0       |
      | hash('Jason Kidd')            | hash('Suns')          | 0       |
      | hash('Jason Kidd')            | hash('Nets')          | 0       |
      | hash('Jason Kidd')            | hash('Mavericks')     | 1       |
      | hash('Jason Kidd')            | hash('Knicks')        | 0       |
      | hash('Dirk Nowitzki')         | hash('Mavericks')     | 0       |
      | hash('Paul George')           | hash('Pacers')        | 0       |
      | hash('Paul George')           | hash('Thunders')      | 0       |
      | hash('Grant Hill')            | hash('Pistons')       | 0       |
      | hash('Grant Hill')            | hash('Magic')         | 0       |
      | hash('Grant Hill')            | hash('Suns')          | 0       |
      | hash('Grant Hill')            | hash('Clippers')      | 0       |
      | hash("Shaquile O'Neal")       | hash('Magic')         | 0       |
      | hash("Shaquile O'Neal")       | hash('Lakers')        | 0       |
      | hash("Shaquile O'Neal")       | hash('Heat')          | 0       |
      | hash("Shaquile O'Neal")       | hash('Suns')          | 0       |
      | hash("Shaquile O'Neal")       | hash('Cavaliers')     | 0       |
      | hash("Shaquile O'Neal")       | hash('Celtics')       | 0       |
      | hash('JaVale McGee')          | hash('Wizards')       | 0       |
      | hash('JaVale McGee')          | hash('Nuggets')       | 0       |
      | hash('JaVale McGee')          | hash('Mavericks')     | 0       |
      | hash('JaVale McGee')          | hash('Warriors')      | 0       |
      | hash('JaVale McGee')          | hash('Lakers')        | 0       |
      | hash('Dwight Howard')         | hash('Magic')         | 0       |
      | hash('Dwight Howard')         | hash('Lakers')        | 0       |
      | hash('Dwight Howard')         | hash('Rockets')       | 0       |
      | hash('Dwight Howard')         | hash('Hawks')         | 0       |
      | hash('Dwight Howard')         | hash('Hornets')       | 0       |
      | hash('Dwight Howard')         | hash('Wizards')       | 0       |
    When executing query:
      """
      LOOKUP ON serve YIELD serve.start_year AS startYear
      """
    Then the result should be, in any order:
      | SrcVID                        | DstVID                | Ranking | startYear |
      | hash("Amar'e Stoudemire")     | hash('Suns')          | 0       | 2002      |
      | hash("Amar'e Stoudemire")     | hash('Knicks')        | 0       | 2010      |
      | hash("Amar'e Stoudemire")     | hash('Heat')          | 0       | 2015      |
      | hash('Russell Westbrook')     | hash('Thunders')      | 0       | 2008      |
      | hash('James Harden')          | hash('Thunders')      | 0       | 2009      |
      | hash('James Harden')          | hash('Rockets')       | 0       | 2012      |
      | hash('Kobe Bryant')           | hash('Lakers')        | 0       | 1996      |
      | hash('Tracy McGrady')         | hash('Raptors')       | 0       | 1997      |
      | hash('Tracy McGrady')         | hash('Magic')         | 0       | 2000      |
      | hash('Tracy McGrady')         | hash('Rockets')       | 0       | 2004      |
      | hash('Tracy McGrady')         | hash('Spurs')         | 0       | 2013      |
      | hash('Chris Paul')            | hash('Hornets')       | 0       | 2005      |
      | hash('Chris Paul')            | hash('Clippers')      | 0       | 2011      |
      | hash('Chris Paul')            | hash('Rockets')       | 0       | 2017      |
      | hash('Boris Diaw')            | hash('Hawks')         | 0       | 2003      |
      | hash('Boris Diaw')            | hash('Suns')          | 0       | 2005      |
      | hash('Boris Diaw')            | hash('Hornets')       | 0       | 2008      |
      | hash('Boris Diaw')            | hash('Spurs')         | 0       | 2012      |
      | hash('Boris Diaw')            | hash('Jazz')          | 0       | 2016      |
      | hash('LeBron James')          | hash('Cavaliers')     | 0       | 2003      |
      | hash('LeBron James')          | hash('Heat')          | 0       | 2010      |
      | hash('LeBron James')          | hash('Cavaliers')     | 1       | 2014      |
      | hash('LeBron James')          | hash('Lakers')        | 0       | 2018      |
      | hash('Klay Thompson')         | hash('Warriors')      | 0       | 2011      |
      | hash('Kristaps Porzingis')    | hash('Knicks')        | 0       | 2015      |
      | hash('Kristaps Porzingis')    | hash('Mavericks')     | 0       | 2019      |
      | hash('Jonathon Simmons')      | hash('Spurs')         | 0       | 2015      |
      | hash('Jonathon Simmons')      | hash('Magic')         | 0       | 2017      |
      | hash('Jonathon Simmons')      | hash('76ers')         | 0       | 2019      |
      | hash('Marco Belinelli')       | hash('Warriors')      | 0       | 2007      |
      | hash('Marco Belinelli')       | hash('Raptors')       | 0       | 2009      |
      | hash('Marco Belinelli')       | hash('Hornets')       | 0       | 2010      |
      | hash('Marco Belinelli')       | hash('Bulls')         | 0       | 2012      |
      | hash('Marco Belinelli')       | hash('Spurs')         | 0       | 2013      |
      | hash('Marco Belinelli')       | hash('Kings')         | 0       | 2015      |
      | hash('Marco Belinelli')       | hash('Hornets')       | 1       | 2016      |
      | hash('Marco Belinelli')       | hash('Hawks')         | 0       | 2017      |
      | hash('Marco Belinelli')       | hash('76ers')         | 0       | 2018      |
      | hash('Marco Belinelli')       | hash('Spurs')         | 1       | 2018      |
      | hash('Luka Doncic')           | hash('Mavericks')     | 0       | 2018      |
      | hash('David West')            | hash('Hornets')       | 0       | 2003      |
      | hash('David West')            | hash('Pacers')        | 0       | 2011      |
      | hash('David West')            | hash('Spurs')         | 0       | 2015      |
      | hash('David West')            | hash('Warriors')      | 0       | 2016      |
      | hash('Tony Parker')           | hash('Spurs')         | 0       | 1999      |
      | hash('Tony Parker')           | hash('Hornets')       | 0       | 2018      |
      | hash('Danny Green')           | hash('Cavaliers')     | 0       | 2009      |
      | hash('Danny Green')           | hash('Spurs')         | 0       | 2010      |
      | hash('Danny Green')           | hash('Raptors')       | 0       | 2018      |
      | hash('Rudy Gay')              | hash('Grizzlies')     | 0       | 2006      |
      | hash('Rudy Gay')              | hash('Raptors')       | 0       | 2013      |
      | hash('Rudy Gay')              | hash('Kings')         | 0       | 2013      |
      | hash('Rudy Gay')              | hash('Spurs')         | 0       | 2017      |
      | hash('LaMarcus Aldridge')     | hash('Trail Blazers') | 0       | 2006      |
      | hash('LaMarcus Aldridge')     | hash('Spurs')         | 0       | 2015      |
      | hash('Tim Duncan')            | hash('Spurs')         | 0       | 1997      |
      | hash('Kevin Durant')          | hash('Thunders')      | 0       | 2007      |
      | hash('Kevin Durant')          | hash('Warriors')      | 0       | 2016      |
      | hash('Stephen Curry')         | hash('Warriors')      | 0       | 2009      |
      | hash('Ray Allen')             | hash('Bucks')         | 0       | 1996      |
      | hash('Ray Allen')             | hash('Thunders')      | 0       | 2003      |
      | hash('Ray Allen')             | hash('Celtics')       | 0       | 2007      |
      | hash('Ray Allen')             | hash('Heat')          | 0       | 2012      |
      | hash('Tiago Splitter')        | hash('Spurs')         | 0       | 2010      |
      | hash('Tiago Splitter')        | hash('Hawks')         | 0       | 2015      |
      | hash('Tiago Splitter')        | hash('76ers')         | 0       | 2017      |
      | hash('DeAndre Jordan')        | hash('Clippers')      | 0       | 2008      |
      | hash('DeAndre Jordan')        | hash('Mavericks')     | 0       | 2018      |
      | hash('DeAndre Jordan')        | hash('Knicks')        | 0       | 2019      |
      | hash('Paul Gasol')            | hash('Grizzlies')     | 0       | 2001      |
      | hash('Paul Gasol')            | hash('Lakers')        | 0       | 2008      |
      | hash('Paul Gasol')            | hash('Bulls')         | 0       | 2014      |
      | hash('Paul Gasol')            | hash('Spurs')         | 0       | 2016      |
      | hash('Paul Gasol')            | hash('Bucks')         | 0       | 2019      |
      | hash('Aron Baynes')           | hash('Spurs')         | 0       | 2013      |
      | hash('Aron Baynes')           | hash('Pistons')       | 0       | 2015      |
      | hash('Aron Baynes')           | hash('Celtics')       | 0       | 2017      |
      | hash('Cory Joseph')           | hash('Spurs')         | 0       | 2011      |
      | hash('Cory Joseph')           | hash('Raptors')       | 0       | 2015      |
      | hash('Cory Joseph')           | hash('Pacers')        | 0       | 2017      |
      | hash('Vince Carter')          | hash('Raptors')       | 0       | 1998      |
      | hash('Vince Carter')          | hash('Nets')          | 0       | 2004      |
      | hash('Vince Carter')          | hash('Magic')         | 0       | 2009      |
      | hash('Vince Carter')          | hash('Suns')          | 0       | 2010      |
      | hash('Vince Carter')          | hash('Mavericks')     | 0       | 2011      |
      | hash('Vince Carter')          | hash('Grizzlies')     | 0       | 2014      |
      | hash('Vince Carter')          | hash('Kings')         | 0       | 2017      |
      | hash('Vince Carter')          | hash('Hawks')         | 0       | 2018      |
      | hash('Marc Gasol')            | hash('Grizzlies')     | 0       | 2008      |
      | hash('Marc Gasol')            | hash('Raptors')       | 0       | 2019      |
      | hash('Ricky Rubio')           | hash('Timberwolves')  | 0       | 2011      |
      | hash('Ricky Rubio')           | hash('Jazz')          | 0       | 2017      |
      | hash('Ben Simmons')           | hash('76ers')         | 0       | 2016      |
      | hash('Giannis Antetokounmpo') | hash('Bucks')         | 0       | 2013      |
      | hash('Rajon Rondo')           | hash('Celtics')       | 0       | 2006      |
      | hash('Rajon Rondo')           | hash('Mavericks')     | 0       | 2014      |
      | hash('Rajon Rondo')           | hash('Kings')         | 0       | 2015      |
      | hash('Rajon Rondo')           | hash('Bulls')         | 0       | 2016      |
      | hash('Rajon Rondo')           | hash('Pelicans')      | 0       | 2017      |
      | hash('Rajon Rondo')           | hash('Lakers')        | 0       | 2018      |
      | hash('Manu Ginobili')         | hash('Spurs')         | 0       | 2002      |
      | hash('Kyrie Irving')          | hash('Cavaliers')     | 0       | 2011      |
      | hash('Kyrie Irving')          | hash('Celtics')       | 0       | 2017      |
      | hash('Carmelo Anthony')       | hash('Nuggets')       | 0       | 2003      |
      | hash('Carmelo Anthony')       | hash('Knicks')        | 0       | 2011      |
      | hash('Carmelo Anthony')       | hash('Thunders')      | 0       | 2017      |
      | hash('Carmelo Anthony')       | hash('Rockets')       | 0       | 2018      |
      | hash('Dwyane Wade')           | hash('Heat')          | 0       | 2003      |
      | hash('Dwyane Wade')           | hash('Bulls')         | 0       | 2016      |
      | hash('Dwyane Wade')           | hash('Cavaliers')     | 0       | 2017      |
      | hash('Dwyane Wade')           | hash('Heat')          | 1       | 2018      |
      | hash('Joel Embiid')           | hash('76ers')         | 0       | 2014      |
      | hash('Damian Lillard')        | hash('Trail Blazers') | 0       | 2012      |
      | hash('Yao Ming')              | hash('Rockets')       | 0       | 2002      |
      | hash('Kyle Anderson')         | hash('Spurs')         | 0       | 2014      |
      | hash('Kyle Anderson')         | hash('Grizzlies')     | 0       | 2018      |
      | hash('Dejounte Murray')       | hash('Spurs')         | 0       | 2016      |
      | hash('Blake Griffin')         | hash('Clippers')      | 0       | 2009      |
      | hash('Blake Griffin')         | hash('Pistons')       | 0       | 2018      |
      | hash('Steve Nash')            | hash('Suns')          | 0       | 1996      |
      | hash('Steve Nash')            | hash('Mavericks')     | 0       | 1998      |
      | hash('Steve Nash')            | hash('Suns')          | 1       | 2004      |
      | hash('Steve Nash')            | hash('Lakers')        | 0       | 2012      |
      | hash('Jason Kidd')            | hash('Mavericks')     | 0       | 1994      |
      | hash('Jason Kidd')            | hash('Suns')          | 0       | 1996      |
      | hash('Jason Kidd')            | hash('Nets')          | 0       | 2001      |
      | hash('Jason Kidd')            | hash('Mavericks')     | 1       | 2008      |
      | hash('Jason Kidd')            | hash('Knicks')        | 0       | 2012      |
      | hash('Dirk Nowitzki')         | hash('Mavericks')     | 0       | 1998      |
      | hash('Paul George')           | hash('Pacers')        | 0       | 2010      |
      | hash('Paul George')           | hash('Thunders')      | 0       | 2017      |
      | hash('Grant Hill')            | hash('Pistons')       | 0       | 1994      |
      | hash('Grant Hill')            | hash('Magic')         | 0       | 2000      |
      | hash('Grant Hill')            | hash('Suns')          | 0       | 2007      |
      | hash('Grant Hill')            | hash('Clippers')      | 0       | 2012      |
      | hash("Shaquile O'Neal")       | hash('Magic')         | 0       | 1992      |
      | hash("Shaquile O'Neal")       | hash('Lakers')        | 0       | 1996      |
      | hash("Shaquile O'Neal")       | hash('Heat')          | 0       | 2004      |
      | hash("Shaquile O'Neal")       | hash('Suns')          | 0       | 2008      |
      | hash("Shaquile O'Neal")       | hash('Cavaliers')     | 0       | 2009      |
      | hash("Shaquile O'Neal")       | hash('Celtics')       | 0       | 2010      |
      | hash('JaVale McGee')          | hash('Wizards')       | 0       | 2008      |
      | hash('JaVale McGee')          | hash('Nuggets')       | 0       | 2012      |
      | hash('JaVale McGee')          | hash('Mavericks')     | 0       | 2015      |
      | hash('JaVale McGee')          | hash('Warriors')      | 0       | 2016      |
      | hash('JaVale McGee')          | hash('Lakers')        | 0       | 2018      |
      | hash('Dwight Howard')         | hash('Magic')         | 0       | 2004      |
      | hash('Dwight Howard')         | hash('Lakers')        | 0       | 2012      |
      | hash('Dwight Howard')         | hash('Rockets')       | 0       | 2013      |
      | hash('Dwight Howard')         | hash('Hawks')         | 0       | 2016      |
      | hash('Dwight Howard')         | hash('Hornets')       | 0       | 2017      |
      | hash('Dwight Howard')         | hash('Wizards')       | 0       | 2018      |

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
