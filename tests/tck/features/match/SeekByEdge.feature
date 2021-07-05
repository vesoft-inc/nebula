Feature: Match seek by edge
  Examples:
    | space_name  |
    | nba         |
    | nba_int_vid |

  Background: Prepare space
    Given a graph with space named "<space_name>"

  Scenario Outline: seek by edge index
    When executing query:
      """
      MATCH (player)-[:serve]->(team)
      RETURN player.name, team.name
      """
    Then the result should be, in any order:
      | player.name             | team.name       |
      | "Amar'e Stoudemire"     | "Suns"          |
      | "Amar'e Stoudemire"     | "Knicks"        |
      | "Amar'e Stoudemire"     | "Heat"          |
      | "Russell Westbrook"     | "Thunders"      |
      | "James Harden"          | "Thunders"      |
      | "James Harden"          | "Rockets"       |
      | "Kobe Bryant"           | "Lakers"        |
      | "Tracy McGrady"         | "Raptors"       |
      | "Tracy McGrady"         | "Magic"         |
      | "Tracy McGrady"         | "Rockets"       |
      | "Tracy McGrady"         | "Spurs"         |
      | "Chris Paul"            | "Hornets"       |
      | "Chris Paul"            | "Clippers"      |
      | "Chris Paul"            | "Rockets"       |
      | "Boris Diaw"            | "Hawks"         |
      | "Boris Diaw"            | "Suns"          |
      | "Boris Diaw"            | "Hornets"       |
      | "Boris Diaw"            | "Spurs"         |
      | "Boris Diaw"            | "Jazz"          |
      | "LeBron James"          | "Cavaliers"     |
      | "LeBron James"          | "Heat"          |
      | "LeBron James"          | "Cavaliers"     |
      | "LeBron James"          | "Lakers"        |
      | "Klay Thompson"         | "Warriors"      |
      | "Kristaps Porzingis"    | "Knicks"        |
      | "Kristaps Porzingis"    | "Mavericks"     |
      | "Jonathon Simmons"      | "Spurs"         |
      | "Jonathon Simmons"      | "Magic"         |
      | "Jonathon Simmons"      | "76ers"         |
      | "Marco Belinelli"       | "Warriors"      |
      | "Marco Belinelli"       | "Raptors"       |
      | "Marco Belinelli"       | "Hornets"       |
      | "Marco Belinelli"       | "Bulls"         |
      | "Marco Belinelli"       | "Spurs"         |
      | "Marco Belinelli"       | "Kings"         |
      | "Marco Belinelli"       | "Hornets"       |
      | "Marco Belinelli"       | "Hawks"         |
      | "Marco Belinelli"       | "76ers"         |
      | "Marco Belinelli"       | "Spurs"         |
      | "Luka Doncic"           | "Mavericks"     |
      | "David West"            | "Hornets"       |
      | "David West"            | "Pacers"        |
      | "David West"            | "Spurs"         |
      | "David West"            | "Warriors"      |
      | "Tony Parker"           | "Spurs"         |
      | "Tony Parker"           | "Hornets"       |
      | "Danny Green"           | "Cavaliers"     |
      | "Danny Green"           | "Spurs"         |
      | "Danny Green"           | "Raptors"       |
      | "Rudy Gay"              | "Grizzlies"     |
      | "Rudy Gay"              | "Raptors"       |
      | "Rudy Gay"              | "Kings"         |
      | "Rudy Gay"              | "Spurs"         |
      | "LaMarcus Aldridge"     | "Trail Blazers" |
      | "LaMarcus Aldridge"     | "Spurs"         |
      | "Tim Duncan"            | "Spurs"         |
      | "Kevin Durant"          | "Thunders"      |
      | "Kevin Durant"          | "Warriors"      |
      | "Stephen Curry"         | "Warriors"      |
      | "Ray Allen"             | "Bucks"         |
      | "Ray Allen"             | "Thunders"      |
      | "Ray Allen"             | "Celtics"       |
      | "Ray Allen"             | "Heat"          |
      | "Tiago Splitter"        | "Spurs"         |
      | "Tiago Splitter"        | "Hawks"         |
      | "Tiago Splitter"        | "76ers"         |
      | "DeAndre Jordan"        | "Clippers"      |
      | "DeAndre Jordan"        | "Mavericks"     |
      | "DeAndre Jordan"        | "Knicks"        |
      | "Paul Gasol"            | "Grizzlies"     |
      | "Paul Gasol"            | "Lakers"        |
      | "Paul Gasol"            | "Bulls"         |
      | "Paul Gasol"            | "Spurs"         |
      | "Paul Gasol"            | "Bucks"         |
      | "Aron Baynes"           | "Spurs"         |
      | "Aron Baynes"           | "Pistons"       |
      | "Aron Baynes"           | "Celtics"       |
      | "Cory Joseph"           | "Spurs"         |
      | "Cory Joseph"           | "Raptors"       |
      | "Cory Joseph"           | "Pacers"        |
      | "Vince Carter"          | "Raptors"       |
      | "Vince Carter"          | "Nets"          |
      | "Vince Carter"          | "Magic"         |
      | "Vince Carter"          | "Suns"          |
      | "Vince Carter"          | "Mavericks"     |
      | "Vince Carter"          | "Grizzlies"     |
      | "Vince Carter"          | "Kings"         |
      | "Vince Carter"          | "Hawks"         |
      | "Marc Gasol"            | "Grizzlies"     |
      | "Marc Gasol"            | "Raptors"       |
      | "Ricky Rubio"           | "Timberwolves"  |
      | "Ricky Rubio"           | "Jazz"          |
      | "Ben Simmons"           | "76ers"         |
      | "Giannis Antetokounmpo" | "Bucks"         |
      | "Rajon Rondo"           | "Celtics"       |
      | "Rajon Rondo"           | "Mavericks"     |
      | "Rajon Rondo"           | "Kings"         |
      | "Rajon Rondo"           | "Bulls"         |
      | "Rajon Rondo"           | "Pelicans"      |
      | "Rajon Rondo"           | "Lakers"        |
      | "Manu Ginobili"         | "Spurs"         |
      | "Kyrie Irving"          | "Cavaliers"     |
      | "Kyrie Irving"          | "Celtics"       |
      | "Carmelo Anthony"       | "Nuggets"       |
      | "Carmelo Anthony"       | "Knicks"        |
      | "Carmelo Anthony"       | "Thunders"      |
      | "Carmelo Anthony"       | "Rockets"       |
      | "Dwyane Wade"           | "Heat"          |
      | "Dwyane Wade"           | "Bulls"         |
      | "Dwyane Wade"           | "Cavaliers"     |
      | "Dwyane Wade"           | "Heat"          |
      | "Joel Embiid"           | "76ers"         |
      | "Damian Lillard"        | "Trail Blazers" |
      | "Yao Ming"              | "Rockets"       |
      | "Kyle Anderson"         | "Spurs"         |
      | "Kyle Anderson"         | "Grizzlies"     |
      | "Dejounte Murray"       | "Spurs"         |
      | "Blake Griffin"         | "Clippers"      |
      | "Blake Griffin"         | "Pistons"       |
      | "Steve Nash"            | "Suns"          |
      | "Steve Nash"            | "Mavericks"     |
      | "Steve Nash"            | "Suns"          |
      | "Steve Nash"            | "Lakers"        |
      | "Jason Kidd"            | "Mavericks"     |
      | "Jason Kidd"            | "Suns"          |
      | "Jason Kidd"            | "Nets"          |
      | "Jason Kidd"            | "Mavericks"     |
      | "Jason Kidd"            | "Knicks"        |
      | "Dirk Nowitzki"         | "Mavericks"     |
      | "Paul George"           | "Pacers"        |
      | "Paul George"           | "Thunders"      |
      | "Grant Hill"            | "Pistons"       |
      | "Grant Hill"            | "Magic"         |
      | "Grant Hill"            | "Suns"          |
      | "Grant Hill"            | "Clippers"      |
      | "Shaquile O'Neal"       | "Magic"         |
      | "Shaquile O'Neal"       | "Lakers"        |
      | "Shaquile O'Neal"       | "Heat"          |
      | "Shaquile O'Neal"       | "Suns"          |
      | "Shaquile O'Neal"       | "Cavaliers"     |
      | "Shaquile O'Neal"       | "Celtics"       |
      | "JaVale McGee"          | "Wizards"       |
      | "JaVale McGee"          | "Nuggets"       |
      | "JaVale McGee"          | "Mavericks"     |
      | "JaVale McGee"          | "Warriors"      |
      | "JaVale McGee"          | "Lakers"        |
      | "Dwight Howard"         | "Magic"         |
      | "Dwight Howard"         | "Lakers"        |
      | "Dwight Howard"         | "Rockets"       |
      | "Dwight Howard"         | "Hawks"         |
      | "Dwight Howard"         | "Hornets"       |
      | "Dwight Howard"         | "Wizards"       |
    When executing query:
      """
      MATCH (team)<-[:serve]-(player)
      RETURN player.name, team.name
      """
    Then the result should be, in any order:
      | player.name             | team.name       |
      | "Amar'e Stoudemire"     | "Suns"          |
      | "Amar'e Stoudemire"     | "Knicks"        |
      | "Amar'e Stoudemire"     | "Heat"          |
      | "Russell Westbrook"     | "Thunders"      |
      | "James Harden"          | "Thunders"      |
      | "James Harden"          | "Rockets"       |
      | "Kobe Bryant"           | "Lakers"        |
      | "Tracy McGrady"         | "Raptors"       |
      | "Tracy McGrady"         | "Magic"         |
      | "Tracy McGrady"         | "Rockets"       |
      | "Tracy McGrady"         | "Spurs"         |
      | "Chris Paul"            | "Hornets"       |
      | "Chris Paul"            | "Clippers"      |
      | "Chris Paul"            | "Rockets"       |
      | "Boris Diaw"            | "Hawks"         |
      | "Boris Diaw"            | "Suns"          |
      | "Boris Diaw"            | "Hornets"       |
      | "Boris Diaw"            | "Spurs"         |
      | "Boris Diaw"            | "Jazz"          |
      | "LeBron James"          | "Cavaliers"     |
      | "LeBron James"          | "Heat"          |
      | "LeBron James"          | "Cavaliers"     |
      | "LeBron James"          | "Lakers"        |
      | "Klay Thompson"         | "Warriors"      |
      | "Kristaps Porzingis"    | "Knicks"        |
      | "Kristaps Porzingis"    | "Mavericks"     |
      | "Jonathon Simmons"      | "Spurs"         |
      | "Jonathon Simmons"      | "Magic"         |
      | "Jonathon Simmons"      | "76ers"         |
      | "Marco Belinelli"       | "Warriors"      |
      | "Marco Belinelli"       | "Raptors"       |
      | "Marco Belinelli"       | "Hornets"       |
      | "Marco Belinelli"       | "Bulls"         |
      | "Marco Belinelli"       | "Spurs"         |
      | "Marco Belinelli"       | "Kings"         |
      | "Marco Belinelli"       | "Hornets"       |
      | "Marco Belinelli"       | "Hawks"         |
      | "Marco Belinelli"       | "76ers"         |
      | "Marco Belinelli"       | "Spurs"         |
      | "Luka Doncic"           | "Mavericks"     |
      | "David West"            | "Hornets"       |
      | "David West"            | "Pacers"        |
      | "David West"            | "Spurs"         |
      | "David West"            | "Warriors"      |
      | "Tony Parker"           | "Spurs"         |
      | "Tony Parker"           | "Hornets"       |
      | "Danny Green"           | "Cavaliers"     |
      | "Danny Green"           | "Spurs"         |
      | "Danny Green"           | "Raptors"       |
      | "Rudy Gay"              | "Grizzlies"     |
      | "Rudy Gay"              | "Raptors"       |
      | "Rudy Gay"              | "Kings"         |
      | "Rudy Gay"              | "Spurs"         |
      | "LaMarcus Aldridge"     | "Trail Blazers" |
      | "LaMarcus Aldridge"     | "Spurs"         |
      | "Tim Duncan"            | "Spurs"         |
      | "Kevin Durant"          | "Thunders"      |
      | "Kevin Durant"          | "Warriors"      |
      | "Stephen Curry"         | "Warriors"      |
      | "Ray Allen"             | "Bucks"         |
      | "Ray Allen"             | "Thunders"      |
      | "Ray Allen"             | "Celtics"       |
      | "Ray Allen"             | "Heat"          |
      | "Tiago Splitter"        | "Spurs"         |
      | "Tiago Splitter"        | "Hawks"         |
      | "Tiago Splitter"        | "76ers"         |
      | "DeAndre Jordan"        | "Clippers"      |
      | "DeAndre Jordan"        | "Mavericks"     |
      | "DeAndre Jordan"        | "Knicks"        |
      | "Paul Gasol"            | "Grizzlies"     |
      | "Paul Gasol"            | "Lakers"        |
      | "Paul Gasol"            | "Bulls"         |
      | "Paul Gasol"            | "Spurs"         |
      | "Paul Gasol"            | "Bucks"         |
      | "Aron Baynes"           | "Spurs"         |
      | "Aron Baynes"           | "Pistons"       |
      | "Aron Baynes"           | "Celtics"       |
      | "Cory Joseph"           | "Spurs"         |
      | "Cory Joseph"           | "Raptors"       |
      | "Cory Joseph"           | "Pacers"        |
      | "Vince Carter"          | "Raptors"       |
      | "Vince Carter"          | "Nets"          |
      | "Vince Carter"          | "Magic"         |
      | "Vince Carter"          | "Suns"          |
      | "Vince Carter"          | "Mavericks"     |
      | "Vince Carter"          | "Grizzlies"     |
      | "Vince Carter"          | "Kings"         |
      | "Vince Carter"          | "Hawks"         |
      | "Marc Gasol"            | "Grizzlies"     |
      | "Marc Gasol"            | "Raptors"       |
      | "Ricky Rubio"           | "Timberwolves"  |
      | "Ricky Rubio"           | "Jazz"          |
      | "Ben Simmons"           | "76ers"         |
      | "Giannis Antetokounmpo" | "Bucks"         |
      | "Rajon Rondo"           | "Celtics"       |
      | "Rajon Rondo"           | "Mavericks"     |
      | "Rajon Rondo"           | "Kings"         |
      | "Rajon Rondo"           | "Bulls"         |
      | "Rajon Rondo"           | "Pelicans"      |
      | "Rajon Rondo"           | "Lakers"        |
      | "Manu Ginobili"         | "Spurs"         |
      | "Kyrie Irving"          | "Cavaliers"     |
      | "Kyrie Irving"          | "Celtics"       |
      | "Carmelo Anthony"       | "Nuggets"       |
      | "Carmelo Anthony"       | "Knicks"        |
      | "Carmelo Anthony"       | "Thunders"      |
      | "Carmelo Anthony"       | "Rockets"       |
      | "Dwyane Wade"           | "Heat"          |
      | "Dwyane Wade"           | "Bulls"         |
      | "Dwyane Wade"           | "Cavaliers"     |
      | "Dwyane Wade"           | "Heat"          |
      | "Joel Embiid"           | "76ers"         |
      | "Damian Lillard"        | "Trail Blazers" |
      | "Yao Ming"              | "Rockets"       |
      | "Kyle Anderson"         | "Spurs"         |
      | "Kyle Anderson"         | "Grizzlies"     |
      | "Dejounte Murray"       | "Spurs"         |
      | "Blake Griffin"         | "Clippers"      |
      | "Blake Griffin"         | "Pistons"       |
      | "Steve Nash"            | "Suns"          |
      | "Steve Nash"            | "Mavericks"     |
      | "Steve Nash"            | "Suns"          |
      | "Steve Nash"            | "Lakers"        |
      | "Jason Kidd"            | "Mavericks"     |
      | "Jason Kidd"            | "Suns"          |
      | "Jason Kidd"            | "Nets"          |
      | "Jason Kidd"            | "Mavericks"     |
      | "Jason Kidd"            | "Knicks"        |
      | "Dirk Nowitzki"         | "Mavericks"     |
      | "Paul George"           | "Pacers"        |
      | "Paul George"           | "Thunders"      |
      | "Grant Hill"            | "Pistons"       |
      | "Grant Hill"            | "Magic"         |
      | "Grant Hill"            | "Suns"          |
      | "Grant Hill"            | "Clippers"      |
      | "Shaquile O'Neal"       | "Magic"         |
      | "Shaquile O'Neal"       | "Lakers"        |
      | "Shaquile O'Neal"       | "Heat"          |
      | "Shaquile O'Neal"       | "Suns"          |
      | "Shaquile O'Neal"       | "Cavaliers"     |
      | "Shaquile O'Neal"       | "Celtics"       |
      | "JaVale McGee"          | "Wizards"       |
      | "JaVale McGee"          | "Nuggets"       |
      | "JaVale McGee"          | "Mavericks"     |
      | "JaVale McGee"          | "Warriors"      |
      | "JaVale McGee"          | "Lakers"        |
      | "Dwight Howard"         | "Magic"         |
      | "Dwight Howard"         | "Lakers"        |
      | "Dwight Howard"         | "Rockets"       |
      | "Dwight Howard"         | "Hawks"         |
      | "Dwight Howard"         | "Hornets"       |
      | "Dwight Howard"         | "Wizards"       |
    When executing query:
      """
      MATCH (team)-[:serve]-(player)
      RETURN player.name, team.name
      """
    Then the result should be, in any order:
      | player.name             | team.name               |
      | "Heat"                  | "Amar'e Stoudemire"     |
      | "Suns"                  | "Amar'e Stoudemire"     |
      | "Knicks"                | "Amar'e Stoudemire"     |
      | "Thunders"              | "Russell Westbrook"     |
      | "Rockets"               | "James Harden"          |
      | "Thunders"              | "James Harden"          |
      | "Lakers"                | "Kobe Bryant"           |
      | "Rockets"               | "Tracy McGrady"         |
      | "Spurs"                 | "Tracy McGrady"         |
      | "Magic"                 | "Tracy McGrady"         |
      | "Raptors"               | "Tracy McGrady"         |
      | "Hornets"               | "Chris Paul"            |
      | "Rockets"               | "Chris Paul"            |
      | "Clippers"              | "Chris Paul"            |
      | "Spurs"                 | "Boris Diaw"            |
      | "Hornets"               | "Boris Diaw"            |
      | "Hawks"                 | "Boris Diaw"            |
      | "Jazz"                  | "Boris Diaw"            |
      | "Suns"                  | "Boris Diaw"            |
      | "Heat"                  | "LeBron James"          |
      | "Cavaliers"             | "LeBron James"          |
      | "Cavaliers"             | "LeBron James"          |
      | "Lakers"                | "LeBron James"          |
      | "Warriors"              | "Klay Thompson"         |
      | "Knicks"                | "Kristaps Porzingis"    |
      | "Mavericks"             | "Kristaps Porzingis"    |
      | "Magic"                 | "Jonathon Simmons"      |
      | "76ers"                 | "Jonathon Simmons"      |
      | "Spurs"                 | "Jonathon Simmons"      |
      | "Kings"                 | "Marco Belinelli"       |
      | "Warriors"              | "Marco Belinelli"       |
      | "Raptors"               | "Marco Belinelli"       |
      | "Hornets"               | "Marco Belinelli"       |
      | "Spurs"                 | "Marco Belinelli"       |
      | "76ers"                 | "Marco Belinelli"       |
      | "Hawks"                 | "Marco Belinelli"       |
      | "Hornets"               | "Marco Belinelli"       |
      | "Spurs"                 | "Marco Belinelli"       |
      | "Bulls"                 | "Marco Belinelli"       |
      | "Mavericks"             | "Luka Doncic"           |
      | "Hornets"               | "David West"            |
      | "Warriors"              | "David West"            |
      | "Pacers"                | "David West"            |
      | "Spurs"                 | "David West"            |
      | "Spurs"                 | "Tony Parker"           |
      | "Hornets"               | "Tony Parker"           |
      | "Raptors"               | "Danny Green"           |
      | "Cavaliers"             | "Danny Green"           |
      | "Spurs"                 | "Danny Green"           |
      | "Grizzlies"             | "Rudy Gay"              |
      | "Kings"                 | "Rudy Gay"              |
      | "Spurs"                 | "Rudy Gay"              |
      | "Raptors"               | "Rudy Gay"              |
      | "Spurs"                 | "LaMarcus Aldridge"     |
      | "Trail Blazers"         | "LaMarcus Aldridge"     |
      | "Spurs"                 | "Tim Duncan"            |
      | "Thunders"              | "Kevin Durant"          |
      | "Warriors"              | "Kevin Durant"          |
      | "Warriors"              | "Stephen Curry"         |
      | "Bucks"                 | "Ray Allen"             |
      | "Thunders"              | "Ray Allen"             |
      | "Heat"                  | "Ray Allen"             |
      | "Celtics"               | "Ray Allen"             |
      | "76ers"                 | "Tiago Splitter"        |
      | "Spurs"                 | "Tiago Splitter"        |
      | "Hawks"                 | "Tiago Splitter"        |
      | "Clippers"              | "DeAndre Jordan"        |
      | "Mavericks"             | "DeAndre Jordan"        |
      | "Knicks"                | "DeAndre Jordan"        |
      | "Bucks"                 | "Paul Gasol"            |
      | "Spurs"                 | "Paul Gasol"            |
      | "Grizzlies"             | "Paul Gasol"            |
      | "Bulls"                 | "Paul Gasol"            |
      | "Lakers"                | "Paul Gasol"            |
      | "Celtics"               | "Aron Baynes"           |
      | "Pistons"               | "Aron Baynes"           |
      | "Spurs"                 | "Aron Baynes"           |
      | "Spurs"                 | "Cory Joseph"           |
      | "Raptors"               | "Cory Joseph"           |
      | "Pacers"                | "Cory Joseph"           |
      | "Nets"                  | "Vince Carter"          |
      | "Grizzlies"             | "Vince Carter"          |
      | "Raptors"               | "Vince Carter"          |
      | "Hawks"                 | "Vince Carter"          |
      | "Suns"                  | "Vince Carter"          |
      | "Mavericks"             | "Vince Carter"          |
      | "Magic"                 | "Vince Carter"          |
      | "Kings"                 | "Vince Carter"          |
      | "Raptors"               | "Marc Gasol"            |
      | "Grizzlies"             | "Marc Gasol"            |
      | "Jazz"                  | "Ricky Rubio"           |
      | "Timberwolves"          | "Ricky Rubio"           |
      | "76ers"                 | "Ben Simmons"           |
      | "Bucks"                 | "Giannis Antetokounmpo" |
      | "Bulls"                 | "Rajon Rondo"           |
      | "Kings"                 | "Rajon Rondo"           |
      | "Mavericks"             | "Rajon Rondo"           |
      | "Lakers"                | "Rajon Rondo"           |
      | "Celtics"               | "Rajon Rondo"           |
      | "Pelicans"              | "Rajon Rondo"           |
      | "Spurs"                 | "Manu Ginobili"         |
      | "Cavaliers"             | "Kyrie Irving"          |
      | "Celtics"               | "Kyrie Irving"          |
      | "Rockets"               | "Carmelo Anthony"       |
      | "Thunders"              | "Carmelo Anthony"       |
      | "Nuggets"               | "Carmelo Anthony"       |
      | "Knicks"                | "Carmelo Anthony"       |
      | "Bulls"                 | "Dwyane Wade"           |
      | "Heat"                  | "Dwyane Wade"           |
      | "Cavaliers"             | "Dwyane Wade"           |
      | "Heat"                  | "Dwyane Wade"           |
      | "76ers"                 | "Joel Embiid"           |
      | "Trail Blazers"         | "Damian Lillard"        |
      | "Rockets"               | "Yao Ming"              |
      | "Spurs"                 | "Kyle Anderson"         |
      | "Grizzlies"             | "Kyle Anderson"         |
      | "Spurs"                 | "Dejounte Murray"       |
      | "Clippers"              | "Blake Griffin"         |
      | "Pistons"               | "Blake Griffin"         |
      | "Suns"                  | "Steve Nash"            |
      | "Suns"                  | "Steve Nash"            |
      | "Mavericks"             | "Steve Nash"            |
      | "Lakers"                | "Steve Nash"            |
      | "Mavericks"             | "Jason Kidd"            |
      | "Mavericks"             | "Jason Kidd"            |
      | "Nets"                  | "Jason Kidd"            |
      | "Knicks"                | "Jason Kidd"            |
      | "Suns"                  | "Jason Kidd"            |
      | "Mavericks"             | "Dirk Nowitzki"         |
      | "Thunders"              | "Paul George"           |
      | "Pacers"                | "Paul George"           |
      | "Suns"                  | "Grant Hill"            |
      | "Magic"                 | "Grant Hill"            |
      | "Clippers"              | "Grant Hill"            |
      | "Pistons"               | "Grant Hill"            |
      | "Celtics"               | "Shaquile O'Neal"       |
      | "Cavaliers"             | "Shaquile O'Neal"       |
      | "Lakers"                | "Shaquile O'Neal"       |
      | "Magic"                 | "Shaquile O'Neal"       |
      | "Suns"                  | "Shaquile O'Neal"       |
      | "Heat"                  | "Shaquile O'Neal"       |
      | "Warriors"              | "JaVale McGee"          |
      | "Mavericks"             | "JaVale McGee"          |
      | "Wizards"               | "JaVale McGee"          |
      | "Lakers"                | "JaVale McGee"          |
      | "Nuggets"               | "JaVale McGee"          |
      | "Rockets"               | "Dwight Howard"         |
      | "Wizards"               | "Dwight Howard"         |
      | "Magic"                 | "Dwight Howard"         |
      | "Hornets"               | "Dwight Howard"         |
      | "Lakers"                | "Dwight Howard"         |
      | "Hawks"                 | "Dwight Howard"         |
      | "Vince Carter"          | "Nets"                  |
      | "Jason Kidd"            | "Nets"                  |
      | "Grant Hill"            | "Pistons"               |
      | "Blake Griffin"         | "Pistons"               |
      | "Aron Baynes"           | "Pistons"               |
      | "Ray Allen"             | "Bucks"                 |
      | "Paul Gasol"            | "Bucks"                 |
      | "Giannis Antetokounmpo" | "Bucks"                 |
      | "Jason Kidd"            | "Mavericks"             |
      | "Steve Nash"            | "Mavericks"             |
      | "Jason Kidd"            | "Mavericks"             |
      | "Dirk Nowitzki"         | "Mavericks"             |
      | "JaVale McGee"          | "Mavericks"             |
      | "Rajon Rondo"           | "Mavericks"             |
      | "Kristaps Porzingis"    | "Mavericks"             |
      | "Vince Carter"          | "Mavericks"             |
      | "DeAndre Jordan"        | "Mavericks"             |
      | "Luka Doncic"           | "Mavericks"             |
      | "Grant Hill"            | "Clippers"              |
      | "Blake Griffin"         | "Clippers"              |
      | "DeAndre Jordan"        | "Clippers"              |
      | "Chris Paul"            | "Clippers"              |
      | "Russell Westbrook"     | "Thunders"              |
      | "Kevin Durant"          | "Thunders"              |
      | "Carmelo Anthony"       | "Thunders"              |
      | "Ray Allen"             | "Thunders"              |
      | "James Harden"          | "Thunders"              |
      | "Paul George"           | "Thunders"              |
      | "Kobe Bryant"           | "Lakers"                |
      | "Steve Nash"            | "Lakers"                |
      | "Rajon Rondo"           | "Lakers"                |
      | "LeBron James"          | "Lakers"                |
      | "JaVale McGee"          | "Lakers"                |
      | "Dwight Howard"         | "Lakers"                |
      | "Shaquile O'Neal"       | "Lakers"                |
      | "Paul Gasol"            | "Lakers"                |
      | "Ricky Rubio"           | "Jazz"                  |
      | "Boris Diaw"            | "Jazz"                  |
      | "Carmelo Anthony"       | "Nuggets"               |
      | "JaVale McGee"          | "Nuggets"               |
      | "Dwight Howard"         | "Wizards"               |
      | "JaVale McGee"          | "Wizards"               |
      | "David West"            | "Pacers"                |
      | "Cory Joseph"           | "Pacers"                |
      | "Paul George"           | "Pacers"                |
      | "Ricky Rubio"           | "Timberwolves"          |
      | "Vince Carter"          | "Hawks"                 |
      | "Boris Diaw"            | "Hawks"                 |
      | "Marco Belinelli"       | "Hawks"                 |
      | "Tiago Splitter"        | "Hawks"                 |
      | "Dwight Howard"         | "Hawks"                 |
      | "Marco Belinelli"       | "Warriors"              |
      | "Klay Thompson"         | "Warriors"              |
      | "JaVale McGee"          | "Warriors"              |
      | "David West"            | "Warriors"              |
      | "Stephen Curry"         | "Warriors"              |
      | "Kevin Durant"          | "Warriors"              |
      | "Jonathon Simmons"      | "Magic"                 |
      | "Grant Hill"            | "Magic"                 |
      | "Dwight Howard"         | "Magic"                 |
      | "Tracy McGrady"         | "Magic"                 |
      | "Vince Carter"          | "Magic"                 |
      | "Shaquile O'Neal"       | "Magic"                 |
      | "Carmelo Anthony"       | "Rockets"               |
      | "Tracy McGrady"         | "Rockets"               |
      | "Dwight Howard"         | "Rockets"               |
      | "Yao Ming"              | "Rockets"               |
      | "James Harden"          | "Rockets"               |
      | "Chris Paul"            | "Rockets"               |
      | "Rajon Rondo"           | "Pelicans"              |
      | "Marc Gasol"            | "Raptors"               |
      | "Vince Carter"          | "Raptors"               |
      | "Danny Green"           | "Raptors"               |
      | "Marco Belinelli"       | "Raptors"               |
      | "Cory Joseph"           | "Raptors"               |
      | "Rudy Gay"              | "Raptors"               |
      | "Tracy McGrady"         | "Raptors"               |
      | "Boris Diaw"            | "Spurs"                 |
      | "Kyle Anderson"         | "Spurs"                 |
      | "Cory Joseph"           | "Spurs"                 |
      | "Tiago Splitter"        | "Spurs"                 |
      | "LaMarcus Aldridge"     | "Spurs"                 |
      | "Paul Gasol"            | "Spurs"                 |
      | "Marco Belinelli"       | "Spurs"                 |
      | "Tracy McGrady"         | "Spurs"                 |
      | "David West"            | "Spurs"                 |
      | "Manu Ginobili"         | "Spurs"                 |
      | "Tony Parker"           | "Spurs"                 |
      | "Rudy Gay"              | "Spurs"                 |
      | "Jonathon Simmons"      | "Spurs"                 |
      | "Aron Baynes"           | "Spurs"                 |
      | "Danny Green"           | "Spurs"                 |
      | "Tim Duncan"            | "Spurs"                 |
      | "Marco Belinelli"       | "Spurs"                 |
      | "Dejounte Murray"       | "Spurs"                 |
      | "LeBron James"          | "Heat"                  |
      | "Dwyane Wade"           | "Heat"                  |
      | "Ray Allen"             | "Heat"                  |
      | "Amar'e Stoudemire"     | "Heat"                  |
      | "Dwyane Wade"           | "Heat"                  |
      | "Shaquile O'Neal"       | "Heat"                  |
      | "Marc Gasol"            | "Grizzlies"             |
      | "Kyle Anderson"         | "Grizzlies"             |
      | "Vince Carter"          | "Grizzlies"             |
      | "Paul Gasol"            | "Grizzlies"             |
      | "Rudy Gay"              | "Grizzlies"             |
      | "Kristaps Porzingis"    | "Knicks"                |
      | "Jason Kidd"            | "Knicks"                |
      | "Carmelo Anthony"       | "Knicks"                |
      | "DeAndre Jordan"        | "Knicks"                |
      | "Amar'e Stoudemire"     | "Knicks"                |
      | "Steve Nash"            | "Suns"                  |
      | "Steve Nash"            | "Suns"                  |
      | "Grant Hill"            | "Suns"                  |
      | "Vince Carter"          | "Suns"                  |
      | "Amar'e Stoudemire"     | "Suns"                  |
      | "Shaquile O'Neal"       | "Suns"                  |
      | "Jason Kidd"            | "Suns"                  |
      | "Boris Diaw"            | "Suns"                  |
      | "David West"            | "Hornets"               |
      | "Boris Diaw"            | "Hornets"               |
      | "Marco Belinelli"       | "Hornets"               |
      | "Chris Paul"            | "Hornets"               |
      | "Dwight Howard"         | "Hornets"               |
      | "Tony Parker"           | "Hornets"               |
      | "Marco Belinelli"       | "Hornets"               |
      | "LeBron James"          | "Cavaliers"             |
      | "LeBron James"          | "Cavaliers"             |
      | "Dwyane Wade"           | "Cavaliers"             |
      | "Kyrie Irving"          | "Cavaliers"             |
      | "Danny Green"           | "Cavaliers"             |
      | "Shaquile O'Neal"       | "Cavaliers"             |
      | "Marco Belinelli"       | "Kings"                 |
      | "Rajon Rondo"           | "Kings"                 |
      | "Rudy Gay"              | "Kings"                 |
      | "Vince Carter"          | "Kings"                 |
      | "Aron Baynes"           | "Celtics"               |
      | "Shaquile O'Neal"       | "Celtics"               |
      | "Kyrie Irving"          | "Celtics"               |
      | "Rajon Rondo"           | "Celtics"               |
      | "Ray Allen"             | "Celtics"               |
      | "Ben Simmons"           | "76ers"                 |
      | "Joel Embiid"           | "76ers"                 |
      | "Tiago Splitter"        | "76ers"                 |
      | "Marco Belinelli"       | "76ers"                 |
      | "Jonathon Simmons"      | "76ers"                 |
      | "LaMarcus Aldridge"     | "Trail Blazers"         |
      | "Damian Lillard"        | "Trail Blazers"         |
      | "Dwyane Wade"           | "Bulls"                 |
      | "Rajon Rondo"           | "Bulls"                 |
      | "Paul Gasol"            | "Bulls"                 |
      | "Marco Belinelli"       | "Bulls"                 |
    When executing query:
      """
      MATCH (p1)-[:like]->(player)-[:serve]->(team)
      RETURN p1.name, player.name, team.name
      """
    Then the result should be, in any order:
      | p1.name              | player.name          | team.name       |
      | "Amar'e Stoudemire"  | "Steve Nash"         | "Suns"          |
      | "Amar'e Stoudemire"  | "Steve Nash"         | "Lakers"        |
      | "Amar'e Stoudemire"  | "Steve Nash"         | "Mavericks"     |
      | "Amar'e Stoudemire"  | "Steve Nash"         | "Suns"          |
      | "Russell Westbrook"  | "James Harden"       | "Thunders"      |
      | "Russell Westbrook"  | "James Harden"       | "Rockets"       |
      | "Russell Westbrook"  | "Paul George"        | "Thunders"      |
      | "Russell Westbrook"  | "Paul George"        | "Pacers"        |
      | "James Harden"       | "Russell Westbrook"  | "Thunders"      |
      | "Tracy McGrady"      | "Rudy Gay"           | "Raptors"       |
      | "Tracy McGrady"      | "Rudy Gay"           | "Spurs"         |
      | "Tracy McGrady"      | "Rudy Gay"           | "Kings"         |
      | "Tracy McGrady"      | "Rudy Gay"           | "Grizzlies"     |
      | "Tracy McGrady"      | "Grant Hill"         | "Clippers"      |
      | "Tracy McGrady"      | "Grant Hill"         | "Magic"         |
      | "Tracy McGrady"      | "Grant Hill"         | "Pistons"       |
      | "Tracy McGrady"      | "Grant Hill"         | "Suns"          |
      | "Tracy McGrady"      | "Kobe Bryant"        | "Lakers"        |
      | "Chris Paul"         | "Dwyane Wade"        | "Heat"          |
      | "Chris Paul"         | "Dwyane Wade"        | "Cavaliers"     |
      | "Chris Paul"         | "Dwyane Wade"        | "Bulls"         |
      | "Chris Paul"         | "Dwyane Wade"        | "Heat"          |
      | "Chris Paul"         | "Carmelo Anthony"    | "Knicks"        |
      | "Chris Paul"         | "Carmelo Anthony"    | "Rockets"       |
      | "Chris Paul"         | "Carmelo Anthony"    | "Nuggets"       |
      | "Chris Paul"         | "Carmelo Anthony"    | "Thunders"      |
      | "Chris Paul"         | "LeBron James"       | "Cavaliers"     |
      | "Chris Paul"         | "LeBron James"       | "Lakers"        |
      | "Chris Paul"         | "LeBron James"       | "Heat"          |
      | "Chris Paul"         | "LeBron James"       | "Cavaliers"     |
      | "Boris Diaw"         | "Tim Duncan"         | "Spurs"         |
      | "Boris Diaw"         | "Tony Parker"        | "Spurs"         |
      | "Boris Diaw"         | "Tony Parker"        | "Hornets"       |
      | "LeBron James"       | "Ray Allen"          | "Heat"          |
      | "LeBron James"       | "Ray Allen"          | "Celtics"       |
      | "LeBron James"       | "Ray Allen"          | "Bucks"         |
      | "LeBron James"       | "Ray Allen"          | "Thunders"      |
      | "Klay Thompson"      | "Stephen Curry"      | "Warriors"      |
      | "Kristaps Porzingis" | "Luka Doncic"        | "Mavericks"     |
      | "Marco Belinelli"    | "Danny Green"        | "Cavaliers"     |
      | "Marco Belinelli"    | "Danny Green"        | "Spurs"         |
      | "Marco Belinelli"    | "Danny Green"        | "Raptors"       |
      | "Marco Belinelli"    | "Tim Duncan"         | "Spurs"         |
      | "Marco Belinelli"    | "Tony Parker"        | "Spurs"         |
      | "Marco Belinelli"    | "Tony Parker"        | "Hornets"       |
      | "Luka Doncic"        | "James Harden"       | "Thunders"      |
      | "Luka Doncic"        | "James Harden"       | "Rockets"       |
      | "Luka Doncic"        | "Kristaps Porzingis" | "Knicks"        |
      | "Luka Doncic"        | "Kristaps Porzingis" | "Mavericks"     |
      | "Luka Doncic"        | "Dirk Nowitzki"      | "Mavericks"     |
      | "Tony Parker"        | "LaMarcus Aldridge"  | "Spurs"         |
      | "Tony Parker"        | "LaMarcus Aldridge"  | "Trail Blazers" |
      | "Tony Parker"        | "Manu Ginobili"      | "Spurs"         |
      | "Tony Parker"        | "Tim Duncan"         | "Spurs"         |
      | "Danny Green"        | "LeBron James"       | "Cavaliers"     |
      | "Danny Green"        | "LeBron James"       | "Lakers"        |
      | "Danny Green"        | "LeBron James"       | "Heat"          |
      | "Danny Green"        | "LeBron James"       | "Cavaliers"     |
      | "Danny Green"        | "Tim Duncan"         | "Spurs"         |
      | "Danny Green"        | "Marco Belinelli"    | "76ers"         |
      | "Danny Green"        | "Marco Belinelli"    | "Warriors"      |
      | "Danny Green"        | "Marco Belinelli"    | "Hornets"       |
      | "Danny Green"        | "Marco Belinelli"    | "Kings"         |
      | "Danny Green"        | "Marco Belinelli"    | "Raptors"       |
      | "Danny Green"        | "Marco Belinelli"    | "Bulls"         |
      | "Danny Green"        | "Marco Belinelli"    | "Spurs"         |
      | "Danny Green"        | "Marco Belinelli"    | "Hornets"       |
      | "Danny Green"        | "Marco Belinelli"    | "Spurs"         |
      | "Danny Green"        | "Marco Belinelli"    | "Hawks"         |
      | "Rudy Gay"           | "LaMarcus Aldridge"  | "Spurs"         |
      | "Rudy Gay"           | "LaMarcus Aldridge"  | "Trail Blazers" |
      | "LaMarcus Aldridge"  | "Tim Duncan"         | "Spurs"         |
      | "LaMarcus Aldridge"  | "Tony Parker"        | "Spurs"         |
      | "LaMarcus Aldridge"  | "Tony Parker"        | "Hornets"       |
      | "Tim Duncan"         | "Manu Ginobili"      | "Spurs"         |
      | "Tim Duncan"         | "Tony Parker"        | "Spurs"         |
      | "Tim Duncan"         | "Tony Parker"        | "Hornets"       |
      | "Ray Allen"          | "Rajon Rondo"        | "Mavericks"     |
      | "Ray Allen"          | "Rajon Rondo"        | "Kings"         |
      | "Ray Allen"          | "Rajon Rondo"        | "Celtics"       |
      | "Ray Allen"          | "Rajon Rondo"        | "Pelicans"      |
      | "Ray Allen"          | "Rajon Rondo"        | "Lakers"        |
      | "Ray Allen"          | "Rajon Rondo"        | "Bulls"         |
      | "Tiago Splitter"     | "Tim Duncan"         | "Spurs"         |
      | "Tiago Splitter"     | "Manu Ginobili"      | "Spurs"         |
      | "Paul Gasol"         | "Marc Gasol"         | "Raptors"       |
      | "Paul Gasol"         | "Marc Gasol"         | "Grizzlies"     |
      | "Paul Gasol"         | "Kobe Bryant"        | "Lakers"        |
      | "Aron Baynes"        | "Tim Duncan"         | "Spurs"         |
      | "Vince Carter"       | "Jason Kidd"         | "Nets"          |
      | "Vince Carter"       | "Jason Kidd"         | "Suns"          |
      | "Vince Carter"       | "Jason Kidd"         | "Mavericks"     |
      | "Vince Carter"       | "Jason Kidd"         | "Knicks"        |
      | "Vince Carter"       | "Jason Kidd"         | "Mavericks"     |
      | "Vince Carter"       | "Tracy McGrady"      | "Spurs"         |
      | "Vince Carter"       | "Tracy McGrady"      | "Magic"         |
      | "Vince Carter"       | "Tracy McGrady"      | "Rockets"       |
      | "Vince Carter"       | "Tracy McGrady"      | "Raptors"       |
      | "Marc Gasol"         | "Paul Gasol"         | "Lakers"        |
      | "Marc Gasol"         | "Paul Gasol"         | "Grizzlies"     |
      | "Marc Gasol"         | "Paul Gasol"         | "Bucks"         |
      | "Marc Gasol"         | "Paul Gasol"         | "Spurs"         |
      | "Marc Gasol"         | "Paul Gasol"         | "Bulls"         |
      | "Ben Simmons"        | "Joel Embiid"        | "76ers"         |
      | "Rajon Rondo"        | "Ray Allen"          | "Heat"          |
      | "Rajon Rondo"        | "Ray Allen"          | "Celtics"       |
      | "Rajon Rondo"        | "Ray Allen"          | "Bucks"         |
      | "Rajon Rondo"        | "Ray Allen"          | "Thunders"      |
      | "Manu Ginobili"      | "Tim Duncan"         | "Spurs"         |
      | "Kyrie Irving"       | "LeBron James"       | "Cavaliers"     |
      | "Kyrie Irving"       | "LeBron James"       | "Lakers"        |
      | "Kyrie Irving"       | "LeBron James"       | "Heat"          |
      | "Kyrie Irving"       | "LeBron James"       | "Cavaliers"     |
      | "Carmelo Anthony"    | "LeBron James"       | "Cavaliers"     |
      | "Carmelo Anthony"    | "LeBron James"       | "Lakers"        |
      | "Carmelo Anthony"    | "LeBron James"       | "Heat"          |
      | "Carmelo Anthony"    | "LeBron James"       | "Cavaliers"     |
      | "Carmelo Anthony"    | "Dwyane Wade"        | "Heat"          |
      | "Carmelo Anthony"    | "Dwyane Wade"        | "Cavaliers"     |
      | "Carmelo Anthony"    | "Dwyane Wade"        | "Bulls"         |
      | "Carmelo Anthony"    | "Dwyane Wade"        | "Heat"          |
      | "Carmelo Anthony"    | "Chris Paul"         | "Hornets"       |
      | "Carmelo Anthony"    | "Chris Paul"         | "Clippers"      |
      | "Carmelo Anthony"    | "Chris Paul"         | "Rockets"       |
      | "Dwyane Wade"        | "Carmelo Anthony"    | "Knicks"        |
      | "Dwyane Wade"        | "Carmelo Anthony"    | "Rockets"       |
      | "Dwyane Wade"        | "Carmelo Anthony"    | "Nuggets"       |
      | "Dwyane Wade"        | "Carmelo Anthony"    | "Thunders"      |
      | "Dwyane Wade"        | "Chris Paul"         | "Hornets"       |
      | "Dwyane Wade"        | "Chris Paul"         | "Clippers"      |
      | "Dwyane Wade"        | "Chris Paul"         | "Rockets"       |
      | "Dwyane Wade"        | "LeBron James"       | "Cavaliers"     |
      | "Dwyane Wade"        | "LeBron James"       | "Lakers"        |
      | "Dwyane Wade"        | "LeBron James"       | "Heat"          |
      | "Dwyane Wade"        | "LeBron James"       | "Cavaliers"     |
      | "Joel Embiid"        | "Ben Simmons"        | "76ers"         |
      | "Damian Lillard"     | "LaMarcus Aldridge"  | "Spurs"         |
      | "Damian Lillard"     | "LaMarcus Aldridge"  | "Trail Blazers" |
      | "Yao Ming"           | "Tracy McGrady"      | "Spurs"         |
      | "Yao Ming"           | "Tracy McGrady"      | "Magic"         |
      | "Yao Ming"           | "Tracy McGrady"      | "Rockets"       |
      | "Yao Ming"           | "Tracy McGrady"      | "Raptors"       |
      | "Yao Ming"           | "Shaquile O'Neal"    | "Suns"          |
      | "Yao Ming"           | "Shaquile O'Neal"    | "Celtics"       |
      | "Yao Ming"           | "Shaquile O'Neal"    | "Heat"          |
      | "Yao Ming"           | "Shaquile O'Neal"    | "Magic"         |
      | "Yao Ming"           | "Shaquile O'Neal"    | "Cavaliers"     |
      | "Yao Ming"           | "Shaquile O'Neal"    | "Lakers"        |
      | "Dejounte Murray"    | "Chris Paul"         | "Hornets"       |
      | "Dejounte Murray"    | "Chris Paul"         | "Clippers"      |
      | "Dejounte Murray"    | "Chris Paul"         | "Rockets"       |
      | "Dejounte Murray"    | "LeBron James"       | "Cavaliers"     |
      | "Dejounte Murray"    | "LeBron James"       | "Lakers"        |
      | "Dejounte Murray"    | "LeBron James"       | "Heat"          |
      | "Dejounte Murray"    | "LeBron James"       | "Cavaliers"     |
      | "Dejounte Murray"    | "James Harden"       | "Thunders"      |
      | "Dejounte Murray"    | "James Harden"       | "Rockets"       |
      | "Dejounte Murray"    | "Russell Westbrook"  | "Thunders"      |
      | "Dejounte Murray"    | "Marco Belinelli"    | "76ers"         |
      | "Dejounte Murray"    | "Marco Belinelli"    | "Warriors"      |
      | "Dejounte Murray"    | "Marco Belinelli"    | "Hornets"       |
      | "Dejounte Murray"    | "Marco Belinelli"    | "Kings"         |
      | "Dejounte Murray"    | "Marco Belinelli"    | "Raptors"       |
      | "Dejounte Murray"    | "Marco Belinelli"    | "Bulls"         |
      | "Dejounte Murray"    | "Marco Belinelli"    | "Spurs"         |
      | "Dejounte Murray"    | "Marco Belinelli"    | "Hornets"       |
      | "Dejounte Murray"    | "Marco Belinelli"    | "Spurs"         |
      | "Dejounte Murray"    | "Marco Belinelli"    | "Hawks"         |
      | "Dejounte Murray"    | "Danny Green"        | "Cavaliers"     |
      | "Dejounte Murray"    | "Danny Green"        | "Spurs"         |
      | "Dejounte Murray"    | "Danny Green"        | "Raptors"       |
      | "Dejounte Murray"    | "Kyle Anderson"      | "Spurs"         |
      | "Dejounte Murray"    | "Kyle Anderson"      | "Grizzlies"     |
      | "Dejounte Murray"    | "Tim Duncan"         | "Spurs"         |
      | "Dejounte Murray"    | "Manu Ginobili"      | "Spurs"         |
      | "Dejounte Murray"    | "Tony Parker"        | "Spurs"         |
      | "Dejounte Murray"    | "Tony Parker"        | "Hornets"       |
      | "Dejounte Murray"    | "Kevin Durant"       | "Warriors"      |
      | "Dejounte Murray"    | "Kevin Durant"       | "Thunders"      |
      | "Blake Griffin"      | "Chris Paul"         | "Hornets"       |
      | "Blake Griffin"      | "Chris Paul"         | "Clippers"      |
      | "Blake Griffin"      | "Chris Paul"         | "Rockets"       |
      | "Steve Nash"         | "Stephen Curry"      | "Warriors"      |
      | "Steve Nash"         | "Amar'e Stoudemire"  | "Knicks"        |
      | "Steve Nash"         | "Amar'e Stoudemire"  | "Suns"          |
      | "Steve Nash"         | "Amar'e Stoudemire"  | "Heat"          |
      | "Steve Nash"         | "Jason Kidd"         | "Nets"          |
      | "Steve Nash"         | "Jason Kidd"         | "Suns"          |
      | "Steve Nash"         | "Jason Kidd"         | "Mavericks"     |
      | "Steve Nash"         | "Jason Kidd"         | "Knicks"        |
      | "Steve Nash"         | "Jason Kidd"         | "Mavericks"     |
      | "Steve Nash"         | "Dirk Nowitzki"      | "Mavericks"     |
      | "Jason Kidd"         | "Steve Nash"         | "Suns"          |
      | "Jason Kidd"         | "Steve Nash"         | "Lakers"        |
      | "Jason Kidd"         | "Steve Nash"         | "Mavericks"     |
      | "Jason Kidd"         | "Steve Nash"         | "Suns"          |
      | "Jason Kidd"         | "Dirk Nowitzki"      | "Mavericks"     |
      | "Jason Kidd"         | "Vince Carter"       | "Hawks"         |
      | "Jason Kidd"         | "Vince Carter"       | "Kings"         |
      | "Jason Kidd"         | "Vince Carter"       | "Magic"         |
      | "Jason Kidd"         | "Vince Carter"       | "Raptors"       |
      | "Jason Kidd"         | "Vince Carter"       | "Suns"          |
      | "Jason Kidd"         | "Vince Carter"       | "Nets"          |
      | "Jason Kidd"         | "Vince Carter"       | "Mavericks"     |
      | "Jason Kidd"         | "Vince Carter"       | "Grizzlies"     |
      | "Dirk Nowitzki"      | "Jason Kidd"         | "Nets"          |
      | "Dirk Nowitzki"      | "Jason Kidd"         | "Suns"          |
      | "Dirk Nowitzki"      | "Jason Kidd"         | "Mavericks"     |
      | "Dirk Nowitzki"      | "Jason Kidd"         | "Knicks"        |
      | "Dirk Nowitzki"      | "Jason Kidd"         | "Mavericks"     |
      | "Dirk Nowitzki"      | "Dwyane Wade"        | "Heat"          |
      | "Dirk Nowitzki"      | "Dwyane Wade"        | "Cavaliers"     |
      | "Dirk Nowitzki"      | "Dwyane Wade"        | "Bulls"         |
      | "Dirk Nowitzki"      | "Dwyane Wade"        | "Heat"          |
      | "Dirk Nowitzki"      | "Steve Nash"         | "Suns"          |
      | "Dirk Nowitzki"      | "Steve Nash"         | "Lakers"        |
      | "Dirk Nowitzki"      | "Steve Nash"         | "Mavericks"     |
      | "Dirk Nowitzki"      | "Steve Nash"         | "Suns"          |
      | "Paul George"        | "Russell Westbrook"  | "Thunders"      |
      | "Grant Hill"         | "Tracy McGrady"      | "Spurs"         |
      | "Grant Hill"         | "Tracy McGrady"      | "Magic"         |
      | "Grant Hill"         | "Tracy McGrady"      | "Rockets"       |
      | "Grant Hill"         | "Tracy McGrady"      | "Raptors"       |
      | "Shaquile O'Neal"    | "Tim Duncan"         | "Spurs"         |
      | "Shaquile O'Neal"    | "JaVale McGee"       | "Lakers"        |
      | "Shaquile O'Neal"    | "JaVale McGee"       | "Warriors"      |
      | "Shaquile O'Neal"    | "JaVale McGee"       | "Wizards"       |
      | "Shaquile O'Neal"    | "JaVale McGee"       | "Nuggets"       |
      | "Shaquile O'Neal"    | "JaVale McGee"       | "Mavericks"     |

  Scenario Outline: Seek by edge with range
    When executing query:
      """
      match (p1)-[:like*2]->(p2) return p1.name, p2.name
      """
    Then the result should be, in any order:
      | p1.name              | p2.name              |
      | "Amar'e Stoudemire"  | "Dirk Nowitzki"      |
      | "Amar'e Stoudemire"  | "Jason Kidd"         |
      | "Amar'e Stoudemire"  | "Amar'e Stoudemire"  |
      | "Amar'e Stoudemire"  | "Stephen Curry"      |
      | "Russell Westbrook"  | "Russell Westbrook"  |
      | "Russell Westbrook"  | "Russell Westbrook"  |
      | "James Harden"       | "Paul George"        |
      | "James Harden"       | "James Harden"       |
      | "Tracy McGrady"      | "Tracy McGrady"      |
      | "Tracy McGrady"      | "LaMarcus Aldridge"  |
      | "Chris Paul"         | "Ray Allen"          |
      | "Chris Paul"         | "Chris Paul"         |
      | "Chris Paul"         | "Dwyane Wade"        |
      | "Chris Paul"         | "LeBron James"       |
      | "Chris Paul"         | "LeBron James"       |
      | "Chris Paul"         | "Chris Paul"         |
      | "Chris Paul"         | "Carmelo Anthony"    |
      | "Boris Diaw"         | "Tim Duncan"         |
      | "Boris Diaw"         | "Manu Ginobili"      |
      | "Boris Diaw"         | "LaMarcus Aldridge"  |
      | "Boris Diaw"         | "Tony Parker"        |
      | "Boris Diaw"         | "Manu Ginobili"      |
      | "LeBron James"       | "Rajon Rondo"        |
      | "Kristaps Porzingis" | "Dirk Nowitzki"      |
      | "Kristaps Porzingis" | "Kristaps Porzingis" |
      | "Kristaps Porzingis" | "James Harden"       |
      | "Marco Belinelli"    | "Tim Duncan"         |
      | "Marco Belinelli"    | "Manu Ginobili"      |
      | "Marco Belinelli"    | "LaMarcus Aldridge"  |
      | "Marco Belinelli"    | "Tony Parker"        |
      | "Marco Belinelli"    | "Manu Ginobili"      |
      | "Marco Belinelli"    | "Marco Belinelli"    |
      | "Marco Belinelli"    | "Tim Duncan"         |
      | "Marco Belinelli"    | "LeBron James"       |
      | "Luka Doncic"        | "Steve Nash"         |
      | "Luka Doncic"        | "Dwyane Wade"        |
      | "Luka Doncic"        | "Jason Kidd"         |
      | "Luka Doncic"        | "Luka Doncic"        |
      | "Luka Doncic"        | "Russell Westbrook"  |
      | "Tony Parker"        | "Tony Parker"        |
      | "Tony Parker"        | "Manu Ginobili"      |
      | "Tony Parker"        | "Tim Duncan"         |
      | "Tony Parker"        | "Tony Parker"        |
      | "Tony Parker"        | "Tim Duncan"         |
      | "Danny Green"        | "Tony Parker"        |
      | "Danny Green"        | "Tim Duncan"         |
      | "Danny Green"        | "Danny Green"        |
      | "Danny Green"        | "Tony Parker"        |
      | "Danny Green"        | "Manu Ginobili"      |
      | "Danny Green"        | "Ray Allen"          |
      | "Rudy Gay"           | "Tony Parker"        |
      | "Rudy Gay"           | "Tim Duncan"         |
      | "LaMarcus Aldridge"  | "Tim Duncan"         |
      | "LaMarcus Aldridge"  | "Manu Ginobili"      |
      | "LaMarcus Aldridge"  | "LaMarcus Aldridge"  |
      | "LaMarcus Aldridge"  | "Tony Parker"        |
      | "LaMarcus Aldridge"  | "Manu Ginobili"      |
      | "Tim Duncan"         | "Tim Duncan"         |
      | "Tim Duncan"         | "Manu Ginobili"      |
      | "Tim Duncan"         | "LaMarcus Aldridge"  |
      | "Tim Duncan"         | "Tim Duncan"         |
      | "Ray Allen"          | "Ray Allen"          |
      | "Tiago Splitter"     | "Tim Duncan"         |
      | "Tiago Splitter"     | "Tony Parker"        |
      | "Tiago Splitter"     | "Manu Ginobili"      |
      | "Paul Gasol"         | "Paul Gasol"         |
      | "Aron Baynes"        | "Tony Parker"        |
      | "Aron Baynes"        | "Manu Ginobili"      |
      | "Vince Carter"       | "Kobe Bryant"        |
      | "Vince Carter"       | "Grant Hill"         |
      | "Vince Carter"       | "Rudy Gay"           |
      | "Vince Carter"       | "Vince Carter"       |
      | "Vince Carter"       | "Dirk Nowitzki"      |
      | "Vince Carter"       | "Steve Nash"         |
      | "Marc Gasol"         | "Kobe Bryant"        |
      | "Marc Gasol"         | "Marc Gasol"         |
      | "Ben Simmons"        | "Ben Simmons"        |
      | "Rajon Rondo"        | "Rajon Rondo"        |
      | "Manu Ginobili"      | "Tony Parker"        |
      | "Manu Ginobili"      | "Manu Ginobili"      |
      | "Kyrie Irving"       | "Ray Allen"          |
      | "Carmelo Anthony"    | "LeBron James"       |
      | "Carmelo Anthony"    | "Carmelo Anthony"    |
      | "Carmelo Anthony"    | "Dwyane Wade"        |
      | "Carmelo Anthony"    | "LeBron James"       |
      | "Carmelo Anthony"    | "Chris Paul"         |
      | "Carmelo Anthony"    | "Carmelo Anthony"    |
      | "Carmelo Anthony"    | "Ray Allen"          |
      | "Dwyane Wade"        | "Ray Allen"          |
      | "Dwyane Wade"        | "LeBron James"       |
      | "Dwyane Wade"        | "Carmelo Anthony"    |
      | "Dwyane Wade"        | "Dwyane Wade"        |
      | "Dwyane Wade"        | "Chris Paul"         |
      | "Dwyane Wade"        | "Dwyane Wade"        |
      | "Dwyane Wade"        | "LeBron James"       |
      | "Joel Embiid"        | "Joel Embiid"        |
      | "Damian Lillard"     | "Tony Parker"        |
      | "Damian Lillard"     | "Tim Duncan"         |
      | "Yao Ming"           | "JaVale McGee"       |
      | "Yao Ming"           | "Tim Duncan"         |
      | "Yao Ming"           | "Kobe Bryant"        |
      | "Yao Ming"           | "Grant Hill"         |
      | "Yao Ming"           | "Rudy Gay"           |
      | "Dejounte Murray"    | "Tim Duncan"         |
      | "Dejounte Murray"    | "Manu Ginobili"      |
      | "Dejounte Murray"    | "LaMarcus Aldridge"  |
      | "Dejounte Murray"    | "Tim Duncan"         |
      | "Dejounte Murray"    | "Tony Parker"        |
      | "Dejounte Murray"    | "Manu Ginobili"      |
      | "Dejounte Murray"    | "Marco Belinelli"    |
      | "Dejounte Murray"    | "Tim Duncan"         |
      | "Dejounte Murray"    | "LeBron James"       |
      | "Dejounte Murray"    | "Tony Parker"        |
      | "Dejounte Murray"    | "Tim Duncan"         |
      | "Dejounte Murray"    | "Danny Green"        |
      | "Dejounte Murray"    | "Paul George"        |
      | "Dejounte Murray"    | "James Harden"       |
      | "Dejounte Murray"    | "Russell Westbrook"  |
      | "Dejounte Murray"    | "Ray Allen"          |
      | "Dejounte Murray"    | "LeBron James"       |
      | "Dejounte Murray"    | "Carmelo Anthony"    |
      | "Dejounte Murray"    | "Dwyane Wade"        |
      | "Blake Griffin"      | "LeBron James"       |
      | "Blake Griffin"      | "Carmelo Anthony"    |
      | "Blake Griffin"      | "Dwyane Wade"        |
      | "Steve Nash"         | "Steve Nash"         |
      | "Steve Nash"         | "Dwyane Wade"        |
      | "Steve Nash"         | "Jason Kidd"         |
      | "Steve Nash"         | "Vince Carter"       |
      | "Steve Nash"         | "Dirk Nowitzki"      |
      | "Steve Nash"         | "Steve Nash"         |
      | "Steve Nash"         | "Steve Nash"         |
      | "Jason Kidd"         | "Tracy McGrady"      |
      | "Jason Kidd"         | "Jason Kidd"         |
      | "Jason Kidd"         | "Steve Nash"         |
      | "Jason Kidd"         | "Dwyane Wade"        |
      | "Jason Kidd"         | "Jason Kidd"         |
      | "Jason Kidd"         | "Dirk Nowitzki"      |
      | "Jason Kidd"         | "Jason Kidd"         |
      | "Jason Kidd"         | "Amar'e Stoudemire"  |
      | "Jason Kidd"         | "Stephen Curry"      |
      | "Dirk Nowitzki"      | "Dirk Nowitzki"      |
      | "Dirk Nowitzki"      | "Jason Kidd"         |
      | "Dirk Nowitzki"      | "Amar'e Stoudemire"  |
      | "Dirk Nowitzki"      | "Stephen Curry"      |
      | "Dirk Nowitzki"      | "LeBron James"       |
      | "Dirk Nowitzki"      | "Chris Paul"         |
      | "Dirk Nowitzki"      | "Carmelo Anthony"    |
      | "Dirk Nowitzki"      | "Vince Carter"       |
      | "Dirk Nowitzki"      | "Dirk Nowitzki"      |
      | "Dirk Nowitzki"      | "Steve Nash"         |
      | "Paul George"        | "Paul George"        |
      | "Paul George"        | "James Harden"       |
      | "Grant Hill"         | "Kobe Bryant"        |
      | "Grant Hill"         | "Grant Hill"         |
      | "Grant Hill"         | "Rudy Gay"           |
      | "Shaquile O'Neal"    | "Tony Parker"        |
      | "Shaquile O'Neal"    | "Manu Ginobili"      |
    When executing query:
      """
      match (p1)-[:like*1..2]->(p2) return p1.name, p2.name
      """
    Then the result should be, in any order:
      | p1.name              | p2.name              |
      | "Amar'e Stoudemire"  | "Steve Nash"         |
      | "Amar'e Stoudemire"  | "Dirk Nowitzki"      |
      | "Amar'e Stoudemire"  | "Jason Kidd"         |
      | "Amar'e Stoudemire"  | "Amar'e Stoudemire"  |
      | "Amar'e Stoudemire"  | "Stephen Curry"      |
      | "Russell Westbrook"  | "Paul George"        |
      | "Russell Westbrook"  | "Russell Westbrook"  |
      | "Russell Westbrook"  | "James Harden"       |
      | "Russell Westbrook"  | "Russell Westbrook"  |
      | "James Harden"       | "Russell Westbrook"  |
      | "James Harden"       | "Paul George"        |
      | "James Harden"       | "James Harden"       |
      | "Tracy McGrady"      | "Kobe Bryant"        |
      | "Tracy McGrady"      | "Grant Hill"         |
      | "Tracy McGrady"      | "Tracy McGrady"      |
      | "Tracy McGrady"      | "Rudy Gay"           |
      | "Tracy McGrady"      | "LaMarcus Aldridge"  |
      | "Chris Paul"         | "LeBron James"       |
      | "Chris Paul"         | "Ray Allen"          |
      | "Chris Paul"         | "Carmelo Anthony"    |
      | "Chris Paul"         | "Chris Paul"         |
      | "Chris Paul"         | "Dwyane Wade"        |
      | "Chris Paul"         | "LeBron James"       |
      | "Chris Paul"         | "Dwyane Wade"        |
      | "Chris Paul"         | "LeBron James"       |
      | "Chris Paul"         | "Chris Paul"         |
      | "Chris Paul"         | "Carmelo Anthony"    |
      | "Boris Diaw"         | "Tony Parker"        |
      | "Boris Diaw"         | "Tim Duncan"         |
      | "Boris Diaw"         | "Manu Ginobili"      |
      | "Boris Diaw"         | "LaMarcus Aldridge"  |
      | "Boris Diaw"         | "Tim Duncan"         |
      | "Boris Diaw"         | "Tony Parker"        |
      | "Boris Diaw"         | "Manu Ginobili"      |
      | "LeBron James"       | "Ray Allen"          |
      | "LeBron James"       | "Rajon Rondo"        |
      | "Klay Thompson"      | "Stephen Curry"      |
      | "Kristaps Porzingis" | "Luka Doncic"        |
      | "Kristaps Porzingis" | "Dirk Nowitzki"      |
      | "Kristaps Porzingis" | "Kristaps Porzingis" |
      | "Kristaps Porzingis" | "James Harden"       |
      | "Marco Belinelli"    | "Tony Parker"        |
      | "Marco Belinelli"    | "Tim Duncan"         |
      | "Marco Belinelli"    | "Manu Ginobili"      |
      | "Marco Belinelli"    | "LaMarcus Aldridge"  |
      | "Marco Belinelli"    | "Tim Duncan"         |
      | "Marco Belinelli"    | "Tony Parker"        |
      | "Marco Belinelli"    | "Manu Ginobili"      |
      | "Marco Belinelli"    | "Danny Green"        |
      | "Marco Belinelli"    | "Marco Belinelli"    |
      | "Marco Belinelli"    | "Tim Duncan"         |
      | "Marco Belinelli"    | "LeBron James"       |
      | "Luka Doncic"        | "Dirk Nowitzki"      |
      | "Luka Doncic"        | "Steve Nash"         |
      | "Luka Doncic"        | "Dwyane Wade"        |
      | "Luka Doncic"        | "Jason Kidd"         |
      | "Luka Doncic"        | "Kristaps Porzingis" |
      | "Luka Doncic"        | "Luka Doncic"        |
      | "Luka Doncic"        | "James Harden"       |
      | "Luka Doncic"        | "Russell Westbrook"  |
      | "Tony Parker"        | "Tim Duncan"         |
      | "Tony Parker"        | "Tony Parker"        |
      | "Tony Parker"        | "Manu Ginobili"      |
      | "Tony Parker"        | "Manu Ginobili"      |
      | "Tony Parker"        | "Tim Duncan"         |
      | "Tony Parker"        | "LaMarcus Aldridge"  |
      | "Tony Parker"        | "Tony Parker"        |
      | "Tony Parker"        | "Tim Duncan"         |
      | "Danny Green"        | "Marco Belinelli"    |
      | "Danny Green"        | "Tony Parker"        |
      | "Danny Green"        | "Tim Duncan"         |
      | "Danny Green"        | "Danny Green"        |
      | "Danny Green"        | "Tim Duncan"         |
      | "Danny Green"        | "Tony Parker"        |
      | "Danny Green"        | "Manu Ginobili"      |
      | "Danny Green"        | "LeBron James"       |
      | "Danny Green"        | "Ray Allen"          |
      | "Rudy Gay"           | "LaMarcus Aldridge"  |
      | "Rudy Gay"           | "Tony Parker"        |
      | "Rudy Gay"           | "Tim Duncan"         |
      | "LaMarcus Aldridge"  | "Tony Parker"        |
      | "LaMarcus Aldridge"  | "Tim Duncan"         |
      | "LaMarcus Aldridge"  | "Manu Ginobili"      |
      | "LaMarcus Aldridge"  | "LaMarcus Aldridge"  |
      | "LaMarcus Aldridge"  | "Tim Duncan"         |
      | "LaMarcus Aldridge"  | "Tony Parker"        |
      | "LaMarcus Aldridge"  | "Manu Ginobili"      |
      | "Tim Duncan"         | "Tony Parker"        |
      | "Tim Duncan"         | "Tim Duncan"         |
      | "Tim Duncan"         | "Manu Ginobili"      |
      | "Tim Duncan"         | "LaMarcus Aldridge"  |
      | "Tim Duncan"         | "Manu Ginobili"      |
      | "Tim Duncan"         | "Tim Duncan"         |
      | "Ray Allen"          | "Rajon Rondo"        |
      | "Ray Allen"          | "Ray Allen"          |
      | "Tiago Splitter"     | "Manu Ginobili"      |
      | "Tiago Splitter"     | "Tim Duncan"         |
      | "Tiago Splitter"     | "Tim Duncan"         |
      | "Tiago Splitter"     | "Tony Parker"        |
      | "Tiago Splitter"     | "Manu Ginobili"      |
      | "Paul Gasol"         | "Kobe Bryant"        |
      | "Paul Gasol"         | "Marc Gasol"         |
      | "Paul Gasol"         | "Paul Gasol"         |
      | "Aron Baynes"        | "Tim Duncan"         |
      | "Aron Baynes"        | "Tony Parker"        |
      | "Aron Baynes"        | "Manu Ginobili"      |
      | "Vince Carter"       | "Tracy McGrady"      |
      | "Vince Carter"       | "Kobe Bryant"        |
      | "Vince Carter"       | "Grant Hill"         |
      | "Vince Carter"       | "Rudy Gay"           |
      | "Vince Carter"       | "Jason Kidd"         |
      | "Vince Carter"       | "Vince Carter"       |
      | "Vince Carter"       | "Dirk Nowitzki"      |
      | "Vince Carter"       | "Steve Nash"         |
      | "Marc Gasol"         | "Paul Gasol"         |
      | "Marc Gasol"         | "Kobe Bryant"        |
      | "Marc Gasol"         | "Marc Gasol"         |
      | "Ben Simmons"        | "Joel Embiid"        |
      | "Ben Simmons"        | "Ben Simmons"        |
      | "Rajon Rondo"        | "Ray Allen"          |
      | "Rajon Rondo"        | "Rajon Rondo"        |
      | "Manu Ginobili"      | "Tim Duncan"         |
      | "Manu Ginobili"      | "Tony Parker"        |
      | "Manu Ginobili"      | "Manu Ginobili"      |
      | "Kyrie Irving"       | "LeBron James"       |
      | "Kyrie Irving"       | "Ray Allen"          |
      | "Carmelo Anthony"    | "Chris Paul"         |
      | "Carmelo Anthony"    | "LeBron James"       |
      | "Carmelo Anthony"    | "Carmelo Anthony"    |
      | "Carmelo Anthony"    | "Dwyane Wade"        |
      | "Carmelo Anthony"    | "Dwyane Wade"        |
      | "Carmelo Anthony"    | "LeBron James"       |
      | "Carmelo Anthony"    | "Chris Paul"         |
      | "Carmelo Anthony"    | "Carmelo Anthony"    |
      | "Carmelo Anthony"    | "LeBron James"       |
      | "Carmelo Anthony"    | "Ray Allen"          |
      | "Dwyane Wade"        | "LeBron James"       |
      | "Dwyane Wade"        | "Ray Allen"          |
      | "Dwyane Wade"        | "Chris Paul"         |
      | "Dwyane Wade"        | "LeBron James"       |
      | "Dwyane Wade"        | "Carmelo Anthony"    |
      | "Dwyane Wade"        | "Dwyane Wade"        |
      | "Dwyane Wade"        | "Carmelo Anthony"    |
      | "Dwyane Wade"        | "Chris Paul"         |
      | "Dwyane Wade"        | "Dwyane Wade"        |
      | "Dwyane Wade"        | "LeBron James"       |
      | "Joel Embiid"        | "Ben Simmons"        |
      | "Joel Embiid"        | "Joel Embiid"        |
      | "Damian Lillard"     | "LaMarcus Aldridge"  |
      | "Damian Lillard"     | "Tony Parker"        |
      | "Damian Lillard"     | "Tim Duncan"         |
      | "Yao Ming"           | "Shaquile O'Neal"    |
      | "Yao Ming"           | "JaVale McGee"       |
      | "Yao Ming"           | "Tim Duncan"         |
      | "Yao Ming"           | "Tracy McGrady"      |
      | "Yao Ming"           | "Kobe Bryant"        |
      | "Yao Ming"           | "Grant Hill"         |
      | "Yao Ming"           | "Rudy Gay"           |
      | "Dejounte Murray"    | "Kevin Durant"       |
      | "Dejounte Murray"    | "Tony Parker"        |
      | "Dejounte Murray"    | "Tim Duncan"         |
      | "Dejounte Murray"    | "Manu Ginobili"      |
      | "Dejounte Murray"    | "LaMarcus Aldridge"  |
      | "Dejounte Murray"    | "Manu Ginobili"      |
      | "Dejounte Murray"    | "Tim Duncan"         |
      | "Dejounte Murray"    | "Tim Duncan"         |
      | "Dejounte Murray"    | "Tony Parker"        |
      | "Dejounte Murray"    | "Manu Ginobili"      |
      | "Dejounte Murray"    | "Kyle Anderson"      |
      | "Dejounte Murray"    | "Danny Green"        |
      | "Dejounte Murray"    | "Marco Belinelli"    |
      | "Dejounte Murray"    | "Tim Duncan"         |
      | "Dejounte Murray"    | "LeBron James"       |
      | "Dejounte Murray"    | "Marco Belinelli"    |
      | "Dejounte Murray"    | "Tony Parker"        |
      | "Dejounte Murray"    | "Tim Duncan"         |
      | "Dejounte Murray"    | "Danny Green"        |
      | "Dejounte Murray"    | "Russell Westbrook"  |
      | "Dejounte Murray"    | "Paul George"        |
      | "Dejounte Murray"    | "James Harden"       |
      | "Dejounte Murray"    | "James Harden"       |
      | "Dejounte Murray"    | "Russell Westbrook"  |
      | "Dejounte Murray"    | "LeBron James"       |
      | "Dejounte Murray"    | "Ray Allen"          |
      | "Dejounte Murray"    | "Chris Paul"         |
      | "Dejounte Murray"    | "LeBron James"       |
      | "Dejounte Murray"    | "Carmelo Anthony"    |
      | "Dejounte Murray"    | "Dwyane Wade"        |
      | "Blake Griffin"      | "Chris Paul"         |
      | "Blake Griffin"      | "LeBron James"       |
      | "Blake Griffin"      | "Carmelo Anthony"    |
      | "Blake Griffin"      | "Dwyane Wade"        |
      | "Steve Nash"         | "Dirk Nowitzki"      |
      | "Steve Nash"         | "Steve Nash"         |
      | "Steve Nash"         | "Dwyane Wade"        |
      | "Steve Nash"         | "Jason Kidd"         |
      | "Steve Nash"         | "Jason Kidd"         |
      | "Steve Nash"         | "Vince Carter"       |
      | "Steve Nash"         | "Dirk Nowitzki"      |
      | "Steve Nash"         | "Steve Nash"         |
      | "Steve Nash"         | "Amar'e Stoudemire"  |
      | "Steve Nash"         | "Steve Nash"         |
      | "Steve Nash"         | "Stephen Curry"      |
      | "Jason Kidd"         | "Vince Carter"       |
      | "Jason Kidd"         | "Tracy McGrady"      |
      | "Jason Kidd"         | "Jason Kidd"         |
      | "Jason Kidd"         | "Dirk Nowitzki"      |
      | "Jason Kidd"         | "Steve Nash"         |
      | "Jason Kidd"         | "Dwyane Wade"        |
      | "Jason Kidd"         | "Jason Kidd"         |
      | "Jason Kidd"         | "Steve Nash"         |
      | "Jason Kidd"         | "Dirk Nowitzki"      |
      | "Jason Kidd"         | "Jason Kidd"         |
      | "Jason Kidd"         | "Amar'e Stoudemire"  |
      | "Jason Kidd"         | "Stephen Curry"      |
      | "Dirk Nowitzki"      | "Steve Nash"         |
      | "Dirk Nowitzki"      | "Dirk Nowitzki"      |
      | "Dirk Nowitzki"      | "Jason Kidd"         |
      | "Dirk Nowitzki"      | "Amar'e Stoudemire"  |
      | "Dirk Nowitzki"      | "Stephen Curry"      |
      | "Dirk Nowitzki"      | "Dwyane Wade"        |
      | "Dirk Nowitzki"      | "LeBron James"       |
      | "Dirk Nowitzki"      | "Chris Paul"         |
      | "Dirk Nowitzki"      | "Carmelo Anthony"    |
      | "Dirk Nowitzki"      | "Jason Kidd"         |
      | "Dirk Nowitzki"      | "Vince Carter"       |
      | "Dirk Nowitzki"      | "Dirk Nowitzki"      |
      | "Dirk Nowitzki"      | "Steve Nash"         |
      | "Paul George"        | "Russell Westbrook"  |
      | "Paul George"        | "Paul George"        |
      | "Paul George"        | "James Harden"       |
      | "Grant Hill"         | "Tracy McGrady"      |
      | "Grant Hill"         | "Kobe Bryant"        |
      | "Grant Hill"         | "Grant Hill"         |
      | "Grant Hill"         | "Rudy Gay"           |
      | "Shaquile O'Neal"    | "JaVale McGee"       |
      | "Shaquile O'Neal"    | "Tim Duncan"         |
      | "Shaquile O'Neal"    | "Tony Parker"        |
      | "Shaquile O'Neal"    | "Manu Ginobili"      |
    When executing query:
      """
      match (p1)-[:serve*2]->(p2) return p1.name, p2.name
      """
    Then the result should be, in any order:
      | p1.name | p2.name |

  Scenario Outline: Seek by edge with properties
    When executing query:
      """
      match (player)-[:serve {start_year : 2001}]->(team) return player.name AS player, team.name AS team
      """
    Then the result should be, in any order:
      | player       | team        |
      | "Paul Gasol" | "Grizzlies" |
      | "Jason Kidd" | "Nets"      |
    When executing query:
      """
      match (team)<-[:serve {start_year : 2001}]-(player) return player.name AS player, team.name AS team
      """
    Then the result should be, in any order:
      | player       | team        |
      | "Paul Gasol" | "Grizzlies" |
      | "Jason Kidd" | "Nets"      |
    When executing query:
      """
      match (player)-[:serve {start_year : 2001}]-(team) return player.name AS player, team.name AS team
      """
    Then the result should be, in any order:
      | player       | team         |
      | "Paul Gasol" | "Grizzlies"  |
      | "Jason Kidd" | "Nets"       |
      | "Grizzlies"  | "Paul Gasol" |
      | "Nets"       | "Jason Kidd" |
    When executing query:
      """
      match (player)-[s:serve]->(team) where s.start_year == 2001 return player.name AS player, team.name AS team
      """
    Then the result should be, in any order:
      | player       | team        |
      | "Paul Gasol" | "Grizzlies" |
      | "Jason Kidd" | "Nets"      |
    When executing query:
      """
      match (team)<-[s:serve]-(player) where s.start_year == 2001 return player.name AS player, team.name AS team
      """
    Then the result should be, in any order:
      | player       | team        |
      | "Paul Gasol" | "Grizzlies" |
      | "Jason Kidd" | "Nets"      |
    When executing query:
      """
      match (player)-[s:serve]-(team) where s.start_year == 2001 return player.name AS player, team.name AS team
      """
    Then the result should be, in any order:
      | player       | team         |
      | "Paul Gasol" | "Grizzlies"  |
      | "Jason Kidd" | "Nets"       |
      | "Grizzlies"  | "Paul Gasol" |
      | "Nets"       | "Jason Kidd" |

  Scenario Outline: Seek by edge with range with properties
    When executing query:
      """
      match (p1)-[:like*2 {likeness: 90}]->(p2) return p1.name, p2.name
      """
    Then the result should be, in any order:
      | p1.name              | p2.name              |
      | "Amar'e Stoudemire"  | "Amar'e Stoudemire"  |
      | "Amar'e Stoudemire"  | "Stephen Curry"      |
      | "Tracy McGrady"      | "Tracy McGrady"      |
      | "Chris Paul"         | "Chris Paul"         |
      | "Chris Paul"         | "Dwyane Wade"        |
      | "Chris Paul"         | "LeBron James"       |
      | "Chris Paul"         | "LeBron James"       |
      | "Chris Paul"         | "Chris Paul"         |
      | "Chris Paul"         | "Carmelo Anthony"    |
      | "Kristaps Porzingis" | "Dirk Nowitzki"      |
      | "Kristaps Porzingis" | "Kristaps Porzingis" |
      | "Luka Doncic"        | "Luka Doncic"        |
      | "Tiago Splitter"     | "Tim Duncan"         |
      | "Vince Carter"       | "Kobe Bryant"        |
      | "Vince Carter"       | "Grant Hill"         |
      | "Vince Carter"       | "Rudy Gay"           |
      | "Carmelo Anthony"    | "LeBron James"       |
      | "Carmelo Anthony"    | "Carmelo Anthony"    |
      | "Carmelo Anthony"    | "Dwyane Wade"        |
      | "Carmelo Anthony"    | "LeBron James"       |
      | "Carmelo Anthony"    | "Chris Paul"         |
      | "Carmelo Anthony"    | "Carmelo Anthony"    |
      | "Dwyane Wade"        | "LeBron James"       |
      | "Dwyane Wade"        | "Carmelo Anthony"    |
      | "Dwyane Wade"        | "Dwyane Wade"        |
      | "Dwyane Wade"        | "Chris Paul"         |
      | "Dwyane Wade"        | "Dwyane Wade"        |
      | "Dwyane Wade"        | "LeBron James"       |
      | "Yao Ming"           | "Kobe Bryant"        |
      | "Yao Ming"           | "Grant Hill"         |
      | "Yao Ming"           | "Rudy Gay"           |
      | "Steve Nash"         | "Steve Nash"         |
      | "Jason Kidd"         | "Amar'e Stoudemire"  |
      | "Jason Kidd"         | "Stephen Curry"      |
      | "Grant Hill"         | "Kobe Bryant"        |
      | "Grant Hill"         | "Grant Hill"         |
      | "Grant Hill"         | "Rudy Gay"           |
    When executing query:
      """
      match (p1)-[l:like*1..2 {likeness: 90}]->(p2) return p1.name, p2.name
      """
    Then the result should be, in any order:
      | p1.name              | p2.name              |
      | "Amar'e Stoudemire"  | "Steve Nash"         |
      | "Amar'e Stoudemire"  | "Amar'e Stoudemire"  |
      | "Amar'e Stoudemire"  | "Stephen Curry"      |
      | "Russell Westbrook"  | "Paul George"        |
      | "Russell Westbrook"  | "James Harden"       |
      | "Tracy McGrady"      | "Kobe Bryant"        |
      | "Tracy McGrady"      | "Grant Hill"         |
      | "Tracy McGrady"      | "Tracy McGrady"      |
      | "Tracy McGrady"      | "Rudy Gay"           |
      | "Chris Paul"         | "LeBron James"       |
      | "Chris Paul"         | "Carmelo Anthony"    |
      | "Chris Paul"         | "Chris Paul"         |
      | "Chris Paul"         | "Dwyane Wade"        |
      | "Chris Paul"         | "LeBron James"       |
      | "Chris Paul"         | "Dwyane Wade"        |
      | "Chris Paul"         | "LeBron James"       |
      | "Chris Paul"         | "Chris Paul"         |
      | "Chris Paul"         | "Carmelo Anthony"    |
      | "Klay Thompson"      | "Stephen Curry"      |
      | "Kristaps Porzingis" | "Luka Doncic"        |
      | "Kristaps Porzingis" | "Dirk Nowitzki"      |
      | "Kristaps Porzingis" | "Kristaps Porzingis" |
      | "Luka Doncic"        | "Dirk Nowitzki"      |
      | "Luka Doncic"        | "Kristaps Porzingis" |
      | "Luka Doncic"        | "Luka Doncic"        |
      | "Tony Parker"        | "LaMarcus Aldridge"  |
      | "Tiago Splitter"     | "Manu Ginobili"      |
      | "Tiago Splitter"     | "Tim Duncan"         |
      | "Paul Gasol"         | "Kobe Bryant"        |
      | "Vince Carter"       | "Tracy McGrady"      |
      | "Vince Carter"       | "Kobe Bryant"        |
      | "Vince Carter"       | "Grant Hill"         |
      | "Vince Carter"       | "Rudy Gay"           |
      | "Manu Ginobili"      | "Tim Duncan"         |
      | "Carmelo Anthony"    | "Chris Paul"         |
      | "Carmelo Anthony"    | "LeBron James"       |
      | "Carmelo Anthony"    | "Carmelo Anthony"    |
      | "Carmelo Anthony"    | "Dwyane Wade"        |
      | "Carmelo Anthony"    | "Dwyane Wade"        |
      | "Carmelo Anthony"    | "LeBron James"       |
      | "Carmelo Anthony"    | "Chris Paul"         |
      | "Carmelo Anthony"    | "Carmelo Anthony"    |
      | "Carmelo Anthony"    | "LeBron James"       |
      | "Dwyane Wade"        | "LeBron James"       |
      | "Dwyane Wade"        | "Chris Paul"         |
      | "Dwyane Wade"        | "LeBron James"       |
      | "Dwyane Wade"        | "Carmelo Anthony"    |
      | "Dwyane Wade"        | "Dwyane Wade"        |
      | "Dwyane Wade"        | "Carmelo Anthony"    |
      | "Dwyane Wade"        | "Chris Paul"         |
      | "Dwyane Wade"        | "Dwyane Wade"        |
      | "Dwyane Wade"        | "LeBron James"       |
      | "Yao Ming"           | "Shaquile O'Neal"    |
      | "Yao Ming"           | "Tracy McGrady"      |
      | "Yao Ming"           | "Kobe Bryant"        |
      | "Yao Ming"           | "Grant Hill"         |
      | "Yao Ming"           | "Rudy Gay"           |
      | "Steve Nash"         | "Amar'e Stoudemire"  |
      | "Steve Nash"         | "Steve Nash"         |
      | "Steve Nash"         | "Stephen Curry"      |
      | "Jason Kidd"         | "Steve Nash"         |
      | "Jason Kidd"         | "Amar'e Stoudemire"  |
      | "Jason Kidd"         | "Stephen Curry"      |
      | "Grant Hill"         | "Tracy McGrady"      |
      | "Grant Hill"         | "Kobe Bryant"        |
      | "Grant Hill"         | "Grant Hill"         |
      | "Grant Hill"         | "Rudy Gay"           |

  Scenario Outline: seek by edge without index
    When executing query:
      """
      MATCH (p1)-[:teammate]->(p2)
      RETURN p1.name, id(p2)
      """
    Then a SemanticError should be raised at runtime: Can't solve the start vids from the sentence: MATCH (p1)-[:teammate]->(p2) RETURN p1.name,id(p2)
