Feature: Match seek by edge
  Examples:
    | space_name  |
    | nba         |
    | nba_int_vid |

  Background: Prepare space
    Given a graph with space named "<space_name>"

  Scenario: seek by edge index
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

  Scenario: Seek by edge with properties
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

  Scenario: seek by edge without index
    When executing query:
      """
      MATCH (p1)-[:like]->(p2)
      RETURN p1.name, id(p2)
      """
    Then a SemanticError should be raised at runtime: Can't solve the start vids from the sentence: MATCH (p1)-[:like]->(p2) RETURN p1.name,id(p2)
