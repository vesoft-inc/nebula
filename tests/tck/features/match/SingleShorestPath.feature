# Copyright (c) 2022 vesoft inc. All rights reserved.
#
# This source code is licensed under Apache 2.0 License.
Feature: single shortestPath

  Background:
    Given a graph with space named "nba"

  Scenario: single shortestPath1
    When executing query:
      """
      MATCH p = shortestPath( (a:player{name:"Tim Duncan"})-[e*..5]-(b:player{name:"Tony Parker"}) )  RETURN p
      """
    Then the result should be, in any order, with relax comparison:
      | p                                                                                                                                                                                                                     |
      | <("Tim Duncan" :bachelor{name: "Tim Duncan", speciality: "psychology"} :player{age: 42, name: "Tim Duncan"})<-[:teammate@0 {end_year: 2016, start_year: 2001}]-("Tony Parker" :player{age: 36, name: "Tony Parker"})> |
    When executing query:
      """
      MATCH p = shortestPath( (a:player{name:"Tim Duncan"})-[e*..5]-(b:player{name:"Tony Parker"}) )  RETURN a, e, b
      """
    Then the result should be, in any order, with relax comparison:
      | a                                                                                                           | e                                                                               | b                                                     |
      | ("Tim Duncan" :bachelor{name: "Tim Duncan", speciality: "psychology"} :player{age: 42, name: "Tim Duncan"}) | [[:teammate "Tony Parker"->"Tim Duncan" @0 {end_year: 2016, start_year: 2001}]] | ("Tony Parker" :player{age: 36, name: "Tony Parker"}) |
    When executing query:
      """
      MATCH p = shortestPath( (a:player{name:"Tim Duncan"})-[e*..5]-(b:player{name:"Tony Parker"}) )  RETURN e, p
      """
    Then the result should be, in any order, with relax comparison:
      | e                                                                               | p                                                                                                                                                                                                                     |
      | [[:teammate "Tony Parker"->"Tim Duncan" @0 {end_year: 2016, start_year: 2001}]] | <("Tim Duncan" :bachelor{name: "Tim Duncan", speciality: "psychology"} :player{age: 42, name: "Tim Duncan"})<-[:teammate@0 {end_year: 2016, start_year: 2001}]-("Tony Parker" :player{age: 36, name: "Tony Parker"})> |
    When executing query:
      """
      MATCH p = shortestPath( (a:player{name:"Tiago Splitter"})-[e:like*..5]->(b:player{name:"LaMarcus Aldridge"}) ) RETURN  p
      """
    Then the result should be, in any order, with relax comparison:
      | p                                                                                                                                                                                                                                                                                                                                                                               |
      | <("Tiago Splitter" :player{age: 34, name: "Tiago Splitter"})-[:like@0 {likeness: 80}]->("Tim Duncan" :bachelor{name: "Tim Duncan", speciality: "psychology"} :player{age: 42, name: "Tim Duncan"})-[:like@0 {likeness: 95}]->("Tony Parker" :player{age: 36, name: "Tony Parker"})-[:like@0 {likeness: 90}]->("LaMarcus Aldridge" :player{age: 33, name: "LaMarcus Aldridge"})> |
    When executing query:
      """
      MATCH p = shortestPath( (a:player{name:"Tiago Splitter"})-[e*..5]->(b:player{name:"LaMarcus Aldridge"}) ) RETURN  p
      """
    Then the result should be, in any order, with relax comparison:
      | p                                                                                                                                                                                                                                                                                                                       |
      | <("Tiago Splitter" :player{age: 34, name: "Tiago Splitter"})-[:like@0 {likeness: 80}]->("Tim Duncan" :bachelor{name: "Tim Duncan", speciality: "psychology"} :player{age: 42, name: "Tim Duncan"})-[:teammate@0 {end_year: 2016, start_year: 2015}]->("LaMarcus Aldridge" :player{age: 33, name: "LaMarcus Aldridge"})> |
    When executing query:
      """
      MATCH p = shortestPath( (a:player{name:"Tiago Splitter"})-[e*..5]->(b:player{name:"LaMarcus Aldridge"}) )
        WHERE length(p) > 2
        RETURN  p
      """
    Then the result should be, in any order, with relax comparison:
      | p |
    When executing query:
      """
      MATCH p = shortestPath( (a:player{name:"Tiago Splitter"})-[e*..1]->(b:player{name:"Tim Duncan"}) ) RETURN  nodes(p), relationships(p)
      """
    Then the result should be, in any order, with relax comparison:
      | nodes(p)                                                                                                                                                                   | relationships(p)                                           |
      | [("Tiago Splitter" :player{age: 34, name: "Tiago Splitter"}), ("Tim Duncan" :bachelor{name: "Tim Duncan", speciality: "psychology"} :player{age: 42, name: "Tim Duncan"})] | [[:like "Tiago Splitter"->"Tim Duncan" @0 {likeness: 80}]] |
    When executing query:
      """
      MATCH p = shortestPath( (a:player{age:30})-[e*..5]->(b:player{name:"LeBron James"}) )  RETURN p
      """
    Then the result should be, in any order, with relax comparison:
      | p                                                                                                                                                                                                                           |
      | <("Blake Griffin" :player{age: 30, name: "Blake Griffin"})-[:like@0 {likeness: -1}]->("Chris Paul" :player{age: 33, name: "Chris Paul"})-[:like@0 {likeness: 90}]->("LeBron James" :player{age: 34, name: "LeBron James"})> |
    When executing query:
      """
      MATCH p = shortestPath( (a:player{age:30})-[e*..5]->(b:team) ) WHERE id(a) != 'Blake Griffin' RETURN p
      """
    Then the result should be, in any order, with relax comparison:
      | p                                                                                                                                                                                                                                       |
      | <("DeAndre Jordan" :player{age: 30, name: "DeAndre Jordan"})-[:serve@0 {end_year: 2019, start_year: 2019}]->("Knicks" :team{name: "Knicks"})>                                                                                           |
      | <("DeAndre Jordan" :player{age: 30, name: "DeAndre Jordan"})-[:serve@0 {end_year: 2019, start_year: 2018}]->("Mavericks" :team{name: "Mavericks"})>                                                                                     |
      | <("DeAndre Jordan" :player{age: 30, name: "DeAndre Jordan"})-[:serve@0 {end_year: 2018, start_year: 2008}]->("Clippers" :team{name: "Clippers"})>                                                                                       |
      | <("Kevin Durant" :player{age: 30, name: "Kevin Durant"})-[:serve@0 {end_year: 2016, start_year: 2007}]->("Thunders" :team{name: "Thunders"})>                                                                                           |
      | <("Kevin Durant" :player{age: 30, name: "Kevin Durant"})-[:serve@0 {end_year: 2019, start_year: 2016}]->("Warriors" :team{name: "Warriors"})>                                                                                           |
      | <("Russell Westbrook" :player{age: 30, name: "Russell Westbrook"})-[:like@0 {likeness: 90}]->("James Harden" :player{age: 29, name: "James Harden"})-[:serve@0 {end_year: 2019, start_year: 2012}]->("Rockets" :team{name: "Rockets"})> |
      | <("Russell Westbrook" :player{age: 30, name: "Russell Westbrook"})-[:serve@0 {end_year: 2019, start_year: 2008}]->("Thunders" :team{name: "Thunders"})>                                                                                 |
      | <("Russell Westbrook" :player{age: 30, name: "Russell Westbrook"})-[:like@0 {likeness: 90}]->("Paul George" :player{age: 28, name: "Paul George"})-[:serve@0 {end_year: 2017, start_year: 2010}]->("Pacers" :team{name: "Pacers"})>     |
    When executing query:
      """
      MATCH p = shortestPath( (a:player{age:30})-[e*..5]->(b:team) )
        WHERE length(p) == 1
        RETURN p
      """
    Then the result should be, in any order, with relax comparison:
      | p                                                                                                                                                       |
      | <("DeAndre Jordan" :player{age: 30, name: "DeAndre Jordan"})-[:serve@0 {end_year: 2019, start_year: 2019}]->("Knicks" :team{name: "Knicks"})>           |
      | <("DeAndre Jordan" :player{age: 30, name: "DeAndre Jordan"})-[:serve@0 {end_year: 2019, start_year: 2018}]->("Mavericks" :team{name: "Mavericks"})>     |
      | <("DeAndre Jordan" :player{age: 30, name: "DeAndre Jordan"})-[:serve@0 {end_year: 2018, start_year: 2008}]->("Clippers" :team{name: "Clippers"})>       |
      | <("Kevin Durant" :player{age: 30, name: "Kevin Durant"})-[:serve@0 {end_year: 2016, start_year: 2007}]->("Thunders" :team{name: "Thunders"})>           |
      | <("Kevin Durant" :player{age: 30, name: "Kevin Durant"})-[:serve@0 {end_year: 2019, start_year: 2016}]->("Warriors" :team{name: "Warriors"})>           |
      | <("Russell Westbrook" :player{age: 30, name: "Russell Westbrook"})-[:serve@0 {end_year: 2019, start_year: 2008}]->("Thunders" :team{name: "Thunders"})> |
      | <("Blake Griffin" :player{age: 30, name: "Blake Griffin"})-[:serve@0 {end_year: 2018, start_year: 2009}]->("Clippers" :team{name: "Clippers"})>         |
      | <("Blake Griffin" :player{age: 30, name: "Blake Griffin"})-[:serve@0 {end_year: 2019, start_year: 2018}]->("Pistons" :team{name: "Pistons"})>           |
    When executing query:
      """
      MATCH p = shortestPath( (a:player{name:"Yao Ming"})-[e:serve*1..3]-(b:team) ) RETURN p
      """
    Then the result should be, in any order, with relax comparison:
      | p                                                                                                                                                                                                                                                                                                                                  |
      | <("Yao Ming" :player{age: 38, name: "Yao Ming"})-[:serve@0 {end_year: 2011, start_year: 2002}]->("Rockets" :team{name: "Rockets"})<-[:serve@0 {end_year: 2016, start_year: 2013}]-("Dwight Howard" :player{age: 33, name: "Dwight Howard"})-[:serve@0 {end_year: 2013, start_year: 2012}]->("Lakers" :team{name: "Lakers"})>       |
      | <("Yao Ming" :player{age: 38, name: "Yao Ming"})-[:serve@0 {end_year: 2011, start_year: 2002}]->("Rockets" :team{name: "Rockets"})<-[:serve@0 {end_year: 2019, start_year: 2018}]-("Carmelo Anthony" :player{age: 34, name: "Carmelo Anthony"})-[:serve@0 {end_year: 2017, start_year: 2011}]->("Knicks" :team{name: "Knicks"})>   |
      | <("Yao Ming" :player{age: 38, name: "Yao Ming"})-[:serve@0 {end_year: 2011, start_year: 2002}]->("Rockets" :team{name: "Rockets"})<-[:serve@0 {end_year: 2016, start_year: 2013}]-("Dwight Howard" :player{age: 33, name: "Dwight Howard"})-[:serve@0 {end_year: 2018, start_year: 2017}]->("Hornets" :team{name: "Hornets"})>     |
      | <("Yao Ming" :player{age: 38, name: "Yao Ming"})-[:serve@0 {end_year: 2011, start_year: 2002}]->("Rockets" :team{name: "Rockets"})<-[:serve@0 {end_year: 2021, start_year: 2017}]-("Chris Paul" :player{age: 33, name: "Chris Paul"})-[:serve@0 {end_year: 2017, start_year: 2011}]->("Clippers" :team{name: "Clippers"})>         |
      | <("Yao Ming" :player{age: 38, name: "Yao Ming"})-[:serve@0 {end_year: 2011, start_year: 2002}]->("Rockets" :team{name: "Rockets"})<-[:serve@0 {end_year: 2019, start_year: 2018}]-("Carmelo Anthony" :player{age: 34, name: "Carmelo Anthony"})-[:serve@0 {end_year: 2011, start_year: 2003}]->("Nuggets" :team{name: "Nuggets"})> |
      | <("Yao Ming" :player{age: 38, name: "Yao Ming"})-[:serve@0 {end_year: 2011, start_year: 2002}]->("Rockets" :team{name: "Rockets"})>                                                                                                                                                                                                |
      | <("Yao Ming" :player{age: 38, name: "Yao Ming"})-[:serve@0 {end_year: 2011, start_year: 2002}]->("Rockets" :team{name: "Rockets"})<-[:serve@0 {end_year: 2010, start_year: 2004}]-("Tracy McGrady" :player{age: 39, name: "Tracy McGrady"})-[:serve@0 {end_year: 2013, start_year: 2013}]->("Spurs" :team{name: "Spurs"})>         |
      | <("Yao Ming" :player{age: 38, name: "Yao Ming"})-[:serve@0 {end_year: 2011, start_year: 2002}]->("Rockets" :team{name: "Rockets"})<-[:serve@0 {end_year: 2019, start_year: 2012}]-("James Harden" :player{age: 29, name: "James Harden"})-[:serve@0 {end_year: 2012, start_year: 2009}]->("Thunders" :team{name: "Thunders"})>     |
      | <("Yao Ming" :player{age: 38, name: "Yao Ming"})-[:serve@0 {end_year: 2011, start_year: 2002}]->("Rockets" :team{name: "Rockets"})<-[:serve@0 {end_year: 2010, start_year: 2004}]-("Tracy McGrady" :player{age: 39, name: "Tracy McGrady"})-[:serve@0 {end_year: 2004, start_year: 2000}]->("Magic" :team{name: "Magic"})>         |
      | <("Yao Ming" :player{age: 38, name: "Yao Ming"})-[:serve@0 {end_year: 2011, start_year: 2002}]->("Rockets" :team{name: "Rockets"})<-[:serve@0 {end_year: 2016, start_year: 2013}]-("Dwight Howard" :player{age: 33, name: "Dwight Howard"})-[:serve@0 {end_year: 2017, start_year: 2016}]->("Hawks" :team{name: "Hawks"})>         |
      | <("Yao Ming" :player{age: 38, name: "Yao Ming"})-[:serve@0 {end_year: 2011, start_year: 2002}]->("Rockets" :team{name: "Rockets"})<-[:serve@0 {end_year: 2010, start_year: 2004}]-("Tracy McGrady" :player{age: 39, name: "Tracy McGrady"})-[:serve@0 {end_year: 2000, start_year: 1997}]->("Raptors" :team{name: "Raptors"})>     |
      | <("Yao Ming" :player{age: 38, name: "Yao Ming"})-[:serve@0 {end_year: 2011, start_year: 2002}]->("Rockets" :team{name: "Rockets"})<-[:serve@0 {end_year: 2016, start_year: 2013}]-("Dwight Howard" :player{age: 33, name: "Dwight Howard"})-[:serve@0 {end_year: 2019, start_year: 2018}]->("Wizards" :team{name: "Wizards"})>     |

  Scenario: single shortestPaths2
    When executing query:
      """
      MATCH p = shortestPath( (a)-[e*..5]-(b) )
        WHERE id(a) == 'Tim Duncan' and id(b) in ['Spurs', 'Tony Parker', 'Yao Ming']
        RETURN p
      """
    Then the result should be, in any order, with relax comparison:
      | p                                                                                                                                                                                                                                                                                 |
      | <("Tim Duncan" :bachelor{name: "Tim Duncan", speciality: "psychology"} :player{age: 42, name: "Tim Duncan"})-[:serve@0 {end_year: 2016, start_year: 1997}]->("Spurs" :team{name: "Spurs"})>                                                                                       |
      | <("Tim Duncan" :bachelor{name: "Tim Duncan", speciality: "psychology"} :player{age: 42, name: "Tim Duncan"})<-[:teammate@0 {end_year: 2016, start_year: 2001}]-("Tony Parker" :player{age: 36, name: "Tony Parker"})>                                                             |
      | <("Tim Duncan" :bachelor{name: "Tim Duncan", speciality: "psychology"} :player{age: 42, name: "Tim Duncan"})<-[:like@0 {likeness: 80}]-("Shaquille O'Neal" :player{age: 47, name: "Shaquille O'Neal"})<-[:like@0 {likeness: 90}]-("Yao Ming" :player{age: 38, name: "Yao Ming"})> |
    When executing query:
      """
      MATCH p = shortestPath( (a)-[e*..5]->(b) )
        WHERE id(a) == 'Tim Duncan' and id(b) IN ['Spurs', 'Tony Parker', 'Yao Ming']
        RETURN p
      """
    Then the result should be, in any order, with relax comparison:
      | p                                                                                                                                                                                                                     |
      | <("Tim Duncan" :bachelor{name: "Tim Duncan", speciality: "psychology"} :player{age: 42, name: "Tim Duncan"})-[:serve@0 {end_year: 2016, start_year: 1997}]->("Spurs" :team{name: "Spurs"})>                           |
      | <("Tim Duncan" :bachelor{name: "Tim Duncan", speciality: "psychology"} :player{age: 42, name: "Tim Duncan"})-[:teammate@0 {end_year: 2016, start_year: 2001}]->("Tony Parker" :player{age: 36, name: "Tony Parker"})> |
    When executing query:
      """
      MATCH p = shortestPath( (a)-[e*..5]->(b) )
        WHERE id(b) IN ['Manu Ginobili', 'Spurs', 'Lakers'] and id(a) in ['Tony Parker', 'Yao Ming']
        RETURN p
      """
    Then the result should be, in any order, with relax comparison:
      | p                                                                                                                                                                                                                                                                                                                                                                                                                                                                                               |
      | <("Tony Parker" :player{age: 36, name: "Tony Parker"})-[:teammate@0 {end_year: 2018, start_year: 2002}]->("Manu Ginobili" :player{age: 41, name: "Manu Ginobili"})>                                                                                                                                                                                                                                                                                                                             |
      | <("Tony Parker" :player{age: 36, name: "Tony Parker"})-[:serve@0 {end_year: 2018, start_year: 1999}]->("Spurs" :team{name: "Spurs"})>                                                                                                                                                                                                                                                                                                                                                           |
      | <("Tony Parker" :player{age: 36, name: "Tony Parker"})-[:teammate@0 {end_year: 2016, start_year: 2001}]->("Tim Duncan" :bachelor{name: "Tim Duncan", speciality: "psychology"} :player{age: 42, name: "Tim Duncan"})-[:teammate@0 {end_year: 2016, start_year: 2010}]->("Danny Green" :player{age: 31, name: "Danny Green"})-[:like@0 {likeness: 80}]->("LeBron James" :player{age: 34, name: "LeBron James"})-[:serve@0 {end_year: 2019, start_year: 2018}]->("Lakers" :team{name: "Lakers"})> |
      | <("Yao Ming" :player{age: 38, name: "Yao Ming"})-[:like@0 {likeness: 90}]->("Shaquille O'Neal" :player{age: 47, name: "Shaquille O'Neal"})-[:like@0 {likeness: 80}]->("Tim Duncan" :bachelor{name: "Tim Duncan", speciality: "psychology"} :player{age: 42, name: "Tim Duncan"})-[:teammate@0 {end_year: 2016, start_year: 2002}]->("Manu Ginobili" :player{age: 41, name: "Manu Ginobili"})>                                                                                                   |
      | <("Yao Ming" :player{age: 38, name: "Yao Ming"})-[:like@0 {likeness: 90}]->("Tracy McGrady" :player{age: 39, name: "Tracy McGrady"})-[:serve@0 {end_year: 2013, start_year: 2013}]->("Spurs" :team{name: "Spurs"})>                                                                                                                                                                                                                                                                             |
      | <("Yao Ming" :player{age: 38, name: "Yao Ming"})-[:like@0 {likeness: 90}]->("Shaquille O'Neal" :player{age: 47, name: "Shaquille O'Neal"})-[:serve@0 {end_year: 2004, start_year: 1996}]->("Lakers" :team{name: "Lakers"})>                                                                                                                                                                                                                                                                     |
    When executing query:
      """
      MATCH p = shortestPath( (a)-[e:like*..4]->(b) )
        WHERE id(b) IN ['Manu Ginobili', 'Spurs', 'Lakers'] and id(a) in ['Tony Parker', 'Yao Ming']
        RETURN p
      """
    Then the result should be, in any order, with relax comparison:
      | p                                                                                                                                                                                                                                                                                                                                                                     |
      | <("Tony Parker" :player{age: 36, name: "Tony Parker"})-[:like@0 {likeness: 95}]->("Manu Ginobili" :player{age: 41, name: "Manu Ginobili"})>                                                                                                                                                                                                                           |
      | <("Yao Ming" :player{age: 38, name: "Yao Ming"})-[:like@0 {likeness: 90}]->("Shaquille O'Neal" :player{age: 47, name: "Shaquille O'Neal"})-[:like@0 {likeness: 80}]->("Tim Duncan" :bachelor{name: "Tim Duncan", speciality: "psychology"} :player{age: 42, name: "Tim Duncan"})-[:like@0 {likeness: 95}]->("Manu Ginobili" :player{age: 41, name: "Manu Ginobili"})> |
    When executing query:
      """
      MATCH p = shortestPath( (a)-[e:like*..4]->(b) )
        WHERE id(b) IN ['Manu Ginobili', 'Spurs', 'Lakers'] and id(a) in ['xxx', 'zzz']
        RETURN p
      """
    Then the result should be, in any order, with relax comparison:
      | p |

  Scenario: single shortestPaths3
    When executing query:
      """
      MATCH (a:player{name:"Tim Duncan"}), (b:team{name:"Spurs"})
      MATCH p = shortestPath( (a)-[e:serve*..3]-(b) )
        RETURN p
      """
    Then the result should be, in any order, with relax comparison:
      | p                                                                                                                                                                                           |
      | <("Tim Duncan" :bachelor{name: "Tim Duncan", speciality: "psychology"} :player{age: 42, name: "Tim Duncan"})-[:serve@0 {end_year: 2016, start_year: 1997}]->("Spurs" :team{name: "Spurs"})> |
    When executing query:
      """
      MATCH (a:player{name:"Tim Duncan"}), (b:team{name:"Spurs"}), p = shortestPath( (a)-[e:serve*..3]-(b) ) RETURN p
      """
    Then the result should be, in any order, with relax comparison:
      | p                                                                                                                                                                                           |
      | <("Tim Duncan" :bachelor{name: "Tim Duncan", speciality: "psychology"} :player{age: 42, name: "Tim Duncan"})-[:serve@0 {end_year: 2016, start_year: 1997}]->("Spurs" :team{name: "Spurs"})> |
    When executing query:
      """
      MATCH (a:player{name:"Tim Duncan"}), (b:team)
      MATCH p = shortestPath( (a)-[e:serve*..3]-(b) )
      RETURN b, length(p)
      """
    Then the result should be, in any order, with relax comparison:
      | b                                              | length(p) |
      | ("76ers" :team{name: "76ers"})                 | 3         |
      | ("Grizzlies" :team{name: "Grizzlies"})         | 3         |
      | ("Spurs" :team{name: "Spurs"})                 | 1         |
      | ("Bulls" :team{name: "Bulls"})                 | 3         |
      | ("Cavaliers" :team{name: "Cavaliers"})         | 3         |
      | ("Celtics" :team{name: "Celtics"})             | 3         |
      | ("Pacers" :team{name: "Pacers"})               | 3         |
      | ("Hawks" :team{name: "Hawks"})                 | 3         |
      | ("Lakers" :team{name: "Lakers"})               | 3         |
      | ("Raptors" :team{name: "Raptors"})             | 3         |
      | ("Warriors" :team{name: "Warriors"})           | 3         |
      | ("Hornets" :team{name: "Hornets"})             | 3         |
      | ("Kings" :team{name: "Kings"})                 | 3         |
      | ("Pistons" :team{name: "Pistons"})             | 3         |
      | ("Rockets" :team{name: "Rockets"})             | 3         |
      | ("Suns" :team{name: "Suns"})                   | 3         |
      | ("Trail Blazers" :team{name: "Trail Blazers"}) | 3         |
      | ("Bucks" :team{name: "Bucks"})                 | 3         |
      | ("Jazz" :team{name: "Jazz"})                   | 3         |
      | ("Magic" :team{name: "Magic"})                 | 3         |
    When executing query:
      """
      MATCH (b:team), (a:player{age:30})
      MATCH p = shortestPath( (a)-[e*..5]->(b) )
        WHERE length(p) == 1
        RETURN p
      """
    Then the result should be, in any order, with relax comparison:
      | p                                                                                                                                                       |
      | <("DeAndre Jordan" :player{age: 30, name: "DeAndre Jordan"})-[:serve@0 {end_year: 2019, start_year: 2019}]->("Knicks" :team{name: "Knicks"})>           |
      | <("DeAndre Jordan" :player{age: 30, name: "DeAndre Jordan"})-[:serve@0 {end_year: 2019, start_year: 2018}]->("Mavericks" :team{name: "Mavericks"})>     |
      | <("DeAndre Jordan" :player{age: 30, name: "DeAndre Jordan"})-[:serve@0 {end_year: 2018, start_year: 2008}]->("Clippers" :team{name: "Clippers"})>       |
      | <("Kevin Durant" :player{age: 30, name: "Kevin Durant"})-[:serve@0 {end_year: 2016, start_year: 2007}]->("Thunders" :team{name: "Thunders"})>           |
      | <("Kevin Durant" :player{age: 30, name: "Kevin Durant"})-[:serve@0 {end_year: 2019, start_year: 2016}]->("Warriors" :team{name: "Warriors"})>           |
      | <("Russell Westbrook" :player{age: 30, name: "Russell Westbrook"})-[:serve@0 {end_year: 2019, start_year: 2008}]->("Thunders" :team{name: "Thunders"})> |
      | <("Blake Griffin" :player{age: 30, name: "Blake Griffin"})-[:serve@0 {end_year: 2018, start_year: 2009}]->("Clippers" :team{name: "Clippers"})>         |
      | <("Blake Griffin" :player{age: 30, name: "Blake Griffin"})-[:serve@0 {end_year: 2019, start_year: 2018}]->("Pistons" :team{name: "Pistons"})>           |
    When executing query:
      """
      MATCH (b:team)
      MATCH p = shortestPath( (a:player)-[e*..5]->(b) )
        WHERE a.player.age == 30 AND length(p) == 1
        RETURN p
      """
    Then the result should be, in any order, with relax comparison:
      | p                                                                                                                                                       |
      | <("DeAndre Jordan" :player{age: 30, name: "DeAndre Jordan"})-[:serve@0 {end_year: 2019, start_year: 2019}]->("Knicks" :team{name: "Knicks"})>           |
      | <("DeAndre Jordan" :player{age: 30, name: "DeAndre Jordan"})-[:serve@0 {end_year: 2019, start_year: 2018}]->("Mavericks" :team{name: "Mavericks"})>     |
      | <("DeAndre Jordan" :player{age: 30, name: "DeAndre Jordan"})-[:serve@0 {end_year: 2018, start_year: 2008}]->("Clippers" :team{name: "Clippers"})>       |
      | <("Kevin Durant" :player{age: 30, name: "Kevin Durant"})-[:serve@0 {end_year: 2016, start_year: 2007}]->("Thunders" :team{name: "Thunders"})>           |
      | <("Kevin Durant" :player{age: 30, name: "Kevin Durant"})-[:serve@0 {end_year: 2019, start_year: 2016}]->("Warriors" :team{name: "Warriors"})>           |
      | <("Russell Westbrook" :player{age: 30, name: "Russell Westbrook"})-[:serve@0 {end_year: 2019, start_year: 2008}]->("Thunders" :team{name: "Thunders"})> |
      | <("Blake Griffin" :player{age: 30, name: "Blake Griffin"})-[:serve@0 {end_year: 2018, start_year: 2009}]->("Clippers" :team{name: "Clippers"})>         |
      | <("Blake Griffin" :player{age: 30, name: "Blake Griffin"})-[:serve@0 {end_year: 2019, start_year: 2018}]->("Pistons" :team{name: "Pistons"})>           |
    When executing query:
      """
      MATCH (b:team)
      MATCH p = shortestPath( (a:player)-[e*..5]->(b) )
        WHERE id(a) IN ['DeAndre Jordan', 'Kevin Durant', 'Russell Westbrook', 'Blake Griffin'] AND length(p) == 1
        RETURN p
      """
    Then the result should be, in any order, with relax comparison:
      | p                                                                                                                                                       |
      | <("DeAndre Jordan" :player{age: 30, name: "DeAndre Jordan"})-[:serve@0 {end_year: 2019, start_year: 2019}]->("Knicks" :team{name: "Knicks"})>           |
      | <("DeAndre Jordan" :player{age: 30, name: "DeAndre Jordan"})-[:serve@0 {end_year: 2019, start_year: 2018}]->("Mavericks" :team{name: "Mavericks"})>     |
      | <("DeAndre Jordan" :player{age: 30, name: "DeAndre Jordan"})-[:serve@0 {end_year: 2018, start_year: 2008}]->("Clippers" :team{name: "Clippers"})>       |
      | <("Kevin Durant" :player{age: 30, name: "Kevin Durant"})-[:serve@0 {end_year: 2016, start_year: 2007}]->("Thunders" :team{name: "Thunders"})>           |
      | <("Kevin Durant" :player{age: 30, name: "Kevin Durant"})-[:serve@0 {end_year: 2019, start_year: 2016}]->("Warriors" :team{name: "Warriors"})>           |
      | <("Russell Westbrook" :player{age: 30, name: "Russell Westbrook"})-[:serve@0 {end_year: 2019, start_year: 2008}]->("Thunders" :team{name: "Thunders"})> |
      | <("Blake Griffin" :player{age: 30, name: "Blake Griffin"})-[:serve@0 {end_year: 2018, start_year: 2009}]->("Clippers" :team{name: "Clippers"})>         |
      | <("Blake Griffin" :player{age: 30, name: "Blake Griffin"})-[:serve@0 {end_year: 2019, start_year: 2018}]->("Pistons" :team{name: "Pistons"})>           |

  Scenario: single shortestPaths4
    When executing query:
      """
      MATCH p = shortestPath( (a:player)-[e:serve*..3]-(b:team) )
        WHERE a.player.name == 'Tim Duncan' AND b.team.name == 'Spurs'
        RETURN p
      """
    Then the result should be, in any order, with relax comparison:
      | p                                                                                                                                                                                           |
      | <("Tim Duncan" :bachelor{name: "Tim Duncan", speciality: "psychology"} :player{age: 42, name: "Tim Duncan"})-[:serve@0 {end_year: 2016, start_year: 1997}]->("Spurs" :team{name: "Spurs"})> |
    When executing query:
      """
      MATCH p = shortestPath( (a:player)-[e:serve*..3]-(b:team) )
        WHERE a.player.age > 45 AND b.team.name == 'Spurs'
        RETURN length(p)
      """
    Then the result should be, in any order, with relax comparison:
      | length(p) |
      | 3         |
      | 3         |
    When executing query:
      """
      MATCH p = shortestPath( (a:player)-[e:like*..3]-(b:player) )
        WHERE a.player.age >  45 AND b.player.age < 30
        RETURN p
      """
    Then the result should be, in any order, with relax comparison:
      | p                                                                                                                                                                                                                                                                                                                                                                                         |
      | <("Shaquille O'Neal" :player{age: 47, name: "Shaquille O'Neal"})-[:like@0 {likeness: 80}]->("Tim Duncan" :bachelor{name: "Tim Duncan", speciality: "psychology"} :player{age: 42, name: "Tim Duncan"})<-[:like@0 {likeness: 99}]-("Dejounte Murray" :player{age: 29, name: "Dejounte Murray"})-[:like@0 {likeness: 99}]->("James Harden" :player{age: 29, name: "James Harden"})>         |
      | <("Shaquille O'Neal" :player{age: 47, name: "Shaquille O'Neal"})-[:like@0 {likeness: 80}]->("Tim Duncan" :bachelor{name: "Tim Duncan", speciality: "psychology"} :player{age: 42, name: "Tim Duncan"})<-[:like@0 {likeness: 99}]-("Dejounte Murray" :player{age: 29, name: "Dejounte Murray"})-[:like@0 {likeness: 99}]->("Kyle Anderson" :player{age: 25, name: "Kyle Anderson"})>       |
      | <("Shaquille O'Neal" :player{age: 47, name: "Shaquille O'Neal"})-[:like@0 {likeness: 80}]->("Tim Duncan" :bachelor{name: "Tim Duncan", speciality: "psychology"} :player{age: 42, name: "Tim Duncan"})<-[:like@0 {likeness: 75}]-("LaMarcus Aldridge" :player{age: 33, name: "LaMarcus Aldridge"})<-[:like@0 {likeness: 80}]-("Damian Lillard" :player{age: 28, name: "Damian Lillard"})> |
      | <("Shaquille O'Neal" :player{age: 47, name: "Shaquille O'Neal"})-[:like@0 {likeness: 80}]->("Tim Duncan" :bachelor{name: "Tim Duncan", speciality: "psychology"} :player{age: 42, name: "Tim Duncan"})<-[:like@0 {likeness: 99}]-("Dejounte Murray" :player{age: 29, name: "Dejounte Murray"})>                                                                                           |
    When executing query:
      """
      MATCH (a:player)
      MATCH p = shortestPath( (a)<-[e:like*..2]-(b:player{name:"Yao Ming"}) )
        RETURN p
      """
    Then the result should be, in any order, with relax comparison:
      | p                                                                                                                                                                                                                                                                                 |
      | <("Kobe Bryant" :player{age: 40, name: "Kobe Bryant"})<-[:like@0 {likeness: 90}]-("Tracy McGrady" :player{age: 39, name: "Tracy McGrady"})<-[:like@0 {likeness: 90}]-("Yao Ming" :player{age: 38, name: "Yao Ming"})>                                                             |
      | <("Tim Duncan" :bachelor{name: "Tim Duncan", speciality: "psychology"} :player{age: 42, name: "Tim Duncan"})<-[:like@0 {likeness: 80}]-("Shaquille O'Neal" :player{age: 47, name: "Shaquille O'Neal"})<-[:like@0 {likeness: 90}]-("Yao Ming" :player{age: 38, name: "Yao Ming"})> |
      | <("Rudy Gay" :player{age: 32, name: "Rudy Gay"})<-[:like@0 {likeness: 90}]-("Tracy McGrady" :player{age: 39, name: "Tracy McGrady"})<-[:like@0 {likeness: 90}]-("Yao Ming" :player{age: 38, name: "Yao Ming"})>                                                                   |
      | <("Grant Hill" :player{age: 46, name: "Grant Hill"})<-[:like@0 {likeness: 90}]-("Tracy McGrady" :player{age: 39, name: "Tracy McGrady"})<-[:like@0 {likeness: 90}]-("Yao Ming" :player{age: 38, name: "Yao Ming"})>                                                               |
      | <("JaVale McGee" :player{age: 31, name: "JaVale McGee"})<-[:like@0 {likeness: 100}]-("Shaquille O'Neal" :player{age: 47, name: "Shaquille O'Neal"})<-[:like@0 {likeness: 90}]-("Yao Ming" :player{age: 38, name: "Yao Ming"})>                                                    |
      | <("Shaquille O'Neal" :player{age: 47, name: "Shaquille O'Neal"})<-[:like@0 {likeness: 90}]-("Yao Ming" :player{age: 38, name: "Yao Ming"})>                                                                                                                                       |
      | <("Tracy McGrady" :player{age: 39, name: "Tracy McGrady"})<-[:like@0 {likeness: 90}]-("Yao Ming" :player{age: 38, name: "Yao Ming"})>                                                                                                                                             |
    When executing query:
      """
      MATCH (a:player)
      MATCH p = shortestPath( (a)-[e:like*..2]->(b:player{name:"Tony Parker"}) )
        RETURN max(length(p)) AS maxL
      """
    Then the result should be, in any order, with relax comparison:
      | maxL |
      | 2    |
    When executing query:
      """
      MATCH (a:player)
      MATCH p = shortestPath( (a)-[e:like*..2]->(b) )
        WHERE id(b) IN ['xxx', 'zzz', 'yyy', 'Tim Duncan', 'Yao Ming']
        RETURN p
      """
    Then the result should be, in any order, with relax comparison:
      | p                                                                                                                                                                                                                                                                                               |
      | <("Rudy Gay" :player{age: 32, name: "Rudy Gay"})-[:like@0 {likeness: 70}]->("LaMarcus Aldridge" :player{age: 33, name: "LaMarcus Aldridge"})-[:like@0 {likeness: 75}]->("Tim Duncan" :bachelor{name: "Tim Duncan", speciality: "psychology"} :player{age: 42, name: "Tim Duncan"})>             |
      | <("Damian Lillard" :player{age: 28, name: "Damian Lillard"})-[:like@0 {likeness: 80}]->("LaMarcus Aldridge" :player{age: 33, name: "LaMarcus Aldridge"})-[:like@0 {likeness: 75}]->("Tim Duncan" :bachelor{name: "Tim Duncan", speciality: "psychology"} :player{age: 42, name: "Tim Duncan"})> |
      | <("Marco Belinelli" :player{age: 32, name: "Marco Belinelli"})-[:like@0 {likeness: 55}]->("Tim Duncan" :bachelor{name: "Tim Duncan", speciality: "psychology"} :player{age: 42, name: "Tim Duncan"})>                                                                                           |
      | <("Yao Ming" :player{age: 38, name: "Yao Ming"})-[:like@0 {likeness: 90}]->("Shaquille O'Neal" :player{age: 47, name: "Shaquille O'Neal"})-[:like@0 {likeness: 80}]->("Tim Duncan" :bachelor{name: "Tim Duncan", speciality: "psychology"} :player{age: 42, name: "Tim Duncan"})>               |
      | <("Aron Baynes" :player{age: 32, name: "Aron Baynes"})-[:like@0 {likeness: 80}]->("Tim Duncan" :bachelor{name: "Tim Duncan", speciality: "psychology"} :player{age: 42, name: "Tim Duncan"})>                                                                                                   |
      | <("Tiago Splitter" :player{age: 34, name: "Tiago Splitter"})-[:like@0 {likeness: 80}]->("Tim Duncan" :bachelor{name: "Tim Duncan", speciality: "psychology"} :player{age: 42, name: "Tim Duncan"})>                                                                                             |
      | <("Shaquille O'Neal" :player{age: 47, name: "Shaquille O'Neal"})-[:like@0 {likeness: 80}]->("Tim Duncan" :bachelor{name: "Tim Duncan", speciality: "psychology"} :player{age: 42, name: "Tim Duncan"})>                                                                                         |
      | <("Danny Green" :player{age: 31, name: "Danny Green"})-[:like@0 {likeness: 70}]->("Tim Duncan" :bachelor{name: "Tim Duncan", speciality: "psychology"} :player{age: 42, name: "Tim Duncan"})>                                                                                                   |
      | <("Boris Diaw" :player{age: 36, name: "Boris Diaw"})-[:like@0 {likeness: 80}]->("Tim Duncan" :bachelor{name: "Tim Duncan", speciality: "psychology"} :player{age: 42, name: "Tim Duncan"})>                                                                                                     |
      | <("Dejounte Murray" :player{age: 29, name: "Dejounte Murray"})-[:like@0 {likeness: 99}]->("Tim Duncan" :bachelor{name: "Tim Duncan", speciality: "psychology"} :player{age: 42, name: "Tim Duncan"})>                                                                                           |
      | <("Manu Ginobili" :player{age: 41, name: "Manu Ginobili"})-[:like@0 {likeness: 90}]->("Tim Duncan" :bachelor{name: "Tim Duncan", speciality: "psychology"} :player{age: 42, name: "Tim Duncan"})>                                                                                               |
      | <("Tony Parker" :player{age: 36, name: "Tony Parker"})-[:like@0 {likeness: 95}]->("Tim Duncan" :bachelor{name: "Tim Duncan", speciality: "psychology"} :player{age: 42, name: "Tim Duncan"})>                                                                                                   |
      | <("LaMarcus Aldridge" :player{age: 33, name: "LaMarcus Aldridge"})-[:like@0 {likeness: 75}]->("Tim Duncan" :bachelor{name: "Tim Duncan", speciality: "psychology"} :player{age: 42, name: "Tim Duncan"})>                                                                                       |

  Scenario: single shortestPaths5
    When executing query:
      """
      MATCH (a:player{name:"Tim Duncan"}), (b:team{name:"Lakers"})
        OPTIONAL MATCH p = shortestPath( (a)-[:like*]-(b) )
        RETURN
        CASE p is NULL
          WHEN true THEN -1
          ELSE length(p)
        END AS shortestPathLength
      """
    Then the result should be, in any order, with relax comparison:
      | shortestPathLength |
      | -1                 |

  Scenario: single shortestPaths Can Not find index
    When executing query:
      """
      MATCH p = shortestPath( (a)-[e*..5]-(b) )
        WHERE id(a) == 'Tim Duncan' OR id(b) in ['Spurs', 'Tony Parker', 'Yao Ming']
        RETURN p
      """
    Then a ExecutionError should be raised at runtime: Scan vertices or edges need to specify a limit number, or limit number can not push down.
    When executing query:
      """
      MATCH p = shortestPath( (a)-[e*..5]-(b) )
        WHERE id(a) == 'Tim Duncan'
        RETURN p
      """
    Then a ExecutionError should be raised at runtime: Scan vertices or edges need to specify a limit number, or limit number can not push down.
