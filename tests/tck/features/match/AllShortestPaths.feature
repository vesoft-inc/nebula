# Copyright (c) 2022 vesoft inc. All rights reserved.
#
# This source code is licensed under Apache 2.0 License.
Feature: allShortestPaths

  Background:
    Given a graph with space named "nba"

  Scenario: allShortestPaths1
    When executing query:
      """
      MATCH p = allShortestPaths( (a:player{name:"Tim Duncan"})-[e*..5]-(b:player{name:"Tony Parker"}) )  RETURN p
      """
    Then the result should be, in any order, with relax comparison:
      | p                                                                                                                                                                                                                     |
      | <("Tim Duncan" :bachelor{name: "Tim Duncan", speciality: "psychology"} :player{age: 42, name: "Tim Duncan"})<-[:teammate@0 {end_year: 2016, start_year: 2001}]-("Tony Parker" :player{age: 36, name: "Tony Parker"})> |
      | <("Tim Duncan" :bachelor{name: "Tim Duncan", speciality: "psychology"} :player{age: 42, name: "Tim Duncan"})-[:teammate@0 {end_year: 2016, start_year: 2001}]->("Tony Parker" :player{age: 36, name: "Tony Parker"})> |
      | <("Tim Duncan" :bachelor{name: "Tim Duncan", speciality: "psychology"} :player{age: 42, name: "Tim Duncan"})<-[:like@0 {likeness: 95}]-("Tony Parker" :player{age: 36, name: "Tony Parker"})>                         |
      | <("Tim Duncan" :bachelor{name: "Tim Duncan", speciality: "psychology"} :player{age: 42, name: "Tim Duncan"})-[:like@0 {likeness: 95}]->("Tony Parker" :player{age: 36, name: "Tony Parker"})>                         |
    When executing query:
      """
      MATCH p = allShortestPaths( (a:player{name:"Tim Duncan"})-[e*..5]-(b:player{name:"Tony Parker"}) )  RETURN a, e, b
      """
    Then the result should be, in any order, with relax comparison:
      | a                                                                                                           | e                                                                               | b                                                     |
      | ("Tim Duncan" :bachelor{name: "Tim Duncan", speciality: "psychology"} :player{age: 42, name: "Tim Duncan"}) | [[:teammate "Tony Parker"->"Tim Duncan" @0 {end_year: 2016, start_year: 2001}]] | ("Tony Parker" :player{age: 36, name: "Tony Parker"}) |
      | ("Tim Duncan" :bachelor{name: "Tim Duncan", speciality: "psychology"} :player{age: 42, name: "Tim Duncan"}) | [[:teammate "Tim Duncan"->"Tony Parker" @0 {end_year: 2016, start_year: 2001}]] | ("Tony Parker" :player{age: 36, name: "Tony Parker"}) |
      | ("Tim Duncan" :bachelor{name: "Tim Duncan", speciality: "psychology"} :player{age: 42, name: "Tim Duncan"}) | [[:like "Tony Parker"->"Tim Duncan" @0 {likeness: 95}]]                         | ("Tony Parker" :player{age: 36, name: "Tony Parker"}) |
      | ("Tim Duncan" :bachelor{name: "Tim Duncan", speciality: "psychology"} :player{age: 42, name: "Tim Duncan"}) | [[:like "Tim Duncan"->"Tony Parker" @0 {likeness: 95}]]                         | ("Tony Parker" :player{age: 36, name: "Tony Parker"}) |
    When executing query:
      """
      MATCH p = allShortestPaths( (a:player{name:"Tim Duncan"})-[e*..5]-(b:player{name:"Tony Parker"}) )  RETURN e, p
      """
    Then the result should be, in any order, with relax comparison:
      | e                                                                               | p                                                                                                                                                                                                                     |
      | [[:teammate "Tony Parker"->"Tim Duncan" @0 {end_year: 2016, start_year: 2001}]] | <("Tim Duncan" :bachelor{name: "Tim Duncan", speciality: "psychology"} :player{age: 42, name: "Tim Duncan"})<-[:teammate@0 {end_year: 2016, start_year: 2001}]-("Tony Parker" :player{age: 36, name: "Tony Parker"})> |
      | [[:teammate "Tim Duncan"->"Tony Parker" @0 {end_year: 2016, start_year: 2001}]] | <("Tim Duncan" :bachelor{name: "Tim Duncan", speciality: "psychology"} :player{age: 42, name: "Tim Duncan"})-[:teammate@0 {end_year: 2016, start_year: 2001}]->("Tony Parker" :player{age: 36, name: "Tony Parker"})> |
      | [[:like "Tony Parker"->"Tim Duncan" @0 {likeness: 95}]]                         | <("Tim Duncan" :bachelor{name: "Tim Duncan", speciality: "psychology"} :player{age: 42, name: "Tim Duncan"})<-[:like@0 {likeness: 95}]-("Tony Parker" :player{age: 36, name: "Tony Parker"})>                         |
      | [[:like "Tim Duncan"->"Tony Parker" @0 {likeness: 95}]]                         | <("Tim Duncan" :bachelor{name: "Tim Duncan", speciality: "psychology"} :player{age: 42, name: "Tim Duncan"})-[:like@0 {likeness: 95}]->("Tony Parker" :player{age: 36, name: "Tony Parker"})>                         |
    When executing query:
      """
      MATCH p = allShortestPaths( (a:player{name:"Tiago Splitter"})-[e:like*..5]->(b:player{name:"LaMarcus Aldridge"}) ) RETURN  p
      """
    Then the result should be, in any order, with relax comparison:
      | p                                                                                                                                                                                                                                                                                                                                                                               |
      | <("Tiago Splitter" :player{age: 34, name: "Tiago Splitter"})-[:like@0 {likeness: 80}]->("Tim Duncan" :bachelor{name: "Tim Duncan", speciality: "psychology"} :player{age: 42, name: "Tim Duncan"})-[:like@0 {likeness: 95}]->("Tony Parker" :player{age: 36, name: "Tony Parker"})-[:like@0 {likeness: 90}]->("LaMarcus Aldridge" :player{age: 33, name: "LaMarcus Aldridge"})> |
    When executing query:
      """
      MATCH p = allShortestPaths( (a:player{name:"Tiago Splitter"})-[e*..5]->(b:player{name:"LaMarcus Aldridge"}) ) RETURN  p
      """
    Then the result should be, in any order, with relax comparison:
      | p                                                                                                                                                                                                                                                                                                                       |
      | <("Tiago Splitter" :player{age: 34, name: "Tiago Splitter"})-[:like@0 {likeness: 80}]->("Tim Duncan" :bachelor{name: "Tim Duncan", speciality: "psychology"} :player{age: 42, name: "Tim Duncan"})-[:teammate@0 {end_year: 2016, start_year: 2015}]->("LaMarcus Aldridge" :player{age: 33, name: "LaMarcus Aldridge"})> |
    When executing query:
      """
      MATCH p = allShortestPaths( (a:player{name:"Tiago Splitter"})-[e*..5]->(b:player{name:"LaMarcus Aldridge"}) )
        WHERE length(p) > 2
        RETURN  p
      """
    Then the result should be, in any order, with relax comparison:
      | p |
    When executing query:
      """
      MATCH p = allShortestPaths( (a:player{name:"Tiago Splitter"})-[e*..1]->(b:player{name:"Tim Duncan"}) ) RETURN  nodes(p), relationships(p)
      """
    Then the result should be, in any order, with relax comparison:
      | nodes(p)                                                                                                                                                                   | relationships(p)                                           |
      | [("Tiago Splitter" :player{age: 34, name: "Tiago Splitter"}), ("Tim Duncan" :bachelor{name: "Tim Duncan", speciality: "psychology"} :player{age: 42, name: "Tim Duncan"})] | [[:like "Tiago Splitter"->"Tim Duncan" @0 {likeness: 80}]] |
    When executing query:
      """
      MATCH p = allShortestPaths( (a:player{age:30})-[e*..5]->(b:player{name:"LeBron James"}) )  RETURN p
      """
    Then the result should be, in any order, with relax comparison:
      | p                                                                                                                                                                                                                           |
      | <("Blake Griffin" :player{age: 30, name: "Blake Griffin"})-[:like@0 {likeness: -1}]->("Chris Paul" :player{age: 33, name: "Chris Paul"})-[:like@0 {likeness: 90}]->("LeBron James" :player{age: 34, name: "LeBron James"})> |
    When executing query:
      """
      MATCH p = allShortestPaths( (a:player{age:30})-[e*..5]->(b:team) ) RETURN p
      """
    Then the result should be, in any order, with relax comparison:
      | p                                                                                                                                                                                                                                                                                                                                                                                                                                                                             |
      | <("DeAndre Jordan" :player{age: 30, name: "DeAndre Jordan"})-[:serve@0 {end_year: 2019, start_year: 2019}]->("Knicks" :team{name: "Knicks"})>                                                                                                                                                                                                                                                                                                                                 |
      | <("DeAndre Jordan" :player{age: 30, name: "DeAndre Jordan"})-[:serve@0 {end_year: 2019, start_year: 2018}]->("Mavericks" :team{name: "Mavericks"})>                                                                                                                                                                                                                                                                                                                           |
      | <("DeAndre Jordan" :player{age: 30, name: "DeAndre Jordan"})-[:serve@0 {end_year: 2018, start_year: 2008}]->("Clippers" :team{name: "Clippers"})>                                                                                                                                                                                                                                                                                                                             |
      | <("Kevin Durant" :player{age: 30, name: "Kevin Durant"})-[:serve@0 {end_year: 2016, start_year: 2007}]->("Thunders" :team{name: "Thunders"})>                                                                                                                                                                                                                                                                                                                                 |
      | <("Kevin Durant" :player{age: 30, name: "Kevin Durant"})-[:serve@0 {end_year: 2019, start_year: 2016}]->("Warriors" :team{name: "Warriors"})>                                                                                                                                                                                                                                                                                                                                 |
      | <("Russell Westbrook" :player{age: 30, name: "Russell Westbrook"})-[:like@0 {likeness: 90}]->("James Harden" :player{age: 29, name: "James Harden"})-[:serve@0 {end_year: 2019, start_year: 2012}]->("Rockets" :team{name: "Rockets"})>                                                                                                                                                                                                                                       |
      | <("Russell Westbrook" :player{age: 30, name: "Russell Westbrook"})-[:serve@0 {end_year: 2019, start_year: 2008}]->("Thunders" :team{name: "Thunders"})>                                                                                                                                                                                                                                                                                                                       |
      | <("Russell Westbrook" :player{age: 30, name: "Russell Westbrook"})-[:like@0 {likeness: 90}]->("Paul George" :player{age: 28, name: "Paul George"})-[:serve@0 {end_year: 2017, start_year: 2010}]->("Pacers" :team{name: "Pacers"})>                                                                                                                                                                                                                                           |
      | <("Blake Griffin" :player{age: 30, name: "Blake Griffin"})-[:like@0 {likeness: -1}]->("Chris Paul" :player{age: 33, name: "Chris Paul"})-[:like@0 {likeness: 90}]->("LeBron James" :player{age: 34, name: "LeBron James"})-[:serve@0 {end_year: 2014, start_year: 2010}]->("Heat" :team{name: "Heat"})>                                                                                                                                                                       |
      | <("Blake Griffin" :player{age: 30, name: "Blake Griffin"})-[:like@0 {likeness: -1}]->("Chris Paul" :player{age: 33, name: "Chris Paul"})-[:like@0 {likeness: 90}]->("Dwyane Wade" :player{age: 37, name: "Dwyane Wade"})-[:serve@0 {end_year: 2016, start_year: 2003}]->("Heat" :team{name: "Heat"})>                                                                                                                                                                         |
      | <("Blake Griffin" :player{age: 30, name: "Blake Griffin"})-[:like@0 {likeness: -1}]->("Chris Paul" :player{age: 33, name: "Chris Paul"})-[:like@0 {likeness: 90}]->("Dwyane Wade" :player{age: 37, name: "Dwyane Wade"})-[:serve@1 {end_year: 2019, start_year: 2018}]->("Heat" :team{name: "Heat"})>                                                                                                                                                                         |
      | <("Blake Griffin" :player{age: 30, name: "Blake Griffin"})-[:like@0 {likeness: -1}]->("Chris Paul" :player{age: 33, name: "Chris Paul"})-[:like@0 {likeness: 90}]->("LeBron James" :player{age: 34, name: "LeBron James"})-[:like@0 {likeness: 100}]->("Ray Allen" :player{age: 43, name: "Ray Allen"})-[:like@0 {likeness: 9}]->("Rajon Rondo" :player{age: 33, name: "Rajon Rondo"})-[:serve@0 {end_year: 2016, start_year: 2015}]->("Kings" :team{name: "Kings"})>         |
      | <("Blake Griffin" :player{age: 30, name: "Blake Griffin"})-[:like@0 {likeness: -1}]->("Chris Paul" :player{age: 33, name: "Chris Paul"})-[:like@0 {likeness: 90}]->("LeBron James" :player{age: 34, name: "LeBron James"})-[:serve@0 {end_year: 2019, start_year: 2018}]->("Lakers" :team{name: "Lakers"})>                                                                                                                                                                   |
      | <("Blake Griffin" :player{age: 30, name: "Blake Griffin"})-[:like@0 {likeness: -1}]->("Chris Paul" :player{age: 33, name: "Chris Paul"})-[:like@0 {likeness: 90}]->("Carmelo Anthony" :player{age: 34, name: "Carmelo Anthony"})-[:serve@0 {end_year: 2017, start_year: 2011}]->("Knicks" :team{name: "Knicks"})>                                                                                                                                                             |
      | <("Blake Griffin" :player{age: 30, name: "Blake Griffin"})-[:like@0 {likeness: -1}]->("Chris Paul" :player{age: 33, name: "Chris Paul"})-[:serve@0 {end_year: 2011, start_year: 2005}]->("Hornets" :team{name: "Hornets"})>                                                                                                                                                                                                                                                   |
      | <("Blake Griffin" :player{age: 30, name: "Blake Griffin"})-[:like@0 {likeness: -1}]->("Chris Paul" :player{age: 33, name: "Chris Paul"})-[:like@0 {likeness: 90}]->("LeBron James" :player{age: 34, name: "LeBron James"})-[:like@0 {likeness: 100}]->("Ray Allen" :player{age: 43, name: "Ray Allen"})-[:like@0 {likeness: 9}]->("Rajon Rondo" :player{age: 33, name: "Rajon Rondo"})-[:serve@0 {end_year: 2015, start_year: 2014}]->("Mavericks" :team{name: "Mavericks"})> |
      | <("Blake Griffin" :player{age: 30, name: "Blake Griffin"})-[:serve@0 {end_year: 2018, start_year: 2009}]->("Clippers" :team{name: "Clippers"})>                                                                                                                                                                                                                                                                                                                               |
      | <("Blake Griffin" :player{age: 30, name: "Blake Griffin"})-[:like@0 {likeness: -1}]->("Chris Paul" :player{age: 33, name: "Chris Paul"})-[:like@0 {likeness: 90}]->("Dwyane Wade" :player{age: 37, name: "Dwyane Wade"})-[:serve@0 {end_year: 2017, start_year: 2016}]->("Bulls" :team{name: "Bulls"})>                                                                                                                                                                       |
      | <("Blake Griffin" :player{age: 30, name: "Blake Griffin"})-[:like@0 {likeness: -1}]->("Chris Paul" :player{age: 33, name: "Chris Paul"})-[:like@0 {likeness: 90}]->("LeBron James" :player{age: 34, name: "LeBron James"})-[:like@0 {likeness: 100}]->("Ray Allen" :player{age: 43, name: "Ray Allen"})-[:serve@0 {end_year: 2003, start_year: 1996}]->("Bucks" :team{name: "Bucks"})>                                                                                        |
      | <("Blake Griffin" :player{age: 30, name: "Blake Griffin"})-[:like@0 {likeness: -1}]->("Chris Paul" :player{age: 33, name: "Chris Paul"})-[:like@0 {likeness: 90}]->("Carmelo Anthony" :player{age: 34, name: "Carmelo Anthony"})-[:serve@0 {end_year: 2011, start_year: 2003}]->("Nuggets" :team{name: "Nuggets"})>                                                                                                                                                           |
      | <("Blake Griffin" :player{age: 30, name: "Blake Griffin"})-[:like@0 {likeness: -1}]->("Chris Paul" :player{age: 33, name: "Chris Paul"})-[:like@0 {likeness: 90}]->("LeBron James" :player{age: 34, name: "LeBron James"})-[:like@0 {likeness: 100}]->("Ray Allen" :player{age: 43, name: "Ray Allen"})-[:serve@0 {end_year: 2012, start_year: 2007}]->("Celtics" :team{name: "Celtics"})>                                                                                    |
      | <("Blake Griffin" :player{age: 30, name: "Blake Griffin"})-[:like@0 {likeness: -1}]->("Chris Paul" :player{age: 33, name: "Chris Paul"})-[:like@0 {likeness: 90}]->("LeBron James" :player{age: 34, name: "LeBron James"})-[:like@0 {likeness: 100}]->("Ray Allen" :player{age: 43, name: "Ray Allen"})-[:like@0 {likeness: 9}]->("Rajon Rondo" :player{age: 33, name: "Rajon Rondo"})-[:serve@0 {end_year: 2018, start_year: 2017}]->("Pelicans" :team{name: "Pelicans"})>   |
      | <("Blake Griffin" :player{age: 30, name: "Blake Griffin"})-[:like@0 {likeness: -1}]->("Chris Paul" :player{age: 33, name: "Chris Paul"})-[:serve@0 {end_year: 2021, start_year: 2017}]->("Rockets" :team{name: "Rockets"})>                                                                                                                                                                                                                                                   |
      | <("Blake Griffin" :player{age: 30, name: "Blake Griffin"})-[:like@0 {likeness: -1}]->("Chris Paul" :player{age: 33, name: "Chris Paul"})-[:like@0 {likeness: 90}]->("Carmelo Anthony" :player{age: 34, name: "Carmelo Anthony"})-[:serve@0 {end_year: 2018, start_year: 2017}]->("Thunders" :team{name: "Thunders"})>                                                                                                                                                         |
      | <("Blake Griffin" :player{age: 30, name: "Blake Griffin"})-[:serve@0 {end_year: 2019, start_year: 2018}]->("Pistons" :team{name: "Pistons"})>                                                                                                                                                                                                                                                                                                                                 |
      | <("Blake Griffin" :player{age: 30, name: "Blake Griffin"})-[:like@0 {likeness: -1}]->("Chris Paul" :player{age: 33, name: "Chris Paul"})-[:like@0 {likeness: 90}]->("LeBron James" :player{age: 34, name: "LeBron James"})-[:serve@0 {end_year: 2010, start_year: 2003}]->("Cavaliers" :team{name: "Cavaliers"})>                                                                                                                                                             |
      | <("Blake Griffin" :player{age: 30, name: "Blake Griffin"})-[:like@0 {likeness: -1}]->("Chris Paul" :player{age: 33, name: "Chris Paul"})-[:like@0 {likeness: 90}]->("LeBron James" :player{age: 34, name: "LeBron James"})-[:serve@1 {end_year: 2018, start_year: 2014}]->("Cavaliers" :team{name: "Cavaliers"})>                                                                                                                                                             |
      | <("Blake Griffin" :player{age: 30, name: "Blake Griffin"})-[:like@0 {likeness: -1}]->("Chris Paul" :player{age: 33, name: "Chris Paul"})-[:like@0 {likeness: 90}]->("Dwyane Wade" :player{age: 37, name: "Dwyane Wade"})-[:serve@0 {end_year: 2018, start_year: 2017}]->("Cavaliers" :team{name: "Cavaliers"})>                                                                                                                                                               |
    When executing query:
      """
      MATCH p = allShortestPaths( (a:player{age:30})-[e*..5]->(b:team) )
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
      MATCH p = allShortestPaths( (a:player{name:"Yao Ming"})-[e:serve*1..3]-(b:team) ) RETURN p
      """
    Then the result should be, in any order, with relax comparison:
      | p                                                                                                                                                                                                                                                                                                                                    |
      | <("Yao Ming" :player{age: 38, name: "Yao Ming"})-[:serve@0 {end_year: 2011, start_year: 2002}]->("Rockets" :team{name: "Rockets"})<-[:serve@0 {end_year: 2016, start_year: 2013}]-("Dwight Howard" :player{age: 33, name: "Dwight Howard"})-[:serve@0 {end_year: 2013, start_year: 2012}]->("Lakers" :team{name: "Lakers"})>         |
      | <("Yao Ming" :player{age: 38, name: "Yao Ming"})-[:serve@0 {end_year: 2011, start_year: 2002}]->("Rockets" :team{name: "Rockets"})<-[:serve@0 {end_year: 2019, start_year: 2018}]-("Carmelo Anthony" :player{age: 34, name: "Carmelo Anthony"})-[:serve@0 {end_year: 2017, start_year: 2011}]->("Knicks" :team{name: "Knicks"})>     |
      | <("Yao Ming" :player{age: 38, name: "Yao Ming"})-[:serve@0 {end_year: 2011, start_year: 2002}]->("Rockets" :team{name: "Rockets"})<-[:serve@0 {end_year: 2016, start_year: 2013}]-("Dwight Howard" :player{age: 33, name: "Dwight Howard"})-[:serve@0 {end_year: 2018, start_year: 2017}]->("Hornets" :team{name: "Hornets"})>       |
      | <("Yao Ming" :player{age: 38, name: "Yao Ming"})-[:serve@0 {end_year: 2011, start_year: 2002}]->("Rockets" :team{name: "Rockets"})<-[:serve@0 {end_year: 2021, start_year: 2017}]-("Chris Paul" :player{age: 33, name: "Chris Paul"})-[:serve@0 {end_year: 2011, start_year: 2005}]->("Hornets" :team{name: "Hornets"})>             |
      | <("Yao Ming" :player{age: 38, name: "Yao Ming"})-[:serve@0 {end_year: 2011, start_year: 2002}]->("Rockets" :team{name: "Rockets"})<-[:serve@0 {end_year: 2021, start_year: 2017}]-("Chris Paul" :player{age: 33, name: "Chris Paul"})-[:serve@0 {end_year: 2017, start_year: 2011}]->("Clippers" :team{name: "Clippers"})>           |
      | <("Yao Ming" :player{age: 38, name: "Yao Ming"})-[:serve@0 {end_year: 2011, start_year: 2002}]->("Rockets" :team{name: "Rockets"})<-[:serve@0 {end_year: 2019, start_year: 2018}]-("Carmelo Anthony" :player{age: 34, name: "Carmelo Anthony"})-[:serve@0 {end_year: 2011, start_year: 2003}]->("Nuggets" :team{name: "Nuggets"})>   |
      | <("Yao Ming" :player{age: 38, name: "Yao Ming"})-[:serve@0 {end_year: 2011, start_year: 2002}]->("Rockets" :team{name: "Rockets"})>                                                                                                                                                                                                  |
      | <("Yao Ming" :player{age: 38, name: "Yao Ming"})-[:serve@0 {end_year: 2011, start_year: 2002}]->("Rockets" :team{name: "Rockets"})<-[:serve@0 {end_year: 2010, start_year: 2004}]-("Tracy McGrady" :player{age: 39, name: "Tracy McGrady"})-[:serve@0 {end_year: 2013, start_year: 2013}]->("Spurs" :team{name: "Spurs"})>           |
      | <("Yao Ming" :player{age: 38, name: "Yao Ming"})-[:serve@0 {end_year: 2011, start_year: 2002}]->("Rockets" :team{name: "Rockets"})<-[:serve@0 {end_year: 2019, start_year: 2012}]-("James Harden" :player{age: 29, name: "James Harden"})-[:serve@0 {end_year: 2012, start_year: 2009}]->("Thunders" :team{name: "Thunders"})>       |
      | <("Yao Ming" :player{age: 38, name: "Yao Ming"})-[:serve@0 {end_year: 2011, start_year: 2002}]->("Rockets" :team{name: "Rockets"})<-[:serve@0 {end_year: 2019, start_year: 2018}]-("Carmelo Anthony" :player{age: 34, name: "Carmelo Anthony"})-[:serve@0 {end_year: 2018, start_year: 2017}]->("Thunders" :team{name: "Thunders"})> |
      | <("Yao Ming" :player{age: 38, name: "Yao Ming"})-[:serve@0 {end_year: 2011, start_year: 2002}]->("Rockets" :team{name: "Rockets"})<-[:serve@0 {end_year: 2010, start_year: 2004}]-("Tracy McGrady" :player{age: 39, name: "Tracy McGrady"})-[:serve@0 {end_year: 2004, start_year: 2000}]->("Magic" :team{name: "Magic"})>           |
      | <("Yao Ming" :player{age: 38, name: "Yao Ming"})-[:serve@0 {end_year: 2011, start_year: 2002}]->("Rockets" :team{name: "Rockets"})<-[:serve@0 {end_year: 2016, start_year: 2013}]-("Dwight Howard" :player{age: 33, name: "Dwight Howard"})-[:serve@0 {end_year: 2012, start_year: 2004}]->("Magic" :team{name: "Magic"})>           |
      | <("Yao Ming" :player{age: 38, name: "Yao Ming"})-[:serve@0 {end_year: 2011, start_year: 2002}]->("Rockets" :team{name: "Rockets"})<-[:serve@0 {end_year: 2016, start_year: 2013}]-("Dwight Howard" :player{age: 33, name: "Dwight Howard"})-[:serve@0 {end_year: 2017, start_year: 2016}]->("Hawks" :team{name: "Hawks"})>           |
      | <("Yao Ming" :player{age: 38, name: "Yao Ming"})-[:serve@0 {end_year: 2011, start_year: 2002}]->("Rockets" :team{name: "Rockets"})<-[:serve@0 {end_year: 2010, start_year: 2004}]-("Tracy McGrady" :player{age: 39, name: "Tracy McGrady"})-[:serve@0 {end_year: 2000, start_year: 1997}]->("Raptors" :team{name: "Raptors"})>       |
      | <("Yao Ming" :player{age: 38, name: "Yao Ming"})-[:serve@0 {end_year: 2011, start_year: 2002}]->("Rockets" :team{name: "Rockets"})<-[:serve@0 {end_year: 2016, start_year: 2013}]-("Dwight Howard" :player{age: 33, name: "Dwight Howard"})-[:serve@0 {end_year: 2019, start_year: 2018}]->("Wizards" :team{name: "Wizards"})>       |

  Scenario: allShortestPaths2
    When executing query:
      """
      MATCH p = allShortestPaths( (a)-[e*..5]-(b) )
        WHERE id(a) == 'Tim Duncan' and id(b) in ['Spurs', 'Tony Parker', 'Yao Ming']
        RETURN p
      """
    Then the result should be, in any order, with relax comparison:
      | p                                                                                                                                                                                                                                                                                 |
      | <("Tim Duncan" :bachelor{name: "Tim Duncan", speciality: "psychology"} :player{age: 42, name: "Tim Duncan"})-[:serve@0 {end_year: 2016, start_year: 1997}]->("Spurs" :team{name: "Spurs"})>                                                                                       |
      | <("Tim Duncan" :bachelor{name: "Tim Duncan", speciality: "psychology"} :player{age: 42, name: "Tim Duncan"})<-[:teammate@0 {end_year: 2016, start_year: 2001}]-("Tony Parker" :player{age: 36, name: "Tony Parker"})>                                                             |
      | <("Tim Duncan" :bachelor{name: "Tim Duncan", speciality: "psychology"} :player{age: 42, name: "Tim Duncan"})-[:teammate@0 {end_year: 2016, start_year: 2001}]->("Tony Parker" :player{age: 36, name: "Tony Parker"})>                                                             |
      | <("Tim Duncan" :bachelor{name: "Tim Duncan", speciality: "psychology"} :player{age: 42, name: "Tim Duncan"})<-[:like@0 {likeness: 95}]-("Tony Parker" :player{age: 36, name: "Tony Parker"})>                                                                                     |
      | <("Tim Duncan" :bachelor{name: "Tim Duncan", speciality: "psychology"} :player{age: 42, name: "Tim Duncan"})-[:like@0 {likeness: 95}]->("Tony Parker" :player{age: 36, name: "Tony Parker"})>                                                                                     |
      | <("Tim Duncan" :bachelor{name: "Tim Duncan", speciality: "psychology"} :player{age: 42, name: "Tim Duncan"})<-[:like@0 {likeness: 80}]-("Shaquille O'Neal" :player{age: 47, name: "Shaquille O'Neal"})<-[:like@0 {likeness: 90}]-("Yao Ming" :player{age: 38, name: "Yao Ming"})> |
    When executing query:
      """
      MATCH p = allShortestPaths( (a)-[e*..5]->(b) )
        WHERE id(a) == 'Tim Duncan' and id(b) IN ['Spurs', 'Tony Parker', 'Yao Ming']
        RETURN p
      """
    Then the result should be, in any order, with relax comparison:
      | p                                                                                                                                                                                                                     |
      | <("Tim Duncan" :bachelor{name: "Tim Duncan", speciality: "psychology"} :player{age: 42, name: "Tim Duncan"})-[:serve@0 {end_year: 2016, start_year: 1997}]->("Spurs" :team{name: "Spurs"})>                           |
      | <("Tim Duncan" :bachelor{name: "Tim Duncan", speciality: "psychology"} :player{age: 42, name: "Tim Duncan"})-[:teammate@0 {end_year: 2016, start_year: 2001}]->("Tony Parker" :player{age: 36, name: "Tony Parker"})> |
      | <("Tim Duncan" :bachelor{name: "Tim Duncan", speciality: "psychology"} :player{age: 42, name: "Tim Duncan"})-[:like@0 {likeness: 95}]->("Tony Parker" :player{age: 36, name: "Tony Parker"})>                         |
    When executing query:
      """
      MATCH p = allShortestPaths( (a)-[e*..5]->(b) )
        WHERE id(b) IN ['Manu Ginobili', 'Spurs', 'Lakers'] and id(a) in ['Tony Parker', 'Yao Ming']
        RETURN p
      """
    Then the result should be, in any order, with relax comparison:
      | p                                                                                                                                                                                                                                                                                                                                                                                                                                                                                               |
      | <("Tony Parker" :player{age: 36, name: "Tony Parker"})-[:teammate@0 {end_year: 2018, start_year: 2002}]->("Manu Ginobili" :player{age: 41, name: "Manu Ginobili"})>                                                                                                                                                                                                                                                                                                                             |
      | <("Tony Parker" :player{age: 36, name: "Tony Parker"})-[:like@0 {likeness: 95}]->("Manu Ginobili" :player{age: 41, name: "Manu Ginobili"})>                                                                                                                                                                                                                                                                                                                                                     |
      | <("Tony Parker" :player{age: 36, name: "Tony Parker"})-[:serve@0 {end_year: 2018, start_year: 1999}]->("Spurs" :team{name: "Spurs"})>                                                                                                                                                                                                                                                                                                                                                           |
      | <("Tony Parker" :player{age: 36, name: "Tony Parker"})-[:teammate@0 {end_year: 2016, start_year: 2001}]->("Tim Duncan" :bachelor{name: "Tim Duncan", speciality: "psychology"} :player{age: 42, name: "Tim Duncan"})-[:teammate@0 {end_year: 2016, start_year: 2010}]->("Danny Green" :player{age: 31, name: "Danny Green"})-[:like@0 {likeness: 80}]->("LeBron James" :player{age: 34, name: "LeBron James"})-[:serve@0 {end_year: 2019, start_year: 2018}]->("Lakers" :team{name: "Lakers"})> |
      | <("Tony Parker" :player{age: 36, name: "Tony Parker"})-[:like@0 {likeness: 95}]->("Tim Duncan" :bachelor{name: "Tim Duncan", speciality: "psychology"} :player{age: 42, name: "Tim Duncan"})-[:teammate@0 {end_year: 2016, start_year: 2010}]->("Danny Green" :player{age: 31, name: "Danny Green"})-[:like@0 {likeness: 80}]->("LeBron James" :player{age: 34, name: "LeBron James"})-[:serve@0 {end_year: 2019, start_year: 2018}]->("Lakers" :team{name: "Lakers"})>                         |
      | <("Yao Ming" :player{age: 38, name: "Yao Ming"})-[:like@0 {likeness: 90}]->("Shaquille O'Neal" :player{age: 47, name: "Shaquille O'Neal"})-[:like@0 {likeness: 80}]->("Tim Duncan" :bachelor{name: "Tim Duncan", speciality: "psychology"} :player{age: 42, name: "Tim Duncan"})-[:teammate@0 {end_year: 2016, start_year: 2002}]->("Manu Ginobili" :player{age: 41, name: "Manu Ginobili"})>                                                                                                   |
      | <("Yao Ming" :player{age: 38, name: "Yao Ming"})-[:like@0 {likeness: 90}]->("Shaquille O'Neal" :player{age: 47, name: "Shaquille O'Neal"})-[:like@0 {likeness: 80}]->("Tim Duncan" :bachelor{name: "Tim Duncan", speciality: "psychology"} :player{age: 42, name: "Tim Duncan"})-[:like@0 {likeness: 95}]->("Manu Ginobili" :player{age: 41, name: "Manu Ginobili"})>                                                                                                                           |
      | <("Yao Ming" :player{age: 38, name: "Yao Ming"})-[:like@0 {likeness: 90}]->("Tracy McGrady" :player{age: 39, name: "Tracy McGrady"})-[:serve@0 {end_year: 2013, start_year: 2013}]->("Spurs" :team{name: "Spurs"})>                                                                                                                                                                                                                                                                             |
      | <("Yao Ming" :player{age: 38, name: "Yao Ming"})-[:like@0 {likeness: 90}]->("Shaquille O'Neal" :player{age: 47, name: "Shaquille O'Neal"})-[:serve@0 {end_year: 2004, start_year: 1996}]->("Lakers" :team{name: "Lakers"})>                                                                                                                                                                                                                                                                     |
    When executing query:
      """
      MATCH p = allShortestPaths( (a)-[e:like*..4]->(b) )
        WHERE id(b) IN ['Manu Ginobili', 'Spurs', 'Lakers'] and id(a) in ['Tony Parker', 'Yao Ming']
        RETURN p
      """
    Then the result should be, in any order, with relax comparison:
      | p                                                                                                                                                                                                                                                                                                                                                                     |
      | <("Tony Parker" :player{age: 36, name: "Tony Parker"})-[:like@0 {likeness: 95}]->("Manu Ginobili" :player{age: 41, name: "Manu Ginobili"})>                                                                                                                                                                                                                           |
      | <("Yao Ming" :player{age: 38, name: "Yao Ming"})-[:like@0 {likeness: 90}]->("Shaquille O'Neal" :player{age: 47, name: "Shaquille O'Neal"})-[:like@0 {likeness: 80}]->("Tim Duncan" :bachelor{name: "Tim Duncan", speciality: "psychology"} :player{age: 42, name: "Tim Duncan"})-[:like@0 {likeness: 95}]->("Manu Ginobili" :player{age: 41, name: "Manu Ginobili"})> |
    When executing query:
      """
      MATCH p = allShortestPaths( (a)-[e:like*..4]->(b) )
        WHERE id(b) IN ['Manu Ginobili', 'Spurs', 'Lakers'] and id(a) in ['xxx', 'zzz']
        RETURN p
      """
    Then the result should be, in any order, with relax comparison:
      | p |

  Scenario: allShortestPaths3
    When executing query:
      """
      MATCH (a:player{name:"Tim Duncan"}), (b:team{name:"Spurs"})
      MATCH p = allShortestPaths( (a)-[e:serve*..3]-(b) )
        RETURN p
      """
    Then the result should be, in any order, with relax comparison:
      | p                                                                                                                                                                                           |
      | <("Tim Duncan" :bachelor{name: "Tim Duncan", speciality: "psychology"} :player{age: 42, name: "Tim Duncan"})-[:serve@0 {end_year: 2016, start_year: 1997}]->("Spurs" :team{name: "Spurs"})> |
    When executing query:
      """
      MATCH (a:player{name:"Tim Duncan"}), (b:team{name:"Spurs"}), p = allShortestPaths( (a)-[e:serve*..3]-(b) ) RETURN p
      """
    Then the result should be, in any order, with relax comparison:
      | p                                                                                                                                                                                           |
      | <("Tim Duncan" :bachelor{name: "Tim Duncan", speciality: "psychology"} :player{age: 42, name: "Tim Duncan"})-[:serve@0 {end_year: 2016, start_year: 1997}]->("Spurs" :team{name: "Spurs"})> |
    When executing query:
      """
      MATCH (a:player{name:"Tim Duncan"}), (b:team)
      MATCH p = allShortestPaths( (a)-[e:serve*..3]-(b) )
        RETURN p
      """
    Then the result should be, in any order, with relax comparison:
      | p                                                                                                                                                                                                                                                                                                                                                                                                          |
      | <("Tim Duncan" :bachelor{name: "Tim Duncan", speciality: "psychology"} :player{age: 42, name: "Tim Duncan"})-[:serve@0 {end_year: 2016, start_year: 1997}]->("Spurs" :team{name: "Spurs"})<-[:serve@0 {end_year: 2019, start_year: 2016}]-("Paul Gasol" :player{age: 38, name: "Paul Gasol"})-[:serve@0 {end_year: 2008, start_year: 2001}]->("Grizzlies" :team{name: "Grizzlies"})>                       |
      | <("Tim Duncan" :bachelor{name: "Tim Duncan", speciality: "psychology"} :player{age: 42, name: "Tim Duncan"})-[:serve@0 {end_year: 2016, start_year: 1997}]->("Spurs" :team{name: "Spurs"})<-[:serve@0 {end_year: 2018, start_year: 2014}]-("Kyle Anderson" :player{age: 25, name: "Kyle Anderson"})-[:serve@0 {end_year: 2019, start_year: 2018}]->("Grizzlies" :team{name: "Grizzlies"})>                 |
      | <("Tim Duncan" :bachelor{name: "Tim Duncan", speciality: "psychology"} :player{age: 42, name: "Tim Duncan"})-[:serve@0 {end_year: 2016, start_year: 1997}]->("Spurs" :team{name: "Spurs"})<-[:serve@0 {end_year: 2019, start_year: 2017}]-("Rudy Gay" :player{age: 32, name: "Rudy Gay"})-[:serve@0 {end_year: 2013, start_year: 2006}]->("Grizzlies" :team{name: "Grizzlies"})>                           |
      | <("Tim Duncan" :bachelor{name: "Tim Duncan", speciality: "psychology"} :player{age: 42, name: "Tim Duncan"})-[:serve@0 {end_year: 2016, start_year: 1997}]->("Spurs" :team{name: "Spurs"})<-[:serve@0 {end_year: 2019, start_year: 2016}]-("Paul Gasol" :player{age: 38, name: "Paul Gasol"})-[:serve@0 {end_year: 2014, start_year: 2008}]->("Lakers" :team{name: "Lakers"})>                             |
      | <("Tim Duncan" :bachelor{name: "Tim Duncan", speciality: "psychology"} :player{age: 42, name: "Tim Duncan"})-[:serve@0 {end_year: 2016, start_year: 1997}]->("Spurs" :team{name: "Spurs"})<-[:serve@0 {end_year: 2019, start_year: 2016}]-("Paul Gasol" :player{age: 38, name: "Paul Gasol"})-[:serve@0 {end_year: 2016, start_year: 2014}]->("Bulls" :team{name: "Bulls"})>                               |
      | <("Tim Duncan" :bachelor{name: "Tim Duncan", speciality: "psychology"} :player{age: 42, name: "Tim Duncan"})-[:serve@0 {end_year: 2016, start_year: 1997}]->("Spurs" :team{name: "Spurs"})<-[:serve@0 {end_year: 2015, start_year: 2013}]-("Marco Belinelli" :player{age: 32, name: "Marco Belinelli"})-[:serve@0 {end_year: 2013, start_year: 2012}]->("Bulls" :team{name: "Bulls"})>                     |
      | <("Tim Duncan" :bachelor{name: "Tim Duncan", speciality: "psychology"} :player{age: 42, name: "Tim Duncan"})-[:serve@0 {end_year: 2016, start_year: 1997}]->("Spurs" :team{name: "Spurs"})<-[:serve@1 {end_year: 2019, start_year: 2018}]-("Marco Belinelli" :player{age: 32, name: "Marco Belinelli"})-[:serve@0 {end_year: 2013, start_year: 2012}]->("Bulls" :team{name: "Bulls"})>                     |
      | <("Tim Duncan" :bachelor{name: "Tim Duncan", speciality: "psychology"} :player{age: 42, name: "Tim Duncan"})-[:serve@0 {end_year: 2016, start_year: 1997}]->("Spurs" :team{name: "Spurs"})<-[:serve@0 {end_year: 2018, start_year: 1999}]-("Tony Parker" :player{age: 36, name: "Tony Parker"})-[:serve@0 {end_year: 2019, start_year: 2018}]->("Hornets" :team{name: "Hornets"})>                         |
      | <("Tim Duncan" :bachelor{name: "Tim Duncan", speciality: "psychology"} :player{age: 42, name: "Tim Duncan"})-[:serve@0 {end_year: 2016, start_year: 1997}]->("Spurs" :team{name: "Spurs"})<-[:serve@0 {end_year: 2016, start_year: 2015}]-("David West" :player{age: 38, name: "David West"})-[:serve@0 {end_year: 2011, start_year: 2003}]->("Hornets" :team{name: "Hornets"})>                           |
      | <("Tim Duncan" :bachelor{name: "Tim Duncan", speciality: "psychology"} :player{age: 42, name: "Tim Duncan"})-[:serve@0 {end_year: 2016, start_year: 1997}]->("Spurs" :team{name: "Spurs"})<-[:serve@0 {end_year: 2016, start_year: 2012}]-("Boris Diaw" :player{age: 36, name: "Boris Diaw"})-[:serve@0 {end_year: 2012, start_year: 2008}]->("Hornets" :team{name: "Hornets"})>                           |
      | <("Tim Duncan" :bachelor{name: "Tim Duncan", speciality: "psychology"} :player{age: 42, name: "Tim Duncan"})-[:serve@0 {end_year: 2016, start_year: 1997}]->("Spurs" :team{name: "Spurs"})<-[:serve@0 {end_year: 2015, start_year: 2013}]-("Marco Belinelli" :player{age: 32, name: "Marco Belinelli"})-[:serve@0 {end_year: 2012, start_year: 2010}]->("Hornets" :team{name: "Hornets"})>                 |
      | <("Tim Duncan" :bachelor{name: "Tim Duncan", speciality: "psychology"} :player{age: 42, name: "Tim Duncan"})-[:serve@0 {end_year: 2016, start_year: 1997}]->("Spurs" :team{name: "Spurs"})<-[:serve@0 {end_year: 2015, start_year: 2013}]-("Marco Belinelli" :player{age: 32, name: "Marco Belinelli"})-[:serve@1 {end_year: 2017, start_year: 2016}]->("Hornets" :team{name: "Hornets"})>                 |
      | <("Tim Duncan" :bachelor{name: "Tim Duncan", speciality: "psychology"} :player{age: 42, name: "Tim Duncan"})-[:serve@0 {end_year: 2016, start_year: 1997}]->("Spurs" :team{name: "Spurs"})<-[:serve@1 {end_year: 2019, start_year: 2018}]-("Marco Belinelli" :player{age: 32, name: "Marco Belinelli"})-[:serve@0 {end_year: 2012, start_year: 2010}]->("Hornets" :team{name: "Hornets"})>                 |
      | <("Tim Duncan" :bachelor{name: "Tim Duncan", speciality: "psychology"} :player{age: 42, name: "Tim Duncan"})-[:serve@0 {end_year: 2016, start_year: 1997}]->("Spurs" :team{name: "Spurs"})<-[:serve@1 {end_year: 2019, start_year: 2018}]-("Marco Belinelli" :player{age: 32, name: "Marco Belinelli"})-[:serve@1 {end_year: 2017, start_year: 2016}]->("Hornets" :team{name: "Hornets"})>                 |
      | <("Tim Duncan" :bachelor{name: "Tim Duncan", speciality: "psychology"} :player{age: 42, name: "Tim Duncan"})-[:serve@0 {end_year: 2016, start_year: 1997}]->("Spurs" :team{name: "Spurs"})<-[:serve@0 {end_year: 2019, start_year: 2017}]-("Rudy Gay" :player{age: 32, name: "Rudy Gay"})-[:serve@0 {end_year: 2017, start_year: 2013}]->("Kings" :team{name: "Kings"})>                                   |
      | <("Tim Duncan" :bachelor{name: "Tim Duncan", speciality: "psychology"} :player{age: 42, name: "Tim Duncan"})-[:serve@0 {end_year: 2016, start_year: 1997}]->("Spurs" :team{name: "Spurs"})<-[:serve@0 {end_year: 2015, start_year: 2013}]-("Marco Belinelli" :player{age: 32, name: "Marco Belinelli"})-[:serve@0 {end_year: 2016, start_year: 2015}]->("Kings" :team{name: "Kings"})>                     |
      | <("Tim Duncan" :bachelor{name: "Tim Duncan", speciality: "psychology"} :player{age: 42, name: "Tim Duncan"})-[:serve@0 {end_year: 2016, start_year: 1997}]->("Spurs" :team{name: "Spurs"})<-[:serve@1 {end_year: 2019, start_year: 2018}]-("Marco Belinelli" :player{age: 32, name: "Marco Belinelli"})-[:serve@0 {end_year: 2016, start_year: 2015}]->("Kings" :team{name: "Kings"})>                     |
      | <("Tim Duncan" :bachelor{name: "Tim Duncan", speciality: "psychology"} :player{age: 42, name: "Tim Duncan"})-[:serve@0 {end_year: 2016, start_year: 1997}]->("Spurs" :team{name: "Spurs"})<-[:serve@0 {end_year: 2016, start_year: 2012}]-("Boris Diaw" :player{age: 36, name: "Boris Diaw"})-[:serve@0 {end_year: 2017, start_year: 2016}]->("Jazz" :team{name: "Jazz"})>                                 |
      | <("Tim Duncan" :bachelor{name: "Tim Duncan", speciality: "psychology"} :player{age: 42, name: "Tim Duncan"})-[:serve@0 {end_year: 2016, start_year: 1997}]->("Spurs" :team{name: "Spurs"})<-[:serve@0 {end_year: 2016, start_year: 2012}]-("Boris Diaw" :player{age: 36, name: "Boris Diaw"})-[:serve@0 {end_year: 2008, start_year: 2005}]->("Suns" :team{name: "Suns"})>                                 |
      | <("Tim Duncan" :bachelor{name: "Tim Duncan", speciality: "psychology"} :player{age: 42, name: "Tim Duncan"})-[:serve@0 {end_year: 2016, start_year: 1997}]->("Spurs" :team{name: "Spurs"})<-[:serve@0 {end_year: 2015, start_year: 2010}]-("Tiago Splitter" :player{age: 34, name: "Tiago Splitter"})-[:serve@0 {end_year: 2017, start_year: 2015}]->("Hawks" :team{name: "Hawks"})>                       |
      | <("Tim Duncan" :bachelor{name: "Tim Duncan", speciality: "psychology"} :player{age: 42, name: "Tim Duncan"})-[:serve@0 {end_year: 2016, start_year: 1997}]->("Spurs" :team{name: "Spurs"})<-[:serve@0 {end_year: 2016, start_year: 2012}]-("Boris Diaw" :player{age: 36, name: "Boris Diaw"})-[:serve@0 {end_year: 2005, start_year: 2003}]->("Hawks" :team{name: "Hawks"})>                               |
      | <("Tim Duncan" :bachelor{name: "Tim Duncan", speciality: "psychology"} :player{age: 42, name: "Tim Duncan"})-[:serve@0 {end_year: 2016, start_year: 1997}]->("Spurs" :team{name: "Spurs"})<-[:serve@0 {end_year: 2015, start_year: 2013}]-("Marco Belinelli" :player{age: 32, name: "Marco Belinelli"})-[:serve@0 {end_year: 2018, start_year: 2017}]->("Hawks" :team{name: "Hawks"})>                     |
      | <("Tim Duncan" :bachelor{name: "Tim Duncan", speciality: "psychology"} :player{age: 42, name: "Tim Duncan"})-[:serve@0 {end_year: 2016, start_year: 1997}]->("Spurs" :team{name: "Spurs"})<-[:serve@1 {end_year: 2019, start_year: 2018}]-("Marco Belinelli" :player{age: 32, name: "Marco Belinelli"})-[:serve@0 {end_year: 2018, start_year: 2017}]->("Hawks" :team{name: "Hawks"})>                     |
      | <("Tim Duncan" :bachelor{name: "Tim Duncan", speciality: "psychology"} :player{age: 42, name: "Tim Duncan"})-[:serve@0 {end_year: 2016, start_year: 1997}]->("Spurs" :team{name: "Spurs"})<-[:serve@0 {end_year: 2016, start_year: 2015}]-("David West" :player{age: 38, name: "David West"})-[:serve@0 {end_year: 2018, start_year: 2016}]->("Warriors" :team{name: "Warriors"})>                         |
      | <("Tim Duncan" :bachelor{name: "Tim Duncan", speciality: "psychology"} :player{age: 42, name: "Tim Duncan"})-[:serve@0 {end_year: 2016, start_year: 1997}]->("Spurs" :team{name: "Spurs"})<-[:serve@0 {end_year: 2015, start_year: 2013}]-("Marco Belinelli" :player{age: 32, name: "Marco Belinelli"})-[:serve@0 {end_year: 2009, start_year: 2007}]->("Warriors" :team{name: "Warriors"})>               |
      | <("Tim Duncan" :bachelor{name: "Tim Duncan", speciality: "psychology"} :player{age: 42, name: "Tim Duncan"})-[:serve@0 {end_year: 2016, start_year: 1997}]->("Spurs" :team{name: "Spurs"})<-[:serve@1 {end_year: 2019, start_year: 2018}]-("Marco Belinelli" :player{age: 32, name: "Marco Belinelli"})-[:serve@0 {end_year: 2009, start_year: 2007}]->("Warriors" :team{name: "Warriors"})>               |
      | <("Tim Duncan" :bachelor{name: "Tim Duncan", speciality: "psychology"} :player{age: 42, name: "Tim Duncan"})-[:serve@0 {end_year: 2016, start_year: 1997}]->("Spurs" :team{name: "Spurs"})<-[:serve@0 {end_year: 2019, start_year: 2016}]-("Paul Gasol" :player{age: 38, name: "Paul Gasol"})-[:serve@0 {end_year: 2020, start_year: 2019}]->("Bucks" :team{name: "Bucks"})>                               |
      | <("Tim Duncan" :bachelor{name: "Tim Duncan", speciality: "psychology"} :player{age: 42, name: "Tim Duncan"})-[:serve@0 {end_year: 2016, start_year: 1997}]->("Spurs" :team{name: "Spurs"})<-[:serve@0 {end_year: 2015, start_year: 2010}]-("Tiago Splitter" :player{age: 34, name: "Tiago Splitter"})-[:serve@0 {end_year: 2017, start_year: 2017}]->("76ers" :team{name: "76ers"})>                       |
      | <("Tim Duncan" :bachelor{name: "Tim Duncan", speciality: "psychology"} :player{age: 42, name: "Tim Duncan"})-[:serve@0 {end_year: 2016, start_year: 1997}]->("Spurs" :team{name: "Spurs"})<-[:serve@0 {end_year: 2017, start_year: 2015}]-("Jonathon Simmons" :player{age: 29, name: "Jonathon Simmons"})-[:serve@0 {end_year: 2019, start_year: 2019}]->("76ers" :team{name: "76ers"})>                   |
      | <("Tim Duncan" :bachelor{name: "Tim Duncan", speciality: "psychology"} :player{age: 42, name: "Tim Duncan"})-[:serve@0 {end_year: 2016, start_year: 1997}]->("Spurs" :team{name: "Spurs"})<-[:serve@0 {end_year: 2015, start_year: 2013}]-("Marco Belinelli" :player{age: 32, name: "Marco Belinelli"})-[:serve@0 {end_year: 2018, start_year: 2018}]->("76ers" :team{name: "76ers"})>                     |
      | <("Tim Duncan" :bachelor{name: "Tim Duncan", speciality: "psychology"} :player{age: 42, name: "Tim Duncan"})-[:serve@0 {end_year: 2016, start_year: 1997}]->("Spurs" :team{name: "Spurs"})<-[:serve@1 {end_year: 2019, start_year: 2018}]-("Marco Belinelli" :player{age: 32, name: "Marco Belinelli"})-[:serve@0 {end_year: 2018, start_year: 2018}]->("76ers" :team{name: "76ers"})>                     |
      | <("Tim Duncan" :bachelor{name: "Tim Duncan", speciality: "psychology"} :player{age: 42, name: "Tim Duncan"})-[:serve@0 {end_year: 2016, start_year: 1997}]->("Spurs" :team{name: "Spurs"})<-[:serve@0 {end_year: 2015, start_year: 2013}]-("Aron Baynes" :player{age: 32, name: "Aron Baynes"})-[:serve@0 {end_year: 2019, start_year: 2017}]->("Celtics" :team{name: "Celtics"})>                         |
      | <("Tim Duncan" :bachelor{name: "Tim Duncan", speciality: "psychology"} :player{age: 42, name: "Tim Duncan"})-[:serve@0 {end_year: 2016, start_year: 1997}]->("Spurs" :team{name: "Spurs"})<-[:serve@0 {end_year: 2019, start_year: 2015}]-("LaMarcus Aldridge" :player{age: 33, name: "LaMarcus Aldridge"})-[:serve@0 {end_year: 2015, start_year: 2006}]->("Trail Blazers" :team{name: "Trail Blazers"})> |
      | <("Tim Duncan" :bachelor{name: "Tim Duncan", speciality: "psychology"} :player{age: 42, name: "Tim Duncan"})-[:serve@0 {end_year: 2016, start_year: 1997}]->("Spurs" :team{name: "Spurs"})<-[:serve@0 {end_year: 2013, start_year: 2013}]-("Tracy McGrady" :player{age: 39, name: "Tracy McGrady"})-[:serve@0 {end_year: 2010, start_year: 2004}]->("Rockets" :team{name: "Rockets"})>                     |
      | <("Tim Duncan" :bachelor{name: "Tim Duncan", speciality: "psychology"} :player{age: 42, name: "Tim Duncan"})-[:serve@0 {end_year: 2016, start_year: 1997}]->("Spurs" :team{name: "Spurs"})>                                                                                                                                                                                                                |
      | <("Tim Duncan" :bachelor{name: "Tim Duncan", speciality: "psychology"} :player{age: 42, name: "Tim Duncan"})-[:serve@0 {end_year: 2016, start_year: 1997}]->("Spurs" :team{name: "Spurs"})<-[:serve@0 {end_year: 2017, start_year: 2015}]-("Jonathon Simmons" :player{age: 29, name: "Jonathon Simmons"})-[:serve@0 {end_year: 2019, start_year: 2017}]->("Magic" :team{name: "Magic"})>                   |
      | <("Tim Duncan" :bachelor{name: "Tim Duncan", speciality: "psychology"} :player{age: 42, name: "Tim Duncan"})-[:serve@0 {end_year: 2016, start_year: 1997}]->("Spurs" :team{name: "Spurs"})<-[:serve@0 {end_year: 2013, start_year: 2013}]-("Tracy McGrady" :player{age: 39, name: "Tracy McGrady"})-[:serve@0 {end_year: 2004, start_year: 2000}]->("Magic" :team{name: "Magic"})>                         |
      | <("Tim Duncan" :bachelor{name: "Tim Duncan", speciality: "psychology"} :player{age: 42, name: "Tim Duncan"})-[:serve@0 {end_year: 2016, start_year: 1997}]->("Spurs" :team{name: "Spurs"})<-[:serve@0 {end_year: 2016, start_year: 2015}]-("David West" :player{age: 38, name: "David West"})-[:serve@0 {end_year: 2015, start_year: 2011}]->("Pacers" :team{name: "Pacers"})>                             |
      | <("Tim Duncan" :bachelor{name: "Tim Duncan", speciality: "psychology"} :player{age: 42, name: "Tim Duncan"})-[:serve@0 {end_year: 2016, start_year: 1997}]->("Spurs" :team{name: "Spurs"})<-[:serve@0 {end_year: 2015, start_year: 2011}]-("Cory Joseph" :player{age: 27, name: "Cory Joseph"})-[:serve@0 {end_year: 2019, start_year: 2017}]->("Pacers" :team{name: "Pacers"})>                           |
      | <("Tim Duncan" :bachelor{name: "Tim Duncan", speciality: "psychology"} :player{age: 42, name: "Tim Duncan"})-[:serve@0 {end_year: 2016, start_year: 1997}]->("Spurs" :team{name: "Spurs"})<-[:serve@0 {end_year: 2015, start_year: 2013}]-("Aron Baynes" :player{age: 32, name: "Aron Baynes"})-[:serve@0 {end_year: 2017, start_year: 2015}]->("Pistons" :team{name: "Pistons"})>                         |
      | <("Tim Duncan" :bachelor{name: "Tim Duncan", speciality: "psychology"} :player{age: 42, name: "Tim Duncan"})-[:serve@0 {end_year: 2016, start_year: 1997}]->("Spurs" :team{name: "Spurs"})<-[:serve@0 {end_year: 2018, start_year: 2010}]-("Danny Green" :player{age: 31, name: "Danny Green"})-[:serve@0 {end_year: 2010, start_year: 2009}]->("Cavaliers" :team{name: "Cavaliers"})>                     |
      | <("Tim Duncan" :bachelor{name: "Tim Duncan", speciality: "psychology"} :player{age: 42, name: "Tim Duncan"})-[:serve@0 {end_year: 2016, start_year: 1997}]->("Spurs" :team{name: "Spurs"})<-[:serve@0 {end_year: 2018, start_year: 2010}]-("Danny Green" :player{age: 31, name: "Danny Green"})-[:serve@0 {end_year: 2019, start_year: 2018}]->("Raptors" :team{name: "Raptors"})>                         |
      | <("Tim Duncan" :bachelor{name: "Tim Duncan", speciality: "psychology"} :player{age: 42, name: "Tim Duncan"})-[:serve@0 {end_year: 2016, start_year: 1997}]->("Spurs" :team{name: "Spurs"})<-[:serve@0 {end_year: 2015, start_year: 2011}]-("Cory Joseph" :player{age: 27, name: "Cory Joseph"})-[:serve@0 {end_year: 2017, start_year: 2015}]->("Raptors" :team{name: "Raptors"})>                         |
      | <("Tim Duncan" :bachelor{name: "Tim Duncan", speciality: "psychology"} :player{age: 42, name: "Tim Duncan"})-[:serve@0 {end_year: 2016, start_year: 1997}]->("Spurs" :team{name: "Spurs"})<-[:serve@0 {end_year: 2013, start_year: 2013}]-("Tracy McGrady" :player{age: 39, name: "Tracy McGrady"})-[:serve@0 {end_year: 2000, start_year: 1997}]->("Raptors" :team{name: "Raptors"})>                     |
      | <("Tim Duncan" :bachelor{name: "Tim Duncan", speciality: "psychology"} :player{age: 42, name: "Tim Duncan"})-[:serve@0 {end_year: 2016, start_year: 1997}]->("Spurs" :team{name: "Spurs"})<-[:serve@0 {end_year: 2019, start_year: 2017}]-("Rudy Gay" :player{age: 32, name: "Rudy Gay"})-[:serve@0 {end_year: 2013, start_year: 2013}]->("Raptors" :team{name: "Raptors"})>                               |
      | <("Tim Duncan" :bachelor{name: "Tim Duncan", speciality: "psychology"} :player{age: 42, name: "Tim Duncan"})-[:serve@0 {end_year: 2016, start_year: 1997}]->("Spurs" :team{name: "Spurs"})<-[:serve@0 {end_year: 2015, start_year: 2013}]-("Marco Belinelli" :player{age: 32, name: "Marco Belinelli"})-[:serve@0 {end_year: 2010, start_year: 2009}]->("Raptors" :team{name: "Raptors"})>                 |
      | <("Tim Duncan" :bachelor{name: "Tim Duncan", speciality: "psychology"} :player{age: 42, name: "Tim Duncan"})-[:serve@0 {end_year: 2016, start_year: 1997}]->("Spurs" :team{name: "Spurs"})<-[:serve@1 {end_year: 2019, start_year: 2018}]-("Marco Belinelli" :player{age: 32, name: "Marco Belinelli"})-[:serve@0 {end_year: 2010, start_year: 2009}]->("Raptors" :team{name: "Raptors"})>                 |
    When executing query:
      """
      MATCH (b:team), (a:player{age:30})
      MATCH p = allShortestPaths( (a)-[e*..5]->(b) )
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
      MATCH p = allShortestPaths( (a:player)-[e*..5]->(b) )
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
      MATCH p = allShortestPaths( (a:player)-[e*..5]->(b) )
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

  Scenario: allShortestPaths4
    When executing query:
      """
      MATCH p = allShortestPaths( (a:player)-[e:serve*..3]-(b:team) )
        WHERE a.player.name == 'Tim Duncan' AND b.team.name == 'Spurs'
        RETURN p
      """
    Then the result should be, in any order, with relax comparison:
      | p                                                                                                                                                                                           |
      | <("Tim Duncan" :bachelor{name: "Tim Duncan", speciality: "psychology"} :player{age: 42, name: "Tim Duncan"})-[:serve@0 {end_year: 2016, start_year: 1997}]->("Spurs" :team{name: "Spurs"})> |
    When executing query:
      """
      MATCH p = allShortestPaths( (a:player)-[e:serve*..3]-(b:team) )
        WHERE a.player.age > 45 AND b.team.name == 'Spurs'
        RETURN p
      """
    Then the result should be, in any order, with relax comparison:
      | p                                                                                                                                                                                                                                                                                                                                            |
      | <("Grant Hill" :player{age: 46, name: "Grant Hill"})-[:serve@0 {end_year: 2007, start_year: 2000}]->("Magic" :team{name: "Magic"})<-[:serve@0 {end_year: 2004, start_year: 2000}]-("Tracy McGrady" :player{age: 39, name: "Tracy McGrady"})-[:serve@0 {end_year: 2013, start_year: 2013}]->("Spurs" :team{name: "Spurs"})>                   |
      | <("Grant Hill" :player{age: 46, name: "Grant Hill"})-[:serve@0 {end_year: 2007, start_year: 2000}]->("Magic" :team{name: "Magic"})<-[:serve@0 {end_year: 2019, start_year: 2017}]-("Jonathon Simmons" :player{age: 29, name: "Jonathon Simmons"})-[:serve@0 {end_year: 2017, start_year: 2015}]->("Spurs" :team{name: "Spurs"})>             |
      | <("Grant Hill" :player{age: 46, name: "Grant Hill"})-[:serve@0 {end_year: 2000, start_year: 1994}]->("Pistons" :team{name: "Pistons"})<-[:serve@0 {end_year: 2017, start_year: 2015}]-("Aron Baynes" :player{age: 32, name: "Aron Baynes"})-[:serve@0 {end_year: 2015, start_year: 2013}]->("Spurs" :team{name: "Spurs"})>                   |
      | <("Grant Hill" :player{age: 46, name: "Grant Hill"})-[:serve@0 {end_year: 2012, start_year: 2007}]->("Suns" :team{name: "Suns"})<-[:serve@0 {end_year: 2008, start_year: 2005}]-("Boris Diaw" :player{age: 36, name: "Boris Diaw"})-[:serve@0 {end_year: 2016, start_year: 2012}]->("Spurs" :team{name: "Spurs"})>                           |
      | <("Shaquille O'Neal" :player{age: 47, name: "Shaquille O'Neal"})-[:serve@0 {end_year: 1996, start_year: 1992}]->("Magic" :team{name: "Magic"})<-[:serve@0 {end_year: 2004, start_year: 2000}]-("Tracy McGrady" :player{age: 39, name: "Tracy McGrady"})-[:serve@0 {end_year: 2013, start_year: 2013}]->("Spurs" :team{name: "Spurs"})>       |
      | <("Shaquille O'Neal" :player{age: 47, name: "Shaquille O'Neal"})-[:serve@0 {end_year: 1996, start_year: 1992}]->("Magic" :team{name: "Magic"})<-[:serve@0 {end_year: 2019, start_year: 2017}]-("Jonathon Simmons" :player{age: 29, name: "Jonathon Simmons"})-[:serve@0 {end_year: 2017, start_year: 2015}]->("Spurs" :team{name: "Spurs"})> |
      | <("Shaquille O'Neal" :player{age: 47, name: "Shaquille O'Neal"})-[:serve@0 {end_year: 2009, start_year: 2008}]->("Suns" :team{name: "Suns"})<-[:serve@0 {end_year: 2008, start_year: 2005}]-("Boris Diaw" :player{age: 36, name: "Boris Diaw"})-[:serve@0 {end_year: 2016, start_year: 2012}]->("Spurs" :team{name: "Spurs"})>               |
      | <("Shaquille O'Neal" :player{age: 47, name: "Shaquille O'Neal"})-[:serve@0 {end_year: 2004, start_year: 1996}]->("Lakers" :team{name: "Lakers"})<-[:serve@0 {end_year: 2014, start_year: 2008}]-("Paul Gasol" :player{age: 38, name: "Paul Gasol"})-[:serve@0 {end_year: 2019, start_year: 2016}]->("Spurs" :team{name: "Spurs"})>           |
      | <("Shaquille O'Neal" :player{age: 47, name: "Shaquille O'Neal"})-[:serve@0 {end_year: 2011, start_year: 2010}]->("Celtics" :team{name: "Celtics"})<-[:serve@0 {end_year: 2019, start_year: 2017}]-("Aron Baynes" :player{age: 32, name: "Aron Baynes"})-[:serve@0 {end_year: 2015, start_year: 2013}]->("Spurs" :team{name: "Spurs"})>       |
      | <("Shaquille O'Neal" :player{age: 47, name: "Shaquille O'Neal"})-[:serve@0 {end_year: 2010, start_year: 2009}]->("Cavaliers" :team{name: "Cavaliers"})<-[:serve@0 {end_year: 2010, start_year: 2009}]-("Danny Green" :player{age: 31, name: "Danny Green"})-[:serve@0 {end_year: 2018, start_year: 2010}]->("Spurs" :team{name: "Spurs"})>   |
    When executing query:
      """
      MATCH p = allShortestPaths( (a:player)-[e:like*..3]-(b:player) )
        WHERE a.player.age > 45 AND b.player.age < 30
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
      MATCH p = allShortestPaths( (a)<-[e:like*..2]-(b:player{name:"Yao Ming"}) )
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
      MATCH p = allShortestPaths( (a)-[e:like*..2]->(b:player{name:"Tony Parker"}) )
        RETURN p
      """
    Then the result should be, in any order, with relax comparison:
      | p                                                                                                                                                                                                                                                                                       |
      | <("Tim Duncan" :bachelor{name: "Tim Duncan", speciality: "psychology"} :player{age: 42, name: "Tim Duncan"})-[:like@0 {likeness: 95}]->("Tony Parker" :player{age: 36, name: "Tony Parker"})>                                                                                           |
      | <("Rudy Gay" :player{age: 32, name: "Rudy Gay"})-[:like@0 {likeness: 70}]->("LaMarcus Aldridge" :player{age: 33, name: "LaMarcus Aldridge"})-[:like@0 {likeness: 75}]->("Tony Parker" :player{age: 36, name: "Tony Parker"})>                                                           |
      | <("Damian Lillard" :player{age: 28, name: "Damian Lillard"})-[:like@0 {likeness: 80}]->("LaMarcus Aldridge" :player{age: 33, name: "LaMarcus Aldridge"})-[:like@0 {likeness: 75}]->("Tony Parker" :player{age: 36, name: "Tony Parker"})>                                               |
      | <("Marco Belinelli" :player{age: 32, name: "Marco Belinelli"})-[:like@0 {likeness: 50}]->("Tony Parker" :player{age: 36, name: "Tony Parker"})>                                                                                                                                         |
      | <("Aron Baynes" :player{age: 32, name: "Aron Baynes"})-[:like@0 {likeness: 80}]->("Tim Duncan" :bachelor{name: "Tim Duncan", speciality: "psychology"} :player{age: 42, name: "Tim Duncan"})-[:like@0 {likeness: 95}]->("Tony Parker" :player{age: 36, name: "Tony Parker"})>           |
      | <("Tiago Splitter" :player{age: 34, name: "Tiago Splitter"})-[:like@0 {likeness: 80}]->("Tim Duncan" :bachelor{name: "Tim Duncan", speciality: "psychology"} :player{age: 42, name: "Tim Duncan"})-[:like@0 {likeness: 95}]->("Tony Parker" :player{age: 36, name: "Tony Parker"})>     |
      | <("Shaquille O'Neal" :player{age: 47, name: "Shaquille O'Neal"})-[:like@0 {likeness: 80}]->("Tim Duncan" :bachelor{name: "Tim Duncan", speciality: "psychology"} :player{age: 42, name: "Tim Duncan"})-[:like@0 {likeness: 95}]->("Tony Parker" :player{age: 36, name: "Tony Parker"})> |
      | <("Danny Green" :player{age: 31, name: "Danny Green"})-[:like@0 {likeness: 70}]->("Tim Duncan" :bachelor{name: "Tim Duncan", speciality: "psychology"} :player{age: 42, name: "Tim Duncan"})-[:like@0 {likeness: 95}]->("Tony Parker" :player{age: 36, name: "Tony Parker"})>           |
      | <("Danny Green" :player{age: 31, name: "Danny Green"})-[:like@0 {likeness: 83}]->("Marco Belinelli" :player{age: 32, name: "Marco Belinelli"})-[:like@0 {likeness: 50}]->("Tony Parker" :player{age: 36, name: "Tony Parker"})>                                                         |
      | <("Boris Diaw" :player{age: 36, name: "Boris Diaw"})-[:like@0 {likeness: 80}]->("Tony Parker" :player{age: 36, name: "Tony Parker"})>                                                                                                                                                   |
      | <("Dejounte Murray" :player{age: 29, name: "Dejounte Murray"})-[:like@0 {likeness: 99}]->("Tony Parker" :player{age: 36, name: "Tony Parker"})>                                                                                                                                         |
      | <("Manu Ginobili" :player{age: 41, name: "Manu Ginobili"})-[:like@0 {likeness: 90}]->("Tim Duncan" :bachelor{name: "Tim Duncan", speciality: "psychology"} :player{age: 42, name: "Tim Duncan"})-[:like@0 {likeness: 95}]->("Tony Parker" :player{age: 36, name: "Tony Parker"})>       |
      | <("LaMarcus Aldridge" :player{age: 33, name: "LaMarcus Aldridge"})-[:like@0 {likeness: 75}]->("Tony Parker" :player{age: 36, name: "Tony Parker"})>                                                                                                                                     |
    When executing query:
      """
      MATCH (a:player)
      MATCH p = allShortestPaths( (a)-[e:like*..2]->(b) )
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

  Scenario: allShortestPaths5
    When executing query:
      """
      MATCH p= allShortestPaths((a:player {name:"Tim Duncan"})-[*..15]-(b:player {age:33}))
        WITH nodes(p) AS pathNodes
        UNWIND pathNodes AS node
        RETURN DISTINCT node
      """
    Then the result should be, in any order, with relax comparison:
      | node                                                                                                        |
      | ("Tim Duncan" :bachelor{name: "Tim Duncan", speciality: "psychology"} :player{age: 42, name: "Tim Duncan"}) |
      | ("Dejounte Murray" :player{age: 29, name: "Dejounte Murray"})                                               |
      | ("Chris Paul" :player{age: 33, name: "Chris Paul"})                                                         |
      | ("Dwight Howard" :player{age: 33, name: "Dwight Howard"})                                                   |
      | ("Shaquille O'Neal" :player{age: 47, name: "Shaquille O'Neal"})                                             |
      | ("Lakers" :team{name: "Lakers"})                                                                            |
      | ("Rajon Rondo" :player{age: 33, name: "Rajon Rondo"})                                                       |
      | ("Hornets" :team{name: "Hornets"})                                                                          |
      | ("Marco Belinelli" :player{age: 32, name: "Marco Belinelli"})                                               |
      | ("Kings" :team{name: "Kings"})                                                                              |
      | ("Tony Parker" :player{age: 36, name: "Tony Parker"})                                                       |
      | ("Boris Diaw" :player{age: 36, name: "Boris Diaw"})                                                         |
      | ("Aron Baynes" :player{age: 32, name: "Aron Baynes"})                                                       |
      | ("Celtics" :team{name: "Celtics"})                                                                          |
      | ("Hawks" :team{name: "Hawks"})                                                                              |
      | ("Tiago Splitter" :player{age: 34, name: "Tiago Splitter"})                                                 |
      | ("Magic" :team{name: "Magic"})                                                                              |
      | ("LaMarcus Aldridge" :player{age: 33, name: "LaMarcus Aldridge"})                                           |
      | ("Bulls" :team{name: "Bulls"})                                                                              |

  Scenario: allShortestPaths Can Not find index
    When executing query:
      """
      MATCH p = allShortestPaths( (a)-[e*..5]-(b) )
        WHERE id(a) == 'Tim Duncan' OR id(b) in ['Spurs', 'Tony Parker', 'Yao Ming']
        RETURN p
      """
    Then a ExecutionError should be raised at runtime: Scan vertices or edges need to specify a limit number, or limit number can not push down.
    When executing query:
      """
      MATCH p = allShortestPaths( (a)-[e*..5]-(b) )
        WHERE id(a) == 'Tim Duncan'
        RETURN p
      """
    Then a ExecutionError should be raised at runtime: Scan vertices or edges need to specify a limit number, or limit number can not push down.

  Scenario: allShortestPaths for argument issue
    When profiling query:
      """
      MATCH (a:player{name:'Tim Duncan'}), (b:player{name:'Tony Parker'})
      WITH a AS b, b AS a
      MATCH allShortestPaths((a)-[:like*1..3]-(b))
      RETURN a, b
      """
    Then the result should be, in any order, with relax comparison:
      | a                                                     | b                                          |
      | ("Tony Parker" :player{age: 36, name: "Tony Parker"}) | ("Tim Duncan" :player{name: "Tim Duncan"}) |
      | ("Tony Parker" :player{age: 36, name: "Tony Parker"}) | ("Tim Duncan" :player{name: "Tim Duncan"}) |
    And the execution plan should be:
      | id | name           | dependencies | operator info |
      | 19 | Project        | 18           |               |
      | 18 | HashInnerJoin  | 10,17        |               |
      | 10 | Project        | 9            |               |
      | 9  | CrossJoin      | 24,25        |               |
      | 24 | AppendVertices | 20           |               |
      | 20 | IndexScan      | 2            |               |
      | 2  | Start          |              |               |
      | 25 | AppendVertices | 21           |               |
      | 21 | IndexScan      | 6            |               |
      | 6  | Start          |              |               |
      | 17 | Project        | 16           |               |
      | 16 | ShortestPath   | 15           |               |
      | 15 | CrossJoin      | 12,14        |               |
      | 12 | Project        | 11           |               |
      | 11 | Argument       |              |               |
      | 14 | Project        | 13           |               |
      | 13 | Argument       |              |               |
    When profiling query:
      """
      MATCH (a:player{name:'Tim Duncan'}), (b:player{name:'Tony Parker'})
      WITH a AS b, b AS a
      OPTIONAL MATCH allShortestPaths((a)-[:like*1..3]-(b))
      RETURN a, b
      """
    Then the result should be, in any order, with relax comparison:
      | a                                                     | b                                          |
      | ("Tony Parker" :player{age: 36, name: "Tony Parker"}) | ("Tim Duncan" :player{name: "Tim Duncan"}) |
      | ("Tony Parker" :player{age: 36, name: "Tony Parker"}) | ("Tim Duncan" :player{name: "Tim Duncan"}) |
    And the execution plan should be:
      | id | name           | dependencies | operator info |
      | 19 | Project        | 18           |               |
      | 18 | HashLeftJoin   | 10,17        |               |
      | 10 | Project        | 9            |               |
      | 9  | CrossJoin      | 24,25        |               |
      | 24 | AppendVertices | 20           |               |
      | 20 | IndexScan      | 2            |               |
      | 2  | Start          |              |               |
      | 25 | AppendVertices | 21           |               |
      | 21 | IndexScan      | 6            |               |
      | 6  | Start          |              |               |
      | 17 | Project        | 16           |               |
      | 16 | ShortestPath   | 15           |               |
      | 15 | CrossJoin      | 12,14        |               |
      | 12 | Project        | 11           |               |
      | 11 | Argument       |              |               |
      | 14 | Project        | 13           |               |
      | 13 | Argument       |              |               |
