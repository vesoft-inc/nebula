# Copyright (c) 2020 vesoft inc. All rights reserved.
#
# This source code is licensed under Apache 2.0 License,
# attached with Common Clause Condition 1.0, found in the LICENSES directory.
Feature: Start From Any Node

  Background:
    Given a graph with space named "nba"

  Scenario: start from middle node, with prop index, with totally 2 steps
    When executing query:
      """
      MATCH (n)-[]-(m:player{name:"Kyle Anderson"})-[]-(l)
      RETURN n,m,l
      """
    Then the result should be, in any order, with relax comparison:
      | n                   | m                 | l                   |
      | ("Dejounte Murray") | ("Kyle Anderson") | ("Tony Parker")     |
      | ("Grizzlies")       | ("Kyle Anderson") | ("Tony Parker")     |
      | ("Spurs")           | ("Kyle Anderson") | ("Tony Parker")     |
      | ("Tony Parker")     | ("Kyle Anderson") | ("Dejounte Murray") |
      | ("Grizzlies")       | ("Kyle Anderson") | ("Dejounte Murray") |
      | ("Spurs")           | ("Kyle Anderson") | ("Dejounte Murray") |
      | ("Tony Parker")     | ("Kyle Anderson") | ("Grizzlies")       |
      | ("Dejounte Murray") | ("Kyle Anderson") | ("Grizzlies")       |
      | ("Spurs")           | ("Kyle Anderson") | ("Grizzlies")       |
      | ("Tony Parker")     | ("Kyle Anderson") | ("Spurs")           |
      | ("Dejounte Murray") | ("Kyle Anderson") | ("Spurs")           |
      | ("Grizzlies")       | ("Kyle Anderson") | ("Spurs")           |
    When executing query:
      """
      MATCH (n)-[]-(m:player)-[]-(l)
      WHERE m.name=="Kyle Anderson"
      RETURN n,m,l
      """
    Then the result should be, in any order, with relax comparison:
      | n                   | m                 | l                   |
      | ("Dejounte Murray") | ("Kyle Anderson") | ("Tony Parker")     |
      | ("Grizzlies")       | ("Kyle Anderson") | ("Tony Parker")     |
      | ("Spurs")           | ("Kyle Anderson") | ("Tony Parker")     |
      | ("Tony Parker")     | ("Kyle Anderson") | ("Dejounte Murray") |
      | ("Grizzlies")       | ("Kyle Anderson") | ("Dejounte Murray") |
      | ("Spurs")           | ("Kyle Anderson") | ("Dejounte Murray") |
      | ("Tony Parker")     | ("Kyle Anderson") | ("Grizzlies")       |
      | ("Dejounte Murray") | ("Kyle Anderson") | ("Grizzlies")       |
      | ("Spurs")           | ("Kyle Anderson") | ("Grizzlies")       |
      | ("Tony Parker")     | ("Kyle Anderson") | ("Spurs")           |
      | ("Dejounte Murray") | ("Kyle Anderson") | ("Spurs")           |
      | ("Grizzlies")       | ("Kyle Anderson") | ("Spurs")           |
    When executing query:
      """
      MATCH p = (n)-[]-(m:player{name:"Kyle Anderson"})-[]-(l)
      RETURN p
      """
    Then the result should be, in any order, with relax comparison:
      | p                                                                                 |
      | <("Dejounte Murray")-[:like@0]->("Kyle Anderson")<-[:teammate@0]-("Tony Parker")> |
      | <("Grizzlies")<-[:serve@0]-("Kyle Anderson")<-[:teammate@0]-("Tony Parker")>      |
      | <("Spurs")<-[:serve@0]-("Kyle Anderson")<-[:teammate@0]-("Tony Parker")>          |
      | <("Tony Parker")-[:teammate@0]->("Kyle Anderson")<-[:like@0]-("Dejounte Murray")> |
      | <("Grizzlies")<-[:serve@0]-("Kyle Anderson")<-[:like@0]-("Dejounte Murray")>      |
      | <("Spurs")<-[:serve@0]-("Kyle Anderson")<-[:like@0]-("Dejounte Murray")>          |
      | <("Tony Parker")-[:teammate@0]->("Kyle Anderson")-[:serve@0]->("Grizzlies")>      |
      | <("Dejounte Murray")-[:like@0]->("Kyle Anderson")-[:serve@0]->("Grizzlies")>      |
      | <("Spurs")<-[:serve@0]-("Kyle Anderson")-[:serve@0]->("Grizzlies")>               |
      | <("Tony Parker")-[:teammate@0]->("Kyle Anderson")-[:serve@0]->("Spurs")>          |
      | <("Dejounte Murray")-[:like@0]->("Kyle Anderson")-[:serve@0]->("Spurs")>          |
      | <("Grizzlies")<-[:serve@0]-("Kyle Anderson")-[:serve@0]->("Spurs")>               |
    When executing query:
      """
      MATCH (n)-[e1]-(m:player{name:"Kyle Anderson"})-[e2]-(l)
      RETURN *
      """
    Then the result should be, in any order, with relax comparison:
      | n                   | e1                                            | m                 | e2                                            | l                   |
      | ("Dejounte Murray") | [:like "Kyle Anderson"<-"Dejounte Murray" @0] | ("Kyle Anderson") | [:teammate "Kyle Anderson"<-"Tony Parker" @0] | ("Tony Parker")     |
      | ("Grizzlies")       | [:serve "Kyle Anderson"->"Grizzlies" @0]      | ("Kyle Anderson") | [:teammate "Kyle Anderson"<-"Tony Parker" @0] | ("Tony Parker")     |
      | ("Spurs")           | [:serve "Kyle Anderson"->"Spurs" @0]          | ("Kyle Anderson") | [:teammate "Kyle Anderson"<-"Tony Parker" @0] | ("Tony Parker")     |
      | ("Tony Parker")     | [:teammate "Kyle Anderson"<-"Tony Parker" @0] | ("Kyle Anderson") | [:like "Kyle Anderson"<-"Dejounte Murray" @0] | ("Dejounte Murray") |
      | ("Grizzlies")       | [:serve "Kyle Anderson"->"Grizzlies" @0]      | ("Kyle Anderson") | [:like "Kyle Anderson"<-"Dejounte Murray" @0] | ("Dejounte Murray") |
      | ("Spurs")           | [:serve "Kyle Anderson"->"Spurs" @0]          | ("Kyle Anderson") | [:like "Kyle Anderson"<-"Dejounte Murray" @0] | ("Dejounte Murray") |
      | ("Tony Parker")     | [:teammate "Kyle Anderson"<-"Tony Parker" @0] | ("Kyle Anderson") | [:serve "Kyle Anderson"->"Grizzlies" @0]      | ("Grizzlies")       |
      | ("Dejounte Murray") | [:like "Kyle Anderson"<-"Dejounte Murray" @0] | ("Kyle Anderson") | [:serve "Kyle Anderson"->"Grizzlies" @0]      | ("Grizzlies")       |
      | ("Spurs")           | [:serve "Kyle Anderson"->"Spurs" @0]          | ("Kyle Anderson") | [:serve "Kyle Anderson"->"Grizzlies" @0]      | ("Grizzlies")       |
      | ("Tony Parker")     | [:teammate "Kyle Anderson"<-"Tony Parker" @0] | ("Kyle Anderson") | [:serve "Kyle Anderson"->"Spurs" @0]          | ("Spurs")           |
      | ("Dejounte Murray") | [:like "Kyle Anderson"<-"Dejounte Murray" @0] | ("Kyle Anderson") | [:serve "Kyle Anderson"->"Spurs" @0]          | ("Spurs")           |
      | ("Grizzlies")       | [:serve "Kyle Anderson"->"Grizzlies" @0]      | ("Kyle Anderson") | [:serve "Kyle Anderson"->"Spurs" @0]          | ("Spurs")           |
    When executing query:
      """
      MATCH (n)-[e1]-(m:player{name:"Kyle Anderson"})-[e2]->(l)
      RETURN *
      """
    Then the result should be, in any order, with relax comparison:
      | n                   | e1                                            | m                 | e2                                       | l             |
      | ("Tony Parker")     | [:teammate "Kyle Anderson"<-"Tony Parker" @0] | ("Kyle Anderson") | [:serve "Kyle Anderson"->"Grizzlies" @0] | ("Grizzlies") |
      | ("Dejounte Murray") | [:like "Kyle Anderson"<-"Dejounte Murray" @0] | ("Kyle Anderson") | [:serve "Kyle Anderson"->"Grizzlies" @0] | ("Grizzlies") |
      | ("Spurs")           | [:serve "Kyle Anderson"->"Spurs" @0]          | ("Kyle Anderson") | [:serve "Kyle Anderson"->"Grizzlies" @0] | ("Grizzlies") |
      | ("Tony Parker")     | [:teammate "Kyle Anderson"<-"Tony Parker" @0] | ("Kyle Anderson") | [:serve "Kyle Anderson"->"Spurs" @0]     | ("Spurs")     |
      | ("Dejounte Murray") | [:like "Kyle Anderson"<-"Dejounte Murray" @0] | ("Kyle Anderson") | [:serve "Kyle Anderson"->"Spurs" @0]     | ("Spurs")     |
      | ("Grizzlies")       | [:serve "Kyle Anderson"->"Grizzlies" @0]      | ("Kyle Anderson") | [:serve "Kyle Anderson"->"Spurs" @0]     | ("Spurs")     |
    When executing query:
      """
      MATCH (n)-[e1]-(m:player{name:"Kyle Anderson"})<-[e2]-(l)
      RETURN *
      """
    Then the result should be, in any order, with relax comparison:
      | n                   | e1                                            | m                 | e2                                            | l                   |
      | ("Dejounte Murray") | [:like "Kyle Anderson"<-"Dejounte Murray" @0] | ("Kyle Anderson") | [:teammate "Kyle Anderson"<-"Tony Parker" @0] | ("Tony Parker")     |
      | ("Grizzlies")       | [:serve "Kyle Anderson"->"Grizzlies" @0]      | ("Kyle Anderson") | [:teammate "Kyle Anderson"<-"Tony Parker" @0] | ("Tony Parker")     |
      | ("Spurs")           | [:serve "Kyle Anderson"->"Spurs" @0]          | ("Kyle Anderson") | [:teammate "Kyle Anderson"<-"Tony Parker" @0] | ("Tony Parker")     |
      | ("Tony Parker")     | [:teammate "Kyle Anderson"<-"Tony Parker" @0] | ("Kyle Anderson") | [:like "Kyle Anderson"<-"Dejounte Murray" @0] | ("Dejounte Murray") |
      | ("Grizzlies")       | [:serve "Kyle Anderson"->"Grizzlies" @0]      | ("Kyle Anderson") | [:like "Kyle Anderson"<-"Dejounte Murray" @0] | ("Dejounte Murray") |
      | ("Spurs")           | [:serve "Kyle Anderson"->"Spurs" @0]          | ("Kyle Anderson") | [:like "Kyle Anderson"<-"Dejounte Murray" @0] | ("Dejounte Murray") |
    When executing query:
      """
      MATCH (n)-[e1]->(m:player{name:"Kyle Anderson"})-[e2]-(l)
      RETURN *
      """
    Then the result should be, in any order, with relax comparison:
      | n                   | e1                                            | m                 | e2                                            | l                   |
      | ("Dejounte Murray") | [:like "Kyle Anderson"<-"Dejounte Murray" @0] | ("Kyle Anderson") | [:teammate "Kyle Anderson"<-"Tony Parker" @0] | ("Tony Parker")     |
      | ("Tony Parker")     | [:teammate "Kyle Anderson"<-"Tony Parker" @0] | ("Kyle Anderson") | [:like "Kyle Anderson"<-"Dejounte Murray" @0] | ("Dejounte Murray") |
      | ("Tony Parker")     | [:teammate "Kyle Anderson"<-"Tony Parker" @0] | ("Kyle Anderson") | [:serve "Kyle Anderson"->"Grizzlies" @0]      | ("Grizzlies")       |
      | ("Dejounte Murray") | [:like "Kyle Anderson"<-"Dejounte Murray" @0] | ("Kyle Anderson") | [:serve "Kyle Anderson"->"Grizzlies" @0]      | ("Grizzlies")       |
      | ("Tony Parker")     | [:teammate "Kyle Anderson"<-"Tony Parker" @0] | ("Kyle Anderson") | [:serve "Kyle Anderson"->"Spurs" @0]          | ("Spurs")           |
      | ("Dejounte Murray") | [:like "Kyle Anderson"<-"Dejounte Murray" @0] | ("Kyle Anderson") | [:serve "Kyle Anderson"->"Spurs" @0]          | ("Spurs")           |
    When executing query:
      """
      MATCH (n)-[e1]->(m:player{name:"Kyle Anderson"})-[e2]->(l)
      RETURN *
      """
    Then the result should be, in any order, with relax comparison:
      | n                   | e1                                            | m                 | e2                                       | l             |
      | ("Tony Parker")     | [:teammate "Kyle Anderson"<-"Tony Parker" @0] | ("Kyle Anderson") | [:serve "Kyle Anderson"->"Grizzlies" @0] | ("Grizzlies") |
      | ("Dejounte Murray") | [:like "Kyle Anderson"<-"Dejounte Murray" @0] | ("Kyle Anderson") | [:serve "Kyle Anderson"->"Grizzlies" @0] | ("Grizzlies") |
      | ("Tony Parker")     | [:teammate "Kyle Anderson"<-"Tony Parker" @0] | ("Kyle Anderson") | [:serve "Kyle Anderson"->"Spurs" @0]     | ("Spurs")     |
      | ("Dejounte Murray") | [:like "Kyle Anderson"<-"Dejounte Murray" @0] | ("Kyle Anderson") | [:serve "Kyle Anderson"->"Spurs" @0]     | ("Spurs")     |
    When executing query:
      """
      MATCH (n)-[e1]->(m:player{name:"Kyle Anderson"})<-[e2]-(l)
      RETURN *
      """
    Then the result should be, in any order, with relax comparison:
      | n                   | e1                                            | m                 | e2                                            | l                   |
      | ("Dejounte Murray") | [:like "Kyle Anderson"<-"Dejounte Murray" @0] | ("Kyle Anderson") | [:teammate "Kyle Anderson"<-"Tony Parker" @0] | ("Tony Parker")     |
      | ("Tony Parker")     | [:teammate "Kyle Anderson"<-"Tony Parker" @0] | ("Kyle Anderson") | [:like "Kyle Anderson"<-"Dejounte Murray" @0] | ("Dejounte Murray") |
    When executing query:
      """
      MATCH (n)<-[e1]-(m:player{name:"Kyle Anderson"})-[e2]-(l)
      RETURN *
      """
    Then the result should be, in any order, with relax comparison:
      | n             | e1                                       | m                 | e2                                            | l                   |
      | ("Grizzlies") | [:serve "Kyle Anderson"->"Grizzlies" @0] | ("Kyle Anderson") | [:teammate "Kyle Anderson"<-"Tony Parker" @0] | ("Tony Parker")     |
      | ("Spurs")     | [:serve "Kyle Anderson"->"Spurs" @0]     | ("Kyle Anderson") | [:teammate "Kyle Anderson"<-"Tony Parker" @0] | ("Tony Parker")     |
      | ("Grizzlies") | [:serve "Kyle Anderson"->"Grizzlies" @0] | ("Kyle Anderson") | [:like "Kyle Anderson"<-"Dejounte Murray" @0] | ("Dejounte Murray") |
      | ("Spurs")     | [:serve "Kyle Anderson"->"Spurs" @0]     | ("Kyle Anderson") | [:like "Kyle Anderson"<-"Dejounte Murray" @0] | ("Dejounte Murray") |
      | ("Spurs")     | [:serve "Kyle Anderson"->"Spurs" @0]     | ("Kyle Anderson") | [:serve "Kyle Anderson"->"Grizzlies" @0]      | ("Grizzlies")       |
      | ("Grizzlies") | [:serve "Kyle Anderson"->"Grizzlies" @0] | ("Kyle Anderson") | [:serve "Kyle Anderson"->"Spurs" @0]          | ("Spurs")           |
    When executing query:
      """
      MATCH (n)<-[e1]-(m:player{name:"Kyle Anderson"})-[e2]->(l)
      RETURN *
      """
    Then the result should be, in any order, with relax comparison:
      | n             | e1                                       | m                 | e2                                       | l             |
      | ("Spurs")     | [:serve "Kyle Anderson"->"Spurs" @0]     | ("Kyle Anderson") | [:serve "Kyle Anderson"->"Grizzlies" @0] | ("Grizzlies") |
      | ("Grizzlies") | [:serve "Kyle Anderson"->"Grizzlies" @0] | ("Kyle Anderson") | [:serve "Kyle Anderson"->"Spurs" @0]     | ("Spurs")     |
    When executing query:
      """
      MATCH (n)<-[e1]-(m:player{name:"Kyle Anderson"})<-[e2]-(l)
      RETURN *
      """
    Then the result should be, in any order, with relax comparison:
      | n             | e1                                       | m                 | e2                                            | l                   |
      | ("Grizzlies") | [:serve "Kyle Anderson"->"Grizzlies" @0] | ("Kyle Anderson") | [:teammate "Kyle Anderson"<-"Tony Parker" @0] | ("Tony Parker")     |
      | ("Spurs")     | [:serve "Kyle Anderson"->"Spurs" @0]     | ("Kyle Anderson") | [:teammate "Kyle Anderson"<-"Tony Parker" @0] | ("Tony Parker")     |
      | ("Grizzlies") | [:serve "Kyle Anderson"->"Grizzlies" @0] | ("Kyle Anderson") | [:like "Kyle Anderson"<-"Dejounte Murray" @0] | ("Dejounte Murray") |
      | ("Spurs")     | [:serve "Kyle Anderson"->"Spurs" @0]     | ("Kyle Anderson") | [:like "Kyle Anderson"<-"Dejounte Murray" @0] | ("Dejounte Murray") |

  Scenario: start from middle node, with prop index, with totally 3 steps
    When executing query:
      """
      MATCH p = (n)-[]-(m:player{name:"Kyle Anderson"})-[]-(l)-[]-(k)
      RETURN p
      | YIELD count(*) AS count
      """
    Then the result should be, in any order, with relax comparison:
      | count |
      | 141   |
    When executing query:
      """
      MATCH p = (n)-[]-(m:player{name:"Kyle Anderson"})-[]-(l)-[]-(k)
      WHERE k.name == "Marc Gasol"
      RETURN n, m, l, k
      """
    Then the result should be, in any order, with relax comparison:
      | n                   | m                 | l             | k              |
      | ("Tony Parker")     | ("Kyle Anderson") | ("Grizzlies") | ("Marc Gasol") |
      | ("Dejounte Murray") | ("Kyle Anderson") | ("Grizzlies") | ("Marc Gasol") |
      | ("Spurs")           | ("Kyle Anderson") | ("Grizzlies") | ("Marc Gasol") |
    When executing query:
      """
      MATCH p = (n)-[]-(m:player{name:"Kyle Anderson"})-[]-(l)-[]-(k)
      WHERE k.name == "Marc Gasol"
      RETURN p
      """
    Then the result should be, in any order, with relax comparison:
      | p                                                                                                       |
      | <("Tony Parker")-[:teammate@0]->("Kyle Anderson")-[:serve@0]->("Grizzlies")<-[:serve@0]-("Marc Gasol")> |
      | <("Dejounte Murray")-[:like@0]->("Kyle Anderson")-[:serve@0]->("Grizzlies")<-[:serve@0]-("Marc Gasol")> |
      | <("Spurs")<-[:serve@0]-("Kyle Anderson")-[:serve@0]->("Grizzlies")<-[:serve@0]-("Marc Gasol")>          |
    When executing query:
      """
      MATCH p = ()-[e1]-(m:player{name:"Kyle Anderson"})-[e2]-()-[e3]-(k)
      WHERE k.name == "Marc Gasol"
      RETURN e1, e2, e3
      """
    Then the result should be, in any order, with relax comparison:
      | e1                                            | e2                                       | e3                                    |
      | [:teammate "Kyle Anderson"<-"Tony Parker" @0] | [:serve "Kyle Anderson"->"Grizzlies" @0] | [:serve "Grizzlies"<-"Marc Gasol" @0] |
      | [:like "Kyle Anderson"<-"Dejounte Murray" @0] | [:serve "Kyle Anderson"->"Grizzlies" @0] | [:serve "Grizzlies"<-"Marc Gasol" @0] |
      | [:serve "Kyle Anderson"->"Spurs" @0]          | [:serve "Kyle Anderson"->"Grizzlies" @0] | [:serve "Grizzlies"<-"Marc Gasol" @0] |
    When executing query:
      """
      MATCH p = (k)-[]-(n)-[]-(m:player{name:"Kobe Bryant"})-[]-(l)
      RETURN p
      | YIELD count(*) AS count
      """
    Then the result should be, in any order, with relax comparison:
      | count |
      | 46    |
    When executing query:
      """
      MATCH p = (k)-[]-(n)-[]-(m:player{name:"Kobe Bryant"})-[]-(l)
      WHERE l.name == "Lakers"
      RETURN k, n, m, l
      """
    Then the result should be, in any order, with relax comparison:
      | k                | n                 | m               | l          |
      | ("Grant Hill")   | ("Tracy McGrady") | ("Kobe Bryant") | ("Lakers") |
      | ("Vince Carter") | ("Tracy McGrady") | ("Kobe Bryant") | ("Lakers") |
      | ("Yao Ming")     | ("Tracy McGrady") | ("Kobe Bryant") | ("Lakers") |
      | ("Grant Hill")   | ("Tracy McGrady") | ("Kobe Bryant") | ("Lakers") |
      | ("Rudy Gay")     | ("Tracy McGrady") | ("Kobe Bryant") | ("Lakers") |
      | ("Magic")        | ("Tracy McGrady") | ("Kobe Bryant") | ("Lakers") |
      | ("Raptors")      | ("Tracy McGrady") | ("Kobe Bryant") | ("Lakers") |
      | ("Rockets")      | ("Tracy McGrady") | ("Kobe Bryant") | ("Lakers") |
      | ("Spurs")        | ("Tracy McGrady") | ("Kobe Bryant") | ("Lakers") |
      | ("Marc Gasol")   | ("Paul Gasol")    | ("Kobe Bryant") | ("Lakers") |
      | ("Marc Gasol")   | ("Paul Gasol")    | ("Kobe Bryant") | ("Lakers") |
      | ("Bucks")        | ("Paul Gasol")    | ("Kobe Bryant") | ("Lakers") |
      | ("Bulls")        | ("Paul Gasol")    | ("Kobe Bryant") | ("Lakers") |
      | ("Grizzlies")    | ("Paul Gasol")    | ("Kobe Bryant") | ("Lakers") |
      | ("Lakers")       | ("Paul Gasol")    | ("Kobe Bryant") | ("Lakers") |
      | ("Spurs")        | ("Paul Gasol")    | ("Kobe Bryant") | ("Lakers") |
    When executing query:
      """
      MATCH p = (k)-[]-(n)-[]-(m:player{name:"Kobe Bryant"})-[]-(l)
      WHERE l.name == "Lakers"
      RETURN p
      """
    Then the result should be, in any order, with relax comparison:
      | p                                                                                                 |
      | <("Grant Hill")-[:like@0]->("Tracy McGrady")-[:like@0]->("Kobe Bryant")-[:serve@0]->("Lakers")>   |
      | <("Vince Carter")-[:like@0]->("Tracy McGrady")-[:like@0]->("Kobe Bryant")-[:serve@0]->("Lakers")> |
      | <("Yao Ming")-[:like@0]->("Tracy McGrady")-[:like@0]->("Kobe Bryant")-[:serve@0]->("Lakers")>     |
      | <("Grant Hill")<-[:like@0]-("Tracy McGrady")-[:like@0]->("Kobe Bryant")-[:serve@0]->("Lakers")>   |
      | <("Rudy Gay")<-[:like@0]-("Tracy McGrady")-[:like@0]->("Kobe Bryant")-[:serve@0]->("Lakers")>     |
      | <("Magic")<-[:serve@0]-("Tracy McGrady")-[:like@0]->("Kobe Bryant")-[:serve@0]->("Lakers")>       |
      | <("Raptors")<-[:serve@0]-("Tracy McGrady")-[:like@0]->("Kobe Bryant")-[:serve@0]->("Lakers")>     |
      | <("Rockets")<-[:serve@0]-("Tracy McGrady")-[:like@0]->("Kobe Bryant")-[:serve@0]->("Lakers")>     |
      | <("Spurs")<-[:serve@0]-("Tracy McGrady")-[:like@0]->("Kobe Bryant")-[:serve@0]->("Lakers")>       |
      | <("Marc Gasol")-[:like@0]->("Paul Gasol")-[:like@0]->("Kobe Bryant")-[:serve@0]->("Lakers")>      |
      | <("Marc Gasol")<-[:like@0]-("Paul Gasol")-[:like@0]->("Kobe Bryant")-[:serve@0]->("Lakers")>      |
      | <("Bucks")<-[:serve@0]-("Paul Gasol")-[:like@0]->("Kobe Bryant")-[:serve@0]->("Lakers")>          |
      | <("Bulls")<-[:serve@0]-("Paul Gasol")-[:like@0]->("Kobe Bryant")-[:serve@0]->("Lakers")>          |
      | <("Grizzlies")<-[:serve@0]-("Paul Gasol")-[:like@0]->("Kobe Bryant")-[:serve@0]->("Lakers")>      |
      | <("Lakers")<-[:serve@0]-("Paul Gasol")-[:like@0]->("Kobe Bryant")-[:serve@0]->("Lakers")>         |
      | <("Spurs")<-[:serve@0]-("Paul Gasol")-[:like@0]->("Kobe Bryant")-[:serve@0]->("Lakers")>          |
    When executing query:
      """
      MATCH ()-[e1]-()-[e2]-(:player{name:"Kobe Bryant"})-[e3]-(l)
      WHERE l.name == "Lakers"
      RETURN e1, e2, e3
      """
    Then the result should be, in any order, with relax comparison:
      | e1                                         | e2                                        | e3                                  |
      | [:like "Tracy McGrady"<-"Grant Hill" @0]   | [:like "Kobe Bryant"<-"Tracy McGrady" @0] | [:serve "Kobe Bryant"->"Lakers" @0] |
      | [:like "Tracy McGrady"<-"Vince Carter" @0] | [:like "Kobe Bryant"<-"Tracy McGrady" @0] | [:serve "Kobe Bryant"->"Lakers" @0] |
      | [:like "Tracy McGrady"<-"Yao Ming" @0]     | [:like "Kobe Bryant"<-"Tracy McGrady" @0] | [:serve "Kobe Bryant"->"Lakers" @0] |
      | [:like "Tracy McGrady"->"Grant Hill" @0]   | [:like "Kobe Bryant"<-"Tracy McGrady" @0] | [:serve "Kobe Bryant"->"Lakers" @0] |
      | [:like "Tracy McGrady"->"Rudy Gay" @0]     | [:like "Kobe Bryant"<-"Tracy McGrady" @0] | [:serve "Kobe Bryant"->"Lakers" @0] |
      | [:serve "Tracy McGrady"->"Magic" @0]       | [:like "Kobe Bryant"<-"Tracy McGrady" @0] | [:serve "Kobe Bryant"->"Lakers" @0] |
      | [:serve "Tracy McGrady"->"Raptors" @0]     | [:like "Kobe Bryant"<-"Tracy McGrady" @0] | [:serve "Kobe Bryant"->"Lakers" @0] |
      | [:serve "Tracy McGrady"->"Rockets" @0]     | [:like "Kobe Bryant"<-"Tracy McGrady" @0] | [:serve "Kobe Bryant"->"Lakers" @0] |
      | [:serve "Tracy McGrady"->"Spurs" @0]       | [:like "Kobe Bryant"<-"Tracy McGrady" @0] | [:serve "Kobe Bryant"->"Lakers" @0] |
      | [:like "Paul Gasol"<-"Marc Gasol" @0]      | [:like "Kobe Bryant"<-"Paul Gasol" @0]    | [:serve "Kobe Bryant"->"Lakers" @0] |
      | [:like "Paul Gasol"->"Marc Gasol" @0]      | [:like "Kobe Bryant"<-"Paul Gasol" @0]    | [:serve "Kobe Bryant"->"Lakers" @0] |
      | [:serve "Paul Gasol"->"Bucks" @0]          | [:like "Kobe Bryant"<-"Paul Gasol" @0]    | [:serve "Kobe Bryant"->"Lakers" @0] |
      | [:serve "Paul Gasol"->"Bulls" @0]          | [:like "Kobe Bryant"<-"Paul Gasol" @0]    | [:serve "Kobe Bryant"->"Lakers" @0] |
      | [:serve "Paul Gasol"->"Grizzlies" @0]      | [:like "Kobe Bryant"<-"Paul Gasol" @0]    | [:serve "Kobe Bryant"->"Lakers" @0] |
      | [:serve "Paul Gasol"->"Lakers" @0]         | [:like "Kobe Bryant"<-"Paul Gasol" @0]    | [:serve "Kobe Bryant"->"Lakers" @0] |
      | [:serve "Paul Gasol"->"Spurs" @0]          | [:like "Kobe Bryant"<-"Paul Gasol" @0]    | [:serve "Kobe Bryant"->"Lakers" @0] |

  Scenario: start from middle node, with prop index, with totally 4 steps
    When executing query:
      """
      MATCH p = ()-[]-(n)-[]-(m:player{name:"Kobe Bryant"})-[]-(l)-[]-(k)
      RETURN p
      | YIELD count(*) AS count
      """
    Then the result should be, in any order, with relax comparison:
      | count |
      | 348   |
    When executing query:
      """
      MATCH p = ()-[]-(n)-[]-(m:player{name:"Kobe Bryant"})-[]-(l)-[]-(k)
      WHERE k.name == "Paul Gasol"
      RETURN p
      """
    Then the result should be, in any order, with relax comparison:
      | p                                                                                                                            |
      | <("Grant Hill")-[:like@0]->("Tracy McGrady")-[:like@0]->("Kobe Bryant")-[:serve@0]->("Lakers")<-[:serve@0]-("Paul Gasol")>   |
      | <("Vince Carter")-[:like@0]->("Tracy McGrady")-[:like@0]->("Kobe Bryant")-[:serve@0]->("Lakers")<-[:serve@0]-("Paul Gasol")> |
      | <("Yao Ming")-[:like@0]->("Tracy McGrady")-[:like@0]->("Kobe Bryant")-[:serve@0]->("Lakers")<-[:serve@0]-("Paul Gasol")>     |
      | <("Grant Hill")<-[:like@0]-("Tracy McGrady")-[:like@0]->("Kobe Bryant")-[:serve@0]->("Lakers")<-[:serve@0]-("Paul Gasol")>   |
      | <("Rudy Gay")<-[:like@0]-("Tracy McGrady")-[:like@0]->("Kobe Bryant")-[:serve@0]->("Lakers")<-[:serve@0]-("Paul Gasol")>     |
      | <("Magic")<-[:serve@0]-("Tracy McGrady")-[:like@0]->("Kobe Bryant")-[:serve@0]->("Lakers")<-[:serve@0]-("Paul Gasol")>       |
      | <("Raptors")<-[:serve@0]-("Tracy McGrady")-[:like@0]->("Kobe Bryant")-[:serve@0]->("Lakers")<-[:serve@0]-("Paul Gasol")>     |
      | <("Rockets")<-[:serve@0]-("Tracy McGrady")-[:like@0]->("Kobe Bryant")-[:serve@0]->("Lakers")<-[:serve@0]-("Paul Gasol")>     |
      | <("Spurs")<-[:serve@0]-("Tracy McGrady")-[:like@0]->("Kobe Bryant")-[:serve@0]->("Lakers")<-[:serve@0]-("Paul Gasol")>       |
      | <("Marc Gasol")-[:like@0]->("Paul Gasol")-[:like@0]->("Kobe Bryant")-[:serve@0]->("Lakers")<-[:serve@0]-("Paul Gasol")>      |
      | <("Marc Gasol")<-[:like@0]-("Paul Gasol")-[:like@0]->("Kobe Bryant")-[:serve@0]->("Lakers")<-[:serve@0]-("Paul Gasol")>      |
      | <("Bucks")<-[:serve@0]-("Paul Gasol")-[:like@0]->("Kobe Bryant")-[:serve@0]->("Lakers")<-[:serve@0]-("Paul Gasol")>          |
      | <("Bulls")<-[:serve@0]-("Paul Gasol")-[:like@0]->("Kobe Bryant")-[:serve@0]->("Lakers")<-[:serve@0]-("Paul Gasol")>          |
      | <("Grizzlies")<-[:serve@0]-("Paul Gasol")-[:like@0]->("Kobe Bryant")-[:serve@0]->("Lakers")<-[:serve@0]-("Paul Gasol" )>     |
      | <("Spurs")<-[:serve@0]-("Paul Gasol")-[:like@0]->("Kobe Bryant")-[:serve@0]->("Lakers")<-[:serve@0]-("Paul Gasol")>          |
    When executing query:
      """
      MATCH (i)-[]-(n)-[]-(m:player{name:"Kobe Bryant"})-[]-(l)-[]-(k)
      WHERE k.name == "Paul Gasol"
      RETURN i, n, m, l, k
      """
    Then the result should be, in any order, with relax comparison:
      | i                | n                 | m               | l          | k              |
      | ("Grant Hill")   | ("Tracy McGrady") | ("Kobe Bryant") | ("Lakers") | ("Paul Gasol") |
      | ("Vince Carter") | ("Tracy McGrady") | ("Kobe Bryant") | ("Lakers") | ("Paul Gasol") |
      | ("Yao Ming")     | ("Tracy McGrady") | ("Kobe Bryant") | ("Lakers") | ("Paul Gasol") |
      | ("Grant Hill")   | ("Tracy McGrady") | ("Kobe Bryant") | ("Lakers") | ("Paul Gasol") |
      | ("Rudy Gay")     | ("Tracy McGrady") | ("Kobe Bryant") | ("Lakers") | ("Paul Gasol") |
      | ("Magic")        | ("Tracy McGrady") | ("Kobe Bryant") | ("Lakers") | ("Paul Gasol") |
      | ("Raptors")      | ("Tracy McGrady") | ("Kobe Bryant") | ("Lakers") | ("Paul Gasol") |
      | ("Rockets")      | ("Tracy McGrady") | ("Kobe Bryant") | ("Lakers") | ("Paul Gasol") |
      | ("Spurs")        | ("Tracy McGrady") | ("Kobe Bryant") | ("Lakers") | ("Paul Gasol") |
      | ("Marc Gasol")   | ("Paul Gasol")    | ("Kobe Bryant") | ("Lakers") | ("Paul Gasol") |
      | ("Marc Gasol")   | ("Paul Gasol")    | ("Kobe Bryant") | ("Lakers") | ("Paul Gasol") |
      | ("Bucks")        | ("Paul Gasol")    | ("Kobe Bryant") | ("Lakers") | ("Paul Gasol") |
      | ("Bulls")        | ("Paul Gasol")    | ("Kobe Bryant") | ("Lakers") | ("Paul Gasol") |
      | ("Grizzlies")    | ("Paul Gasol")    | ("Kobe Bryant") | ("Lakers") | ("Paul Gasol") |
      | ("Spurs")        | ("Paul Gasol")    | ("Kobe Bryant") | ("Lakers") | ("Paul Gasol") |
    When executing query:
      """
      MATCH ()-[e1]-()-[e2]-(:player{name:"Kobe Bryant"})-[e3]-()-[e4]-(k)
      WHERE k.name == "Paul Gasol"
      RETURN e1, e2, e3, e4
      """
    Then the result should be, in any order, with relax comparison:
      | e1                                         | e2                                        | e3                                  | e4                                 |
      | [:like "Tracy McGrady"<-"Grant Hill" @0]   | [:like "Kobe Bryant"<-"Tracy McGrady" @0] | [:serve "Kobe Bryant"->"Lakers" @0] | [:serve "Lakers"<-"Paul Gasol" @0] |
      | [:like "Tracy McGrady"<-"Vince Carter" @0] | [:like "Kobe Bryant"<-"Tracy McGrady" @0] | [:serve "Kobe Bryant"->"Lakers" @0] | [:serve "Lakers"<-"Paul Gasol" @0] |
      | [:like "Tracy McGrady"<-"Yao Ming" @0]     | [:like "Kobe Bryant"<-"Tracy McGrady" @0] | [:serve "Kobe Bryant"->"Lakers" @0] | [:serve "Lakers"<-"Paul Gasol" @0] |
      | [:like "Tracy McGrady"->"Grant Hill" @0]   | [:like "Kobe Bryant"<-"Tracy McGrady" @0] | [:serve "Kobe Bryant"->"Lakers" @0] | [:serve "Lakers"<-"Paul Gasol" @0] |
      | [:like "Tracy McGrady"->"Rudy Gay" @0]     | [:like "Kobe Bryant"<-"Tracy McGrady" @0] | [:serve "Kobe Bryant"->"Lakers" @0] | [:serve "Lakers"<-"Paul Gasol" @0] |
      | [:serve "Tracy McGrady"->"Magic" @0]       | [:like "Kobe Bryant"<-"Tracy McGrady" @0] | [:serve "Kobe Bryant"->"Lakers" @0] | [:serve "Lakers"<-"Paul Gasol" @0] |
      | [:serve "Tracy McGrady"->"Raptors" @0]     | [:like "Kobe Bryant"<-"Tracy McGrady" @0] | [:serve "Kobe Bryant"->"Lakers" @0] | [:serve "Lakers"<-"Paul Gasol" @0] |
      | [:serve "Tracy McGrady"->"Rockets" @0]     | [:like "Kobe Bryant"<-"Tracy McGrady" @0] | [:serve "Kobe Bryant"->"Lakers" @0] | [:serve "Lakers"<-"Paul Gasol" @0] |
      | [:serve "Tracy McGrady"->"Spurs" @0]       | [:like "Kobe Bryant"<-"Tracy McGrady" @0] | [:serve "Kobe Bryant"->"Lakers" @0] | [:serve "Lakers"<-"Paul Gasol" @0] |
      | [:like "Paul Gasol"<-"Marc Gasol" @0]      | [:like "Kobe Bryant"<-"Paul Gasol" @0]    | [:serve "Kobe Bryant"->"Lakers" @0] | [:serve "Lakers"<-"Paul Gasol" @0] |
      | [:like "Paul Gasol"->"Marc Gasol" @0]      | [:like "Kobe Bryant"<-"Paul Gasol" @0]    | [:serve "Kobe Bryant"->"Lakers" @0] | [:serve "Lakers"<-"Paul Gasol" @0] |
      | [:serve "Paul Gasol"->"Bucks" @0]          | [:like "Kobe Bryant"<-"Paul Gasol" @0]    | [:serve "Kobe Bryant"->"Lakers" @0] | [:serve "Lakers"<-"Paul Gasol" @0] |
      | [:serve "Paul Gasol"->"Bulls" @0]          | [:like "Kobe Bryant"<-"Paul Gasol" @0]    | [:serve "Kobe Bryant"->"Lakers" @0] | [:serve "Lakers"<-"Paul Gasol" @0] |
      | [:serve "Paul Gasol"->"Grizzlies" @0]      | [:like "Kobe Bryant"<-"Paul Gasol" @0]    | [:serve "Kobe Bryant"->"Lakers" @0] | [:serve "Lakers"<-"Paul Gasol" @0] |
      | [:serve "Paul Gasol"->"Spurs" @0]          | [:like "Kobe Bryant"<-"Paul Gasol" @0]    | [:serve "Kobe Bryant"->"Lakers" @0] | [:serve "Lakers"<-"Paul Gasol" @0] |

  Scenario: start from end node, with prop index, with totally 1 steps
    When executing query:
      """
      MATCH (n)-[]-(m:player{name:"Kyle Anderson"})
      RETURN n, m
      """
    Then the result should be, in any order, with relax comparison:
      | n                   | m                 |
      | ("Tony Parker")     | ("Kyle Anderson") |
      | ("Dejounte Murray") | ("Kyle Anderson") |
      | ("Grizzlies")       | ("Kyle Anderson") |
      | ("Spurs")           | ("Kyle Anderson") |
    When executing query:
      """
      MATCH (n)-[]-(m:player{name:"Kyle Anderson"})
      RETURN *
      """
    Then the result should be, in any order, with relax comparison:
      | n                   | m                 |
      | ("Tony Parker")     | ("Kyle Anderson") |
      | ("Dejounte Murray") | ("Kyle Anderson") |
      | ("Grizzlies")       | ("Kyle Anderson") |
      | ("Spurs")           | ("Kyle Anderson") |

  Scenario: start from end node, with prop index, with totally 2 steps
    When executing query:
      """
      MATCH p = (l)-[]-(n)-[]-(m:player{name:"Stephen Curry"})
      RETURN p
      """
    Then the result should be, in any order, with relax comparison:
      | p                                                                              |
      | <("Warriors")<-[:serve@0]-("Klay Thompson")-[:like@0]->("Stephen Curry")>      |
      | <("Amar'e Stoudemire")-[:like@0]->("Steve Nash")-[:like@0]->("Stephen Curry")> |
      | <("Dirk Nowitzki")-[:like@0]->("Steve Nash")-[:like@0]->("Stephen Curry")>     |
      | <("Jason Kidd")-[:like@0]->("Steve Nash")-[:like@0]->("Stephen Curry")>        |
      | <("Amar'e Stoudemire")<-[:like@0]-("Steve Nash")-[:like@0]->("Stephen Curry")> |
      | <("Dirk Nowitzki")<-[:like@0]-("Steve Nash")-[:like@0]->("Stephen Curry")>     |
      | <("Jason Kidd")<-[:like@0]-("Steve Nash")-[:like@0]->("Stephen Curry")>        |
      | <("Lakers")<-[:serve@0]-("Steve Nash")-[:like@0]->("Stephen Curry")>           |
      | <("Mavericks")<-[:serve@0]-("Steve Nash")-[:like@0]->("Stephen Curry")>        |
      | <("Suns")<-[:serve@0]-("Steve Nash")-[:like@0]->("Stephen Curry")>             |
      | <("Suns")<-[:serve@1]-("Steve Nash")-[:like@0]->("Stephen Curry")>             |
      | <("David West")-[:serve@0]->("Warriors")<-[:serve@0]-("Stephen Curry")>        |
      | <("JaVale McGee")-[:serve@0]->("Warriors")<-[:serve@0]-("Stephen Curry")>      |
      | <("Kevin Durant")-[:serve@0]->("Warriors")<-[:serve@0]-("Stephen Curry")>      |
      | <("Klay Thompson")-[:serve@0]->("Warriors")<-[:serve@0]-("Stephen Curry")>     |
      | <("Marco Belinelli")-[:serve@0]->("Warriors")<-[:serve@0]-("Stephen Curry")>   |
    When executing query:
      """
      MATCH (l)-[]-(n)-[]-(m:player{name:"Stephen Curry"})
      RETURN l, n, m
      """
    Then the result should be, in any order, with relax comparison:
      | l                     | n                 | m                 |
      | ("Warriors")          | ("Klay Thompson") | ("Stephen Curry") |
      | ("Amar'e Stoudemire") | ("Steve Nash")    | ("Stephen Curry") |
      | ("Dirk Nowitzki")     | ("Steve Nash")    | ("Stephen Curry") |
      | ("Jason Kidd")        | ("Steve Nash")    | ("Stephen Curry") |
      | ("Amar'e Stoudemire") | ("Steve Nash")    | ("Stephen Curry") |
      | ("Dirk Nowitzki")     | ("Steve Nash")    | ("Stephen Curry") |
      | ("Jason Kidd")        | ("Steve Nash")    | ("Stephen Curry") |
      | ("Lakers")            | ("Steve Nash")    | ("Stephen Curry") |
      | ("Mavericks")         | ("Steve Nash")    | ("Stephen Curry") |
      | ("Suns")              | ("Steve Nash")    | ("Stephen Curry") |
      | ("Suns")              | ("Steve Nash")    | ("Stephen Curry") |
      | ("David West")        | ("Warriors")      | ("Stephen Curry") |
      | ("JaVale McGee")      | ("Warriors")      | ("Stephen Curry") |
      | ("Kevin Durant")      | ("Warriors")      | ("Stephen Curry") |
      | ("Klay Thompson")     | ("Warriors")      | ("Stephen Curry") |
      | ("Marco Belinelli")   | ("Warriors")      | ("Stephen Curry") |
    When executing query:
      """
      MATCH ()-[e1]-()-[e2]-(:player{name:"Stephen Curry"})
      RETURN e1, e2
      """
    Then the result should be, in any order, with relax comparison:
      | e1                                           | e2                                          |
      | [:serve "Klay Thompson"->"Warriors" @0]      | [:like "Stephen Curry"<-"Klay Thompson" @0] |
      | [:like "Steve Nash"<-"Amar'e Stoudemire" @0] | [:like "Stephen Curry"<-"Steve Nash" @0]    |
      | [:like "Steve Nash"<-"Dirk Nowitzki" @0]     | [:like "Stephen Curry"<-"Steve Nash" @0]    |
      | [:like "Steve Nash"<-"Jason Kidd" @0]        | [:like "Stephen Curry"<-"Steve Nash" @0]    |
      | [:like "Steve Nash"->"Amar'e Stoudemire" @0] | [:like "Stephen Curry"<-"Steve Nash" @0]    |
      | [:like "Steve Nash"->"Dirk Nowitzki" @0]     | [:like "Stephen Curry"<-"Steve Nash" @0]    |
      | [:like "Steve Nash"->"Jason Kidd" @0]        | [:like "Stephen Curry"<-"Steve Nash" @0]    |
      | [:serve "Steve Nash"->"Lakers" @0]           | [:like "Stephen Curry"<-"Steve Nash" @0]    |
      | [:serve "Steve Nash"->"Mavericks" @0]        | [:like "Stephen Curry"<-"Steve Nash" @0]    |
      | [:serve "Steve Nash"->"Suns" @0]             | [:like "Stephen Curry"<-"Steve Nash" @0]    |
      | [:serve "Steve Nash"->"Suns" @1]             | [:like "Stephen Curry"<-"Steve Nash" @0]    |
      | [:serve "Warriors"<-"David West" @0]         | [:serve "Stephen Curry"->"Warriors" @0]     |
      | [:serve "Warriors"<-"JaVale McGee" @0]       | [:serve "Stephen Curry"->"Warriors" @0]     |
      | [:serve "Warriors"<-"Kevin Durant" @0]       | [:serve "Stephen Curry"->"Warriors" @0]     |
      | [:serve "Warriors"<-"Klay Thompson" @0]      | [:serve "Stephen Curry"->"Warriors" @0]     |
      | [:serve "Warriors"<-"Marco Belinelli" @0]    | [:serve "Stephen Curry"->"Warriors" @0]     |

  Scenario: start from middle node, with prop index, with upto 3 steps
    When executing query:
      """
      MATCH (n)-[]-(m:player{name:"Kyle Anderson"})-[*1..2]-(l)
      RETURN n,m,l
      | YIELD count(*) AS count
      """
    Then the result should be, in any order, with relax comparison:
      | count |
      | 153   |

  Scenario: start from middle node, with prop index, with upto 4 steps
    When executing query:
      """
      MATCH (n)-[*1..2]-(m:player{name:"Kyle Anderson"})-[*1..2]-(l)
      RETURN n,m,l
      | YIELD count(*) AS count
      """
    Then the result should be, in any order, with relax comparison:
      | count |
      | 1846  |
    When executing query:
      """
      MATCH p = (n)-[*1..2]-(m:player{name:"Kyle Anderson"})-[]-(l)-[]-(k)
      RETURN p
      | YIELD count(*) AS count
      """
    Then the result should be, in any order, with relax comparison:
      | count |
      | 1693  |

  Scenario: start from middle node, with vertex id, with totally 2 steps
    When executing query:
      """
      MATCH (n)-[]-(m)-[]-(l)
      WHERE id(m)=='Kyle Anderson'
      RETURN n,m,l
      """
    Then the result should be, in any order, with relax comparison:
      | n                   | m                 | l                   |
      | ("Dejounte Murray") | ("Kyle Anderson") | ("Tony Parker")     |
      | ("Grizzlies")       | ("Kyle Anderson") | ("Tony Parker")     |
      | ("Spurs")           | ("Kyle Anderson") | ("Tony Parker")     |
      | ("Tony Parker")     | ("Kyle Anderson") | ("Dejounte Murray") |
      | ("Grizzlies")       | ("Kyle Anderson") | ("Dejounte Murray") |
      | ("Spurs")           | ("Kyle Anderson") | ("Dejounte Murray") |
      | ("Tony Parker")     | ("Kyle Anderson") | ("Grizzlies")       |
      | ("Dejounte Murray") | ("Kyle Anderson") | ("Grizzlies")       |
      | ("Spurs")           | ("Kyle Anderson") | ("Grizzlies")       |
      | ("Tony Parker")     | ("Kyle Anderson") | ("Spurs")           |
      | ("Dejounte Murray") | ("Kyle Anderson") | ("Spurs")           |
      | ("Grizzlies")       | ("Kyle Anderson") | ("Spurs")           |
