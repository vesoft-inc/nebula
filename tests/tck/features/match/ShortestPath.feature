# Copyright (c) 2022 vesoft inc. All rights reserved.
#
# This source code is licensed under Apache 2.0 License.
Feature: ShortestPath

  Background:
    Given a graph with space named "nba"

  @wen
  Scenario: basic
    When executing query:
      """
      MATCH shortestPath((p : player{name: "Tim Duncan"})-[s:serve * 1..3]-(t : team)) Return s
      """
    Then the result should be, in any order:
      | s                                                                                                                                                                                                                                        |
      | [[:serve "Tim Duncan"->"Spurs" @0 {end_year: 2016, start_year: 1997}], [:serve "Jonathon Simmons"->"Spurs" @0 {end_year: 2017, start_year: 2015}], [:serve "Jonathon Simmons"->"76ers" @0 {end_year: 2019, start_year: 2019}]]           |
      | [[:serve "Tim Duncan"->"Spurs" @0 {end_year: 2016, start_year: 1997}], [:serve "Paul Gasol"->"Spurs" @0 {end_year: 2019, start_year: 2016}], [:serve "Paul Gasol"->"Grizzlies" @0 {end_year: 2008, start_year: 2001}]]                   |
      | [[:serve "Tim Duncan"->"Spurs" @0 {end_year: 2016, start_year: 1997}]]                                                                                                                                                                   |
      | [[:serve "Tim Duncan"->"Spurs" @0 {end_year: 2016, start_year: 1997}], [:serve "Marco Belinelli"->"Spurs" @0 {end_year: 2015, start_year: 2013}], [:serve "Marco Belinelli"->"Bulls" @0 {end_year: 2013, start_year: 2012}]]             |
      | [[:serve "Tim Duncan"->"Spurs" @0 {end_year: 2016, start_year: 1997}], [:serve "Danny Green"->"Spurs" @0 {end_year: 2018, start_year: 2010}], [:serve "Danny Green"->"Cavaliers" @0 {end_year: 2010, start_year: 2009}]]                 |
      | [[:serve "Tim Duncan"->"Spurs" @0 {end_year: 2016, start_year: 1997}], [:serve "Aron Baynes"->"Spurs" @0 {end_year: 2015, start_year: 2013}], [:serve "Aron Baynes"->"Celtics" @0 {end_year: 2019, start_year: 2017}]]                   |
      | [[:serve "Tim Duncan"->"Spurs" @0 {end_year: 2016, start_year: 1997}], [:serve "Cory Joseph"->"Spurs" @0 {end_year: 2015, start_year: 2011}], [:serve "Cory Joseph"->"Pacers" @0 {end_year: 2019, start_year: 2017}]]                    |
      | [[:serve "Tim Duncan"->"Spurs" @0 {end_year: 2016, start_year: 1997}], [:serve "Boris Diaw"->"Spurs" @0 {end_year: 2016, start_year: 2012}], [:serve "Boris Diaw"->"Hawks" @0 {end_year: 2005, start_year: 2003}]]                       |
      | [[:serve "Tim Duncan"->"Spurs" @0 {end_year: 2016, start_year: 1997}], [:serve "Paul Gasol"->"Spurs" @0 {end_year: 2019, start_year: 2016}], [:serve "Paul Gasol"->"Lakers" @0 {end_year: 2014, start_year: 2008}]]                      |
      | [[:serve "Tim Duncan"->"Spurs" @0 {end_year: 2016, start_year: 1997}], [:serve "Danny Green"->"Spurs" @0 {end_year: 2018, start_year: 2010}], [:serve "Danny Green"->"Raptors" @0 {end_year: 2019, start_year: 2018}]]                   |
      | [[:serve "Tim Duncan"->"Spurs" @0 {end_year: 2016, start_year: 1997}], [:serve "David West"->"Spurs" @0 {end_year: 2016, start_year: 2015}], [:serve "David West"->"Warriors" @0 {end_year: 2018, start_year: 2016}]]                    |
      | [[:serve "Tim Duncan"->"Spurs" @0 {end_year: 2016, start_year: 1997}], [:serve "Boris Diaw"->"Spurs" @0 {end_year: 2016, start_year: 2012}], [:serve "Boris Diaw"->"Hornets" @0 {end_year: 2012, start_year: 2008}]]                     |
      | [[:serve "Tim Duncan"->"Spurs" @0 {end_year: 2016, start_year: 1997}], [:serve "Marco Belinelli"->"Spurs" @0 {end_year: 2015, start_year: 2013}], [:serve "Marco Belinelli"->"Kings" @0 {end_year: 2016, start_year: 2015}]]             |
      | [[:serve "Tim Duncan"->"Spurs" @0 {end_year: 2016, start_year: 1997}], [:serve "Aron Baynes"->"Spurs" @0 {end_year: 2015, start_year: 2013}], [:serve "Aron Baynes"->"Pistons" @0 {end_year: 2017, start_year: 2015}]]                   |
      | [[:serve "Tim Duncan"->"Spurs" @0 {end_year: 2016, start_year: 1997}], [:serve "Tracy McGrady"->"Spurs" @0 {end_year: 2013, start_year: 2013}], [:serve "Tracy McGrady"->"Rockets" @0 {end_year: 2010, start_year: 2004}]]               |
      | [[:serve "Tim Duncan"->"Spurs" @0 {end_year: 2016, start_year: 1997}], [:serve "Boris Diaw"->"Spurs" @0 {end_year: 2016, start_year: 2012}], [:serve "Boris Diaw"->"Suns" @0 {end_year: 2008, start_year: 2005}]]                        |
      | [[:serve "Tim Duncan"->"Spurs" @0 {end_year: 2016, start_year: 1997}], [:serve "LaMarcus Aldridge"->"Spurs" @0 {end_year: 2019, start_year: 2015}], [:serve "LaMarcus Aldridge"->"Trail Blazers" @0 {end_year: 2015, start_year: 2006}]] |
      | [[:serve "Tim Duncan"->"Spurs" @0 {end_year: 2016, start_year: 1997}], [:serve "Paul Gasol"->"Spurs" @0 {end_year: 2019, start_year: 2016}], [:serve "Paul Gasol"->"Bucks" @0 {end_year: 2020, start_year: 2019}]]                       |
      | [[:serve "Tim Duncan"->"Spurs" @0 {end_year: 2016, start_year: 1997}], [:serve "Boris Diaw"->"Spurs" @0 {end_year: 2016, start_year: 2012}], [:serve "Boris Diaw"->"Jazz" @0 {end_year: 2017, start_year: 2016}]]                        |
      | [[:serve "Tim Duncan"->"Spurs" @0 {end_year: 2016, start_year: 1997}], [:serve "Tracy McGrady"->"Spurs" @0 {end_year: 2013, start_year: 2013}], [:serve "Tracy McGrady"->"Magic" @0 {end_year: 2004, start_year: 2000}]]                 |
    When executing query:
      """
      MATCH allShortestPaths((p : player{name: "Tim Duncan"})-[s:serve * 1..3]-(t : team)) RETURN s
      """
    Then the result should be, in any order:
      | s                                                                                                                                                                                                                                        |
      | [[:serve "Tim Duncan"->"Spurs" @0 {end_year: 2016, start_year: 1997}], [:serve "Jonathon Simmons"->"Spurs" @0 {end_year: 2017, start_year: 2015}], [:serve "Jonathon Simmons"->"76ers" @0 {end_year: 2019, start_year: 2019}]]           |
      | [[:serve "Tim Duncan"->"Spurs" @0 {end_year: 2016, start_year: 1997}], [:serve "Tiago Splitter"->"Spurs" @0 {end_year: 2015, start_year: 2010}], [:serve "Tiago Splitter"->"76ers" @0 {end_year: 2017, start_year: 2017}]]               |
      | [[:serve "Tim Duncan"->"Spurs" @0 {end_year: 2016, start_year: 1997}], [:serve "Marco Belinelli"->"Spurs" @0 {end_year: 2015, start_year: 2013}], [:serve "Marco Belinelli"->"76ers" @0 {end_year: 2018, start_year: 2018}]]             |
      | [[:serve "Tim Duncan"->"Spurs" @0 {end_year: 2016, start_year: 1997}], [:serve "Marco Belinelli"->"Spurs" @1 {end_year: 2019, start_year: 2018}], [:serve "Marco Belinelli"->"76ers" @0 {end_year: 2018, start_year: 2018}]]             |
      | [[:serve "Tim Duncan"->"Spurs" @0 {end_year: 2016, start_year: 1997}], [:serve "Paul Gasol"->"Spurs" @0 {end_year: 2019, start_year: 2016}], [:serve "Paul Gasol"->"Grizzlies" @0 {end_year: 2008, start_year: 2001}]]                   |
      | [[:serve "Tim Duncan"->"Spurs" @0 {end_year: 2016, start_year: 1997}], [:serve "Kyle Anderson"->"Spurs" @0 {end_year: 2018, start_year: 2014}], [:serve "Kyle Anderson"->"Grizzlies" @0 {end_year: 2019, start_year: 2018}]]             |
      | [[:serve "Tim Duncan"->"Spurs" @0 {end_year: 2016, start_year: 1997}], [:serve "Rudy Gay"->"Spurs" @0 {end_year: 2019, start_year: 2017}], [:serve "Rudy Gay"->"Grizzlies" @0 {end_year: 2013, start_year: 2006}]]                       |
      | [[:serve "Tim Duncan"->"Spurs" @0 {end_year: 2016, start_year: 1997}]]                                                                                                                                                                   |
      | [[:serve "Tim Duncan"->"Spurs" @0 {end_year: 2016, start_year: 1997}], [:serve "Marco Belinelli"->"Spurs" @0 {end_year: 2015, start_year: 2013}], [:serve "Marco Belinelli"->"Bulls" @0 {end_year: 2013, start_year: 2012}]]             |
      | [[:serve "Tim Duncan"->"Spurs" @0 {end_year: 2016, start_year: 1997}], [:serve "Marco Belinelli"->"Spurs" @1 {end_year: 2019, start_year: 2018}], [:serve "Marco Belinelli"->"Bulls" @0 {end_year: 2013, start_year: 2012}]]             |
      | [[:serve "Tim Duncan"->"Spurs" @0 {end_year: 2016, start_year: 1997}], [:serve "Paul Gasol"->"Spurs" @0 {end_year: 2019, start_year: 2016}], [:serve "Paul Gasol"->"Bulls" @0 {end_year: 2016, start_year: 2014}]]                       |
      | [[:serve "Tim Duncan"->"Spurs" @0 {end_year: 2016, start_year: 1997}], [:serve "Danny Green"->"Spurs" @0 {end_year: 2018, start_year: 2010}], [:serve "Danny Green"->"Cavaliers" @0 {end_year: 2010, start_year: 2009}]]                 |
      | [[:serve "Tim Duncan"->"Spurs" @0 {end_year: 2016, start_year: 1997}], [:serve "Aron Baynes"->"Spurs" @0 {end_year: 2015, start_year: 2013}], [:serve "Aron Baynes"->"Celtics" @0 {end_year: 2019, start_year: 2017}]]                   |
      | [[:serve "Tim Duncan"->"Spurs" @0 {end_year: 2016, start_year: 1997}], [:serve "Cory Joseph"->"Spurs" @0 {end_year: 2015, start_year: 2011}], [:serve "Cory Joseph"->"Pacers" @0 {end_year: 2019, start_year: 2017}]]                    |
      | [[:serve "Tim Duncan"->"Spurs" @0 {end_year: 2016, start_year: 1997}], [:serve "David West"->"Spurs" @0 {end_year: 2016, start_year: 2015}], [:serve "David West"->"Pacers" @0 {end_year: 2015, start_year: 2011}]]                      |
      | [[:serve "Tim Duncan"->"Spurs" @0 {end_year: 2016, start_year: 1997}], [:serve "Boris Diaw"->"Spurs" @0 {end_year: 2016, start_year: 2012}], [:serve "Boris Diaw"->"Hawks" @0 {end_year: 2005, start_year: 2003}]]                       |
      | [[:serve "Tim Duncan"->"Spurs" @0 {end_year: 2016, start_year: 1997}], [:serve "Marco Belinelli"->"Spurs" @0 {end_year: 2015, start_year: 2013}], [:serve "Marco Belinelli"->"Hawks" @0 {end_year: 2018, start_year: 2017}]]             |
      | [[:serve "Tim Duncan"->"Spurs" @0 {end_year: 2016, start_year: 1997}], [:serve "Marco Belinelli"->"Spurs" @1 {end_year: 2019, start_year: 2018}], [:serve "Marco Belinelli"->"Hawks" @0 {end_year: 2018, start_year: 2017}]]             |
      | [[:serve "Tim Duncan"->"Spurs" @0 {end_year: 2016, start_year: 1997}], [:serve "Tiago Splitter"->"Spurs" @0 {end_year: 2015, start_year: 2010}], [:serve "Tiago Splitter"->"Hawks" @0 {end_year: 2017, start_year: 2015}]]               |
      | [[:serve "Tim Duncan"->"Spurs" @0 {end_year: 2016, start_year: 1997}], [:serve "Paul Gasol"->"Spurs" @0 {end_year: 2019, start_year: 2016}], [:serve "Paul Gasol"->"Lakers" @0 {end_year: 2014, start_year: 2008}]]                      |
      | [[:serve "Tim Duncan"->"Spurs" @0 {end_year: 2016, start_year: 1997}], [:serve "Danny Green"->"Spurs" @0 {end_year: 2018, start_year: 2010}], [:serve "Danny Green"->"Raptors" @0 {end_year: 2019, start_year: 2018}]]                   |
      | [[:serve "Tim Duncan"->"Spurs" @0 {end_year: 2016, start_year: 1997}], [:serve "Tracy McGrady"->"Spurs" @0 {end_year: 2013, start_year: 2013}], [:serve "Tracy McGrady"->"Raptors" @0 {end_year: 2000, start_year: 1997}]]               |
      | [[:serve "Tim Duncan"->"Spurs" @0 {end_year: 2016, start_year: 1997}], [:serve "Cory Joseph"->"Spurs" @0 {end_year: 2015, start_year: 2011}], [:serve "Cory Joseph"->"Raptors" @0 {end_year: 2017, start_year: 2015}]]                   |
      | [[:serve "Tim Duncan"->"Spurs" @0 {end_year: 2016, start_year: 1997}], [:serve "Marco Belinelli"->"Spurs" @0 {end_year: 2015, start_year: 2013}], [:serve "Marco Belinelli"->"Raptors" @0 {end_year: 2010, start_year: 2009}]]           |
      | [[:serve "Tim Duncan"->"Spurs" @0 {end_year: 2016, start_year: 1997}], [:serve "Marco Belinelli"->"Spurs" @1 {end_year: 2019, start_year: 2018}], [:serve "Marco Belinelli"->"Raptors" @0 {end_year: 2010, start_year: 2009}]]           |
      | [[:serve "Tim Duncan"->"Spurs" @0 {end_year: 2016, start_year: 1997}], [:serve "Rudy Gay"->"Spurs" @0 {end_year: 2019, start_year: 2017}], [:serve "Rudy Gay"->"Raptors" @0 {end_year: 2013, start_year: 2013}]]                         |
      | [[:serve "Tim Duncan"->"Spurs" @0 {end_year: 2016, start_year: 1997}], [:serve "David West"->"Spurs" @0 {end_year: 2016, start_year: 2015}], [:serve "David West"->"Warriors" @0 {end_year: 2018, start_year: 2016}]]                    |
      | [[:serve "Tim Duncan"->"Spurs" @0 {end_year: 2016, start_year: 1997}], [:serve "Marco Belinelli"->"Spurs" @0 {end_year: 2015, start_year: 2013}], [:serve "Marco Belinelli"->"Warriors" @0 {end_year: 2009, start_year: 2007}]]          |
      | [[:serve "Tim Duncan"->"Spurs" @0 {end_year: 2016, start_year: 1997}], [:serve "Marco Belinelli"->"Spurs" @1 {end_year: 2019, start_year: 2018}], [:serve "Marco Belinelli"->"Warriors" @0 {end_year: 2009, start_year: 2007}]]          |
      | [[:serve "Tim Duncan"->"Spurs" @0 {end_year: 2016, start_year: 1997}], [:serve "Boris Diaw"->"Spurs" @0 {end_year: 2016, start_year: 2012}], [:serve "Boris Diaw"->"Hornets" @0 {end_year: 2012, start_year: 2008}]]                     |
      | [[:serve "Tim Duncan"->"Spurs" @0 {end_year: 2016, start_year: 1997}], [:serve "David West"->"Spurs" @0 {end_year: 2016, start_year: 2015}], [:serve "David West"->"Hornets" @0 {end_year: 2011, start_year: 2003}]]                     |
      | [[:serve "Tim Duncan"->"Spurs" @0 {end_year: 2016, start_year: 1997}], [:serve "Tony Parker"->"Spurs" @0 {end_year: 2018, start_year: 1999}], [:serve "Tony Parker"->"Hornets" @0 {end_year: 2019, start_year: 2018}]]                   |
      | [[:serve "Tim Duncan"->"Spurs" @0 {end_year: 2016, start_year: 1997}], [:serve "Marco Belinelli"->"Spurs" @0 {end_year: 2015, start_year: 2013}], [:serve "Marco Belinelli"->"Hornets" @0 {end_year: 2012, start_year: 2010}]]           |
      | [[:serve "Tim Duncan"->"Spurs" @0 {end_year: 2016, start_year: 1997}], [:serve "Marco Belinelli"->"Spurs" @0 {end_year: 2015, start_year: 2013}], [:serve "Marco Belinelli"->"Hornets" @1 {end_year: 2017, start_year: 2016}]]           |
      | [[:serve "Tim Duncan"->"Spurs" @0 {end_year: 2016, start_year: 1997}], [:serve "Marco Belinelli"->"Spurs" @1 {end_year: 2019, start_year: 2018}], [:serve "Marco Belinelli"->"Hornets" @0 {end_year: 2012, start_year: 2010}]]           |
      | [[:serve "Tim Duncan"->"Spurs" @0 {end_year: 2016, start_year: 1997}], [:serve "Marco Belinelli"->"Spurs" @1 {end_year: 2019, start_year: 2018}], [:serve "Marco Belinelli"->"Hornets" @1 {end_year: 2017, start_year: 2016}]]           |
      | [[:serve "Tim Duncan"->"Spurs" @0 {end_year: 2016, start_year: 1997}], [:serve "Marco Belinelli"->"Spurs" @0 {end_year: 2015, start_year: 2013}], [:serve "Marco Belinelli"->"Kings" @0 {end_year: 2016, start_year: 2015}]]             |
      | [[:serve "Tim Duncan"->"Spurs" @0 {end_year: 2016, start_year: 1997}], [:serve "Marco Belinelli"->"Spurs" @1 {end_year: 2019, start_year: 2018}], [:serve "Marco Belinelli"->"Kings" @0 {end_year: 2016, start_year: 2015}]]             |
      | [[:serve "Tim Duncan"->"Spurs" @0 {end_year: 2016, start_year: 1997}], [:serve "Rudy Gay"->"Spurs" @0 {end_year: 2019, start_year: 2017}], [:serve "Rudy Gay"->"Kings" @0 {end_year: 2017, start_year: 2013}]]                           |
      | [[:serve "Tim Duncan"->"Spurs" @0 {end_year: 2016, start_year: 1997}], [:serve "Aron Baynes"->"Spurs" @0 {end_year: 2015, start_year: 2013}], [:serve "Aron Baynes"->"Pistons" @0 {end_year: 2017, start_year: 2015}]]                   |
      | [[:serve "Tim Duncan"->"Spurs" @0 {end_year: 2016, start_year: 1997}], [:serve "Tracy McGrady"->"Spurs" @0 {end_year: 2013, start_year: 2013}], [:serve "Tracy McGrady"->"Rockets" @0 {end_year: 2010, start_year: 2004}]]               |
      | [[:serve "Tim Duncan"->"Spurs" @0 {end_year: 2016, start_year: 1997}], [:serve "Boris Diaw"->"Spurs" @0 {end_year: 2016, start_year: 2012}], [:serve "Boris Diaw"->"Suns" @0 {end_year: 2008, start_year: 2005}]]                        |
      | [[:serve "Tim Duncan"->"Spurs" @0 {end_year: 2016, start_year: 1997}], [:serve "LaMarcus Aldridge"->"Spurs" @0 {end_year: 2019, start_year: 2015}], [:serve "LaMarcus Aldridge"->"Trail Blazers" @0 {end_year: 2015, start_year: 2006}]] |
      | [[:serve "Tim Duncan"->"Spurs" @0 {end_year: 2016, start_year: 1997}], [:serve "Paul Gasol"->"Spurs" @0 {end_year: 2019, start_year: 2016}], [:serve "Paul Gasol"->"Bucks" @0 {end_year: 2020, start_year: 2019}]]                       |
      | [[:serve "Tim Duncan"->"Spurs" @0 {end_year: 2016, start_year: 1997}], [:serve "Boris Diaw"->"Spurs" @0 {end_year: 2016, start_year: 2012}], [:serve "Boris Diaw"->"Jazz" @0 {end_year: 2017, start_year: 2016}]]                        |
      | [[:serve "Tim Duncan"->"Spurs" @0 {end_year: 2016, start_year: 1997}], [:serve "Tracy McGrady"->"Spurs" @0 {end_year: 2013, start_year: 2013}], [:serve "Tracy McGrady"->"Magic" @0 {end_year: 2004, start_year: 2000}]]                 |
      | [[:serve "Tim Duncan"->"Spurs" @0 {end_year: 2016, start_year: 1997}], [:serve "Jonathon Simmons"->"Spurs" @0 {end_year: 2017, start_year: 2015}], [:serve "Jonathon Simmons"->"Magic" @0 {end_year: 2019, start_year: 2017}]]           |
    When executing query:
      """
      MATCH allShortestPaths((p : player{name: "Tim Duncan"})<-[s:serve * 1..3]-(t : team)) RETURN s
      """
    Then the result should be, in any order:
      | s |
    When executing query:
      """
      MATCH allShortestPaths((p : player{name: "Tim Duncan"})-[s:serve * 1..3]->(t : team)) RETURN s
      """
    Then the result should be, in any order:
      | s                                                                      |
      | [[:serve "Tim Duncan"->"Spurs" @0 {end_year: 2016, start_year: 1997}]] |
    When executing query:
      """
      MATCH pat = shortestPath((p : player{name:"Yao Ming"})-[pa:serve * 1..3]-(t : team)) RETURN pat
      """
    Then the result should be, in any order:
      | pat                                                                                                                                                                                                                                                                                                                                  |
      | <("Yao Ming" :player{age: 38, name: "Yao Ming"})-[:serve@0 {end_year: 2011, start_year: 2002}]->("Rockets" :team{name: "Rockets"})<-[:serve@0 {end_year: 2019, start_year: 2018}]-("Carmelo Anthony" :player{age: 34, name: "Carmelo Anthony"})-[:serve@0 {end_year: 2017, start_year: 2011}]->("Knicks" :team{name: "Knicks"})>     |
      | <("Yao Ming" :player{age: 38, name: "Yao Ming"})-[:serve@0 {end_year: 2011, start_year: 2002}]->("Rockets" :team{name: "Rockets"})<-[:serve@0 {end_year: 2019, start_year: 2018}]-("Carmelo Anthony" :player{age: 34, name: "Carmelo Anthony"})-[:serve@0 {end_year: 2011, start_year: 2003}]->("Nuggets" :team{name: "Nuggets"})>   |
      | <("Yao Ming" :player{age: 38, name: "Yao Ming"})-[:serve@0 {end_year: 2011, start_year: 2002}]->("Rockets" :team{name: "Rockets"})<-[:serve@0 {end_year: 2021, start_year: 2017}]-("Chris Paul" :player{age: 33, name: "Chris Paul"})-[:serve@0 {end_year: 2017, start_year: 2011}]->("Clippers" :team{name: "Clippers"})>           |
      | <("Yao Ming" :player{age: 38, name: "Yao Ming"})-[:serve@0 {end_year: 2011, start_year: 2002}]->("Rockets" :team{name: "Rockets"})<-[:serve@0 {end_year: 2010, start_year: 2004}]-("Tracy McGrady" :player{age: 39, name: "Tracy McGrady"})-[:serve@0 {end_year: 2013, start_year: 2013}]->("Spurs" :team{name: "Spurs"})>           |
      | <("Yao Ming" :player{age: 38, name: "Yao Ming"})-[:serve@0 {end_year: 2011, start_year: 2002}]->("Rockets" :team{name: "Rockets"})<-[:serve@0 {end_year: 2016, start_year: 2013}]-("Dwight Howard" :player{age: 33, name: "Dwight Howard"})-[:serve@0 {end_year: 2017, start_year: 2016}]->("Hawks" :team{name: "Hawks"})>           |
      | <("Yao Ming" :player{age: 38, name: "Yao Ming"})-[:serve@0 {end_year: 2011, start_year: 2002}]->("Rockets" :team{name: "Rockets"})<-[:serve@0 {end_year: 2016, start_year: 2013}]-("Dwight Howard" :player{age: 33, name: "Dwight Howard"})-[:serve@0 {end_year: 2013, start_year: 2012}]->("Lakers" :team{name: "Lakers"})>         |
      | <("Yao Ming" :player{age: 38, name: "Yao Ming"})-[:serve@0 {end_year: 2011, start_year: 2002}]->("Rockets" :team{name: "Rockets"})<-[:serve@0 {end_year: 2010, start_year: 2004}]-("Tracy McGrady" :player{age: 39, name: "Tracy McGrady"})-[:serve@0 {end_year: 2000, start_year: 1997}]->("Raptors" :team{name: "Raptors"})>       |
      | <("Yao Ming" :player{age: 38, name: "Yao Ming"})-[:serve@0 {end_year: 2011, start_year: 2002}]->("Rockets" :team{name: "Rockets"})<-[:serve@0 {end_year: 2016, start_year: 2013}]-("Dwight Howard" :player{age: 33, name: "Dwight Howard"})-[:serve@0 {end_year: 2018, start_year: 2017}]->("Hornets" :team{name: "Hornets"})>       |
      | <("Yao Ming" :player{age: 38, name: "Yao Ming"})-[:serve@0 {end_year: 2011, start_year: 2002}]->("Rockets" :team{name: "Rockets"})>                                                                                                                                                                                                  |
      | <("Yao Ming" :player{age: 38, name: "Yao Ming"})-[:serve@0 {end_year: 2011, start_year: 2002}]->("Rockets" :team{name: "Rockets"})<-[:serve@0 {end_year: 2019, start_year: 2018}]-("Carmelo Anthony" :player{age: 34, name: "Carmelo Anthony"})-[:serve@0 {end_year: 2018, start_year: 2017}]->("Thunders" :team{name: "Thunders"})> |
      | <("Yao Ming" :player{age: 38, name: "Yao Ming"})-[:serve@0 {end_year: 2011, start_year: 2002}]->("Rockets" :team{name: "Rockets"})<-[:serve@0 {end_year: 2016, start_year: 2013}]-("Dwight Howard" :player{age: 33, name: "Dwight Howard"})-[:serve@0 {end_year: 2012, start_year: 2004}]->("Magic" :team{name: "Magic"})>           |
      | <("Yao Ming" :player{age: 38, name: "Yao Ming"})-[:serve@0 {end_year: 2011, start_year: 2002}]->("Rockets" :team{name: "Rockets"})<-[:serve@0 {end_year: 2016, start_year: 2013}]-("Dwight Howard" :player{age: 33, name: "Dwight Howard"})-[:serve@0 {end_year: 2019, start_year: 2018}]->("Wizards" :team{name: "Wizards"})>       |
    When executing query:
      """
      MATCH allShortestPaths((p : player)-[s:serve * 1..3]-(t : team))  WHERE p.player.name == "Yao Ming" RETURN t
      """
    Then the result should be, in any order:
      | t                                    |
      | ("Knicks" :team{name: "Knicks"})     |
      | ("Nuggets" :team{name: "Nuggets"})   |
      | ("Clippers" :team{name: "Clippers"}) |
      | ("Spurs" :team{name: "Spurs"})       |
      | ("Hawks" :team{name: "Hawks"})       |
      | ("Lakers" :team{name: "Lakers"})     |
      | ("Raptors" :team{name: "Raptors"})   |
      | ("Hornets" :team{name: "Hornets"})   |
      | ("Hornets" :team{name: "Hornets"})   |
      | ("Rockets" :team{name: "Rockets"})   |
      | ("Thunders" :team{name: "Thunders"}) |
      | ("Thunders" :team{name: "Thunders"}) |
      | ("Magic" :team{name: "Magic"})       |
      | ("Magic" :team{name: "Magic"})       |
      | ("Wizards" :team{name: "Wizards"})   |
