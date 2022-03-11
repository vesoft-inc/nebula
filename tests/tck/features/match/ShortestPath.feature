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
			| s                                                                                                                                                   |
			| [[:serve "Tim Duncan"->"Spurs" @0 {end_year: 2016, start_year: 1997}], [:serve "Jonathon Simmons"->"Spurs" @0 {end_year: 2017, start_year: 2015}]]  |
			| [[:serve "Tim Duncan"->"Spurs" @0 {end_year: 2016, start_year: 1997}], [:serve "Paul Gasol"->"Spurs" @0 {end_year: 2019, start_year: 2016}]]        |
			| [[:serve "Tim Duncan"->"Spurs" @0 {end_year: 2016, start_year: 1997}]]                                                                              |
			| [[:serve "Tim Duncan"->"Spurs" @0 {end_year: 2016, start_year: 1997}], [:serve "Marco Belinelli"->"Spurs" @0 {end_year: 2015, start_year: 2013}]]   |
			| [[:serve "Tim Duncan"->"Spurs" @0 {end_year: 2016, start_year: 1997}], [:serve "Danny Green"->"Spurs" @0 {end_year: 2018, start_year: 2010}]]       |
			| [[:serve "Tim Duncan"->"Spurs" @0 {end_year: 2016, start_year: 1997}], [:serve "Aron Baynes"->"Spurs" @0 {end_year: 2015, start_year: 2013}]]       |
			| [[:serve "Tim Duncan"->"Spurs" @0 {end_year: 2016, start_year: 1997}], [:serve "Cory Joseph"->"Spurs" @0 {end_year: 2015, start_year: 2011}]]       |
			| [[:serve "Tim Duncan"->"Spurs" @0 {end_year: 2016, start_year: 1997}], [:serve "Boris Diaw"->"Spurs" @0 {end_year: 2016, start_year: 2012}]]        |
			| [[:serve "Tim Duncan"->"Spurs" @0 {end_year: 2016, start_year: 1997}], [:serve "Paul Gasol"->"Spurs" @0 {end_year: 2019, start_year: 2016}]]        |
			| [[:serve "Tim Duncan"->"Spurs" @0 {end_year: 2016, start_year: 1997}], [:serve "Danny Green"->"Spurs" @0 {end_year: 2018, start_year: 2010}]]       |
			| [[:serve "Tim Duncan"->"Spurs" @0 {end_year: 2016, start_year: 1997}], [:serve "David West"->"Spurs" @0 {end_year: 2016, start_year: 2015}]]        |
			| [[:serve "Tim Duncan"->"Spurs" @0 {end_year: 2016, start_year: 1997}], [:serve "Boris Diaw"->"Spurs" @0 {end_year: 2016, start_year: 2012}]]        |
			| [[:serve "Tim Duncan"->"Spurs" @0 {end_year: 2016, start_year: 1997}], [:serve "Marco Belinelli"->"Spurs" @0 {end_year: 2015, start_year: 2013}]]   |
			| [[:serve "Tim Duncan"->"Spurs" @0 {end_year: 2016, start_year: 1997}], [:serve "Aron Baynes"->"Spurs" @0 {end_year: 2015, start_year: 2013}]]       |
			| [[:serve "Tim Duncan"->"Spurs" @0 {end_year: 2016, start_year: 1997}], [:serve "Tracy McGrady"->"Spurs" @0 {end_year: 2013, start_year: 2013}]]     |
			| [[:serve "Tim Duncan"->"Spurs" @0 {end_year: 2016, start_year: 1997}], [:serve "Boris Diaw"->"Spurs" @0 {end_year: 2016, start_year: 2012}]]        |
			| [[:serve "Tim Duncan"->"Spurs" @0 {end_year: 2016, start_year: 1997}], [:serve "LaMarcus Aldridge"->"Spurs" @0 {end_year: 2019, start_year: 2015}]] |
			| [[:serve "Tim Duncan"->"Spurs" @0 {end_year: 2016, start_year: 1997}], [:serve "Paul Gasol"->"Spurs" @0 {end_year: 2019, start_year: 2016}]]        |
			| [[:serve "Tim Duncan"->"Spurs" @0 {end_year: 2016, start_year: 1997}], [:serve "Boris Diaw"->"Spurs" @0 {end_year: 2016, start_year: 2012}]]        |
			| [[:serve "Tim Duncan"->"Spurs" @0 {end_year: 2016, start_year: 1997}], [:serve "Tracy McGrady"->"Spurs" @0 {end_year: 2013, start_year: 2013}]]     |
		When executing query:
			"""
      MATCH allshortestPaths((p : player{name: "Tim Duncan"})-[s:serve * 1..3]-(t : team)) return s
			"""
		Then the result should be, in any order:
			| s                                                                                                                                                   |
			| [[:serve "Tim Duncan"->"Spurs" @0 {end_year: 2016, start_year: 1997}], [:serve "Jonathon Simmons"->"Spurs" @0 {end_year: 2017, start_year: 2015}]]  |
			| [[:serve "Tim Duncan"->"Spurs" @0 {end_year: 2016, start_year: 1997}], [:serve "Tiago Splitter"->"Spurs" @0 {end_year: 2015, start_year: 2010}]]    |
			| [[:serve "Tim Duncan"->"Spurs" @0 {end_year: 2016, start_year: 1997}], [:serve "Marco Belinelli"->"Spurs" @0 {end_year: 2015, start_year: 2013}]]   |
			| [[:serve "Tim Duncan"->"Spurs" @0 {end_year: 2016, start_year: 1997}], [:serve "Marco Belinelli"->"Spurs" @0 {end_year: 2015, start_year: 2013}]]   |
			| [[:serve "Tim Duncan"->"Spurs" @0 {end_year: 2016, start_year: 1997}], [:serve "Paul Gasol"->"Spurs" @0 {end_year: 2019, start_year: 2016}]]        |
			| [[:serve "Tim Duncan"->"Spurs" @0 {end_year: 2016, start_year: 1997}], [:serve "Kyle Anderson"->"Spurs" @0 {end_year: 2018, start_year: 2014}]]     |
			| [[:serve "Tim Duncan"->"Spurs" @0 {end_year: 2016, start_year: 1997}], [:serve "Rudy Gay"->"Spurs" @0 {end_year: 2019, start_year: 2017}]]          |
			| [[:serve "Tim Duncan"->"Spurs" @0 {end_year: 2016, start_year: 1997}]]                                                                              |
			| [[:serve "Tim Duncan"->"Spurs" @0 {end_year: 2016, start_year: 1997}], [:serve "Marco Belinelli"->"Spurs" @0 {end_year: 2015, start_year: 2013}]]   |
			| [[:serve "Tim Duncan"->"Spurs" @0 {end_year: 2016, start_year: 1997}], [:serve "Marco Belinelli"->"Spurs" @0 {end_year: 2015, start_year: 2013}]]   |
			| [[:serve "Tim Duncan"->"Spurs" @0 {end_year: 2016, start_year: 1997}], [:serve "Paul Gasol"->"Spurs" @0 {end_year: 2019, start_year: 2016}]]        |
			| [[:serve "Tim Duncan"->"Spurs" @0 {end_year: 2016, start_year: 1997}], [:serve "Danny Green"->"Spurs" @0 {end_year: 2018, start_year: 2010}]]       |
			| [[:serve "Tim Duncan"->"Spurs" @0 {end_year: 2016, start_year: 1997}], [:serve "Aron Baynes"->"Spurs" @0 {end_year: 2015, start_year: 2013}]]       |
			| [[:serve "Tim Duncan"->"Spurs" @0 {end_year: 2016, start_year: 1997}], [:serve "Cory Joseph"->"Spurs" @0 {end_year: 2015, start_year: 2011}]]       |
			| [[:serve "Tim Duncan"->"Spurs" @0 {end_year: 2016, start_year: 1997}], [:serve "David West"->"Spurs" @0 {end_year: 2016, start_year: 2015}]]        |
			| [[:serve "Tim Duncan"->"Spurs" @0 {end_year: 2016, start_year: 1997}], [:serve "Boris Diaw"->"Spurs" @0 {end_year: 2016, start_year: 2012}]]        |
			| [[:serve "Tim Duncan"->"Spurs" @0 {end_year: 2016, start_year: 1997}], [:serve "Marco Belinelli"->"Spurs" @0 {end_year: 2015, start_year: 2013}]]   |
			| [[:serve "Tim Duncan"->"Spurs" @0 {end_year: 2016, start_year: 1997}], [:serve "Marco Belinelli"->"Spurs" @0 {end_year: 2015, start_year: 2013}]]   |
			| [[:serve "Tim Duncan"->"Spurs" @0 {end_year: 2016, start_year: 1997}], [:serve "Tiago Splitter"->"Spurs" @0 {end_year: 2015, start_year: 2010}]]    |
			| [[:serve "Tim Duncan"->"Spurs" @0 {end_year: 2016, start_year: 1997}], [:serve "Paul Gasol"->"Spurs" @0 {end_year: 2019, start_year: 2016}]]        |
			| [[:serve "Tim Duncan"->"Spurs" @0 {end_year: 2016, start_year: 1997}], [:serve "Danny Green"->"Spurs" @0 {end_year: 2018, start_year: 2010}]]       |
			| [[:serve "Tim Duncan"->"Spurs" @0 {end_year: 2016, start_year: 1997}], [:serve "Tracy McGrady"->"Spurs" @0 {end_year: 2013, start_year: 2013}]]     |
			| [[:serve "Tim Duncan"->"Spurs" @0 {end_year: 2016, start_year: 1997}], [:serve "Cory Joseph"->"Spurs" @0 {end_year: 2015, start_year: 2011}]]       |
			| [[:serve "Tim Duncan"->"Spurs" @0 {end_year: 2016, start_year: 1997}], [:serve "Marco Belinelli"->"Spurs" @0 {end_year: 2015, start_year: 2013}]]   |
			| [[:serve "Tim Duncan"->"Spurs" @0 {end_year: 2016, start_year: 1997}], [:serve "Marco Belinelli"->"Spurs" @0 {end_year: 2015, start_year: 2013}]]   |
			| [[:serve "Tim Duncan"->"Spurs" @0 {end_year: 2016, start_year: 1997}], [:serve "Rudy Gay"->"Spurs" @0 {end_year: 2019, start_year: 2017}]]          |
			| [[:serve "Tim Duncan"->"Spurs" @0 {end_year: 2016, start_year: 1997}], [:serve "David West"->"Spurs" @0 {end_year: 2016, start_year: 2015}]]        |
			| [[:serve "Tim Duncan"->"Spurs" @0 {end_year: 2016, start_year: 1997}], [:serve "Marco Belinelli"->"Spurs" @0 {end_year: 2015, start_year: 2013}]]   |
			| [[:serve "Tim Duncan"->"Spurs" @0 {end_year: 2016, start_year: 1997}], [:serve "Marco Belinelli"->"Spurs" @0 {end_year: 2015, start_year: 2013}]]   |
			| [[:serve "Tim Duncan"->"Spurs" @0 {end_year: 2016, start_year: 1997}], [:serve "Boris Diaw"->"Spurs" @0 {end_year: 2016, start_year: 2012}]]        |
			| [[:serve "Tim Duncan"->"Spurs" @0 {end_year: 2016, start_year: 1997}], [:serve "David West"->"Spurs" @0 {end_year: 2016, start_year: 2015}]]        |
			| [[:serve "Tim Duncan"->"Spurs" @0 {end_year: 2016, start_year: 1997}], [:serve "Tony Parker"->"Spurs" @0 {end_year: 2018, start_year: 1999}]]       |
			| [[:serve "Tim Duncan"->"Spurs" @0 {end_year: 2016, start_year: 1997}], [:serve "Marco Belinelli"->"Spurs" @0 {end_year: 2015, start_year: 2013}]]   |
			| [[:serve "Tim Duncan"->"Spurs" @0 {end_year: 2016, start_year: 1997}], [:serve "Marco Belinelli"->"Spurs" @0 {end_year: 2015, start_year: 2013}]]   |
			| [[:serve "Tim Duncan"->"Spurs" @0 {end_year: 2016, start_year: 1997}], [:serve "Marco Belinelli"->"Spurs" @0 {end_year: 2015, start_year: 2013}]]   |
			| [[:serve "Tim Duncan"->"Spurs" @0 {end_year: 2016, start_year: 1997}], [:serve "Marco Belinelli"->"Spurs" @0 {end_year: 2015, start_year: 2013}]]   |
			| [[:serve "Tim Duncan"->"Spurs" @0 {end_year: 2016, start_year: 1997}], [:serve "Rudy Gay"->"Spurs" @0 {end_year: 2019, start_year: 2017}]]          |
			| [[:serve "Tim Duncan"->"Spurs" @0 {end_year: 2016, start_year: 1997}], [:serve "Aron Baynes"->"Spurs" @0 {end_year: 2015, start_year: 2013}]]       |
			| [[:serve "Tim Duncan"->"Spurs" @0 {end_year: 2016, start_year: 1997}], [:serve "Tracy McGrady"->"Spurs" @0 {end_year: 2013, start_year: 2013}]]     |
			| [[:serve "Tim Duncan"->"Spurs" @0 {end_year: 2016, start_year: 1997}], [:serve "Boris Diaw"->"Spurs" @0 {end_year: 2016, start_year: 2012}]]        |
			| [[:serve "Tim Duncan"->"Spurs" @0 {end_year: 2016, start_year: 1997}], [:serve "LaMarcus Aldridge"->"Spurs" @0 {end_year: 2019, start_year: 2015}]] |
			| [[:serve "Tim Duncan"->"Spurs" @0 {end_year: 2016, start_year: 1997}], [:serve "Paul Gasol"->"Spurs" @0 {end_year: 2019, start_year: 2016}]]        |
			| [[:serve "Tim Duncan"->"Spurs" @0 {end_year: 2016, start_year: 1997}], [:serve "Boris Diaw"->"Spurs" @0 {end_year: 2016, start_year: 2012}]]        |
			| [[:serve "Tim Duncan"->"Spurs" @0 {end_year: 2016, start_year: 1997}], [:serve "Tracy McGrady"->"Spurs" @0 {end_year: 2013, start_year: 2013}]]     |
			| [[:serve "Tim Duncan"->"Spurs" @0 {end_year: 2016, start_year: 1997}], [:serve "Jonathon Simmons"->"Spurs" @0 {end_year: 2017, start_year: 2015}]]  |
		When executing query:
			"""
      MATCH allshortestPaths((p : player{name: "Tim Duncan"})<-[s:serve * 1..3]-(t : team)) return s
			"""
		Then the result should be, in any order:
			| s |
		When executing query:
			"""
      MATCH allshortestPaths((p : player{name: "Tim Duncan"})-[s:serve * 1..3]->(t : team)) return s
			"""
		Then the result should be, in any order:
			| s                                                                      |
			| [[:serve "Tim Duncan"->"Spurs" @0 {end_year: 2016, start_year: 1997}]] |
