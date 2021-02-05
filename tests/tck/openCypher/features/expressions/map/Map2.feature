# Copyright (c) 2021 vesoft inc. All rights reserved.
#
# This source code is licensed under Apache 2.0 License,
# attached with Common Clause Condition 1.0, found in the LICENSES directory.
# Feature: Map2 - Dynamic Value Access
# # Dynamic value access refers to the bracket-operator – <expression resulting in a map>'['<expression resulting in a string>']' – irrespectively of whether the map key – i.e. <expression resulting in a string> – could be evaluated statically in a given scenario.
# Background:
# Given a graph with space named "nba"
# Scenario: [1] Use dynamic property lookup based on parameters when there is no type information
# And parameters are:
# | expr | {name: 'Apa'} |
# | idx  | 'name'        |
# When executing query:
# """
# WITH $expr AS expr, $idx AS idx
# RETURN expr[idx] AS value
# """
# Then the result should be, in any order:
# | value |
# | 'Apa' |
# And no side effects
# Scenario: [2] Use dynamic property lookup based on parameters when there is rhs type information
# And parameters are:
# | expr | {name: 'Apa'} |
# | idx  | 'name'        |
# When executing query:
# """
# WITH $expr AS expr, $idx AS idx
# RETURN expr[toString(idx)] AS value
# """
# Then the result should be, in any order:
# | value |
# | 'Apa' |
# And no side effects
# Scenario: [3] Fail at runtime when attempting to index with an Int into a Map
# And parameters are:
# | expr | {name: 'Apa'} |
# | idx  | 0             |
# When executing query:
# """
# WITH $expr AS expr, $idx AS idx
# RETURN expr[idx]
# """
# Then a TypeError should be raised at runtime: MapElementAccessByNonString
# Scenario: [4] Fail at runtime when trying to index into a map with a non-string
# And parameters are:
# | expr | {name: 'Apa'} |
# | idx  | 12.3          |
# When executing query:
# """
# WITH $expr AS expr, $idx AS idx
# RETURN expr[idx]
# """
# Then a TypeError should be raised at runtime: MapElementAccessByNonString
# Scenario: [5] Fail at runtime when trying to index something which is not a map
# Given any graph
# And parameters are:
# | expr | 100 |
# | idx  | 0   |
# When executing query:
# """
# WITH $expr AS expr, $idx AS idx
# RETURN expr[idx]
# """
# Then a TypeError should be raised at runtime: InvalidArgumentType
