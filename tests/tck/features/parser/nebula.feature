# Copyright (c) 2020 vesoft inc. All rights reserved.
#
# This source code is licensed under Apache 2.0 License,
# attached with Common Clause Condition 1.0, found in the LICENSES directory.
Feature: Value parsing

  Scenario Outline: Parsing from text
    Given a string: <format>
    When They are parsed as Nebula Value
    Then The type of the parsed value should be <_type>

    Examples:
      | format                                               | _type        |
      | EMPTY                                                | EMPTY        |
      | NULL                                                 | NULL         |
      | NaN                                                  | NaN          |
      | BAD_DATA                                             | BAD_DATA     |
      | BAD_TYPE                                             | BAD_TYPE     |
      | OVERFLOW                                             | ERR_OVERFLOW |
      | UNKNOWN_PROP                                         | UNKNOWN_PROP |
      | DIV_BY_ZERO                                          | DIV_BY_ZERO  |
      | OUT_OF_RANGE                                         | OUT_OF_RANGE |
      | 123                                                  | iVal         |
      | -123                                                 | iVal         |
      | 3.14                                                 | fVal         |
      | -3.14                                                | fVal         |
      | true                                                 | bVal         |
      | false                                                | bVal         |
      | 'string'                                             | sVal         |
      | "string"                                             | sVal         |
      | "string'substr'"                                     | sVal         |
      | 'string"substr"'                                     | sVal         |
      | []                                                   | lVal         |
      | [1,2,3]                                              | lVal         |
      | [[:e2{}],[:e3{}]]                                    | lVal         |
      | {1,2,3}                                              | uVal         |
      | {}                                                   | mVal         |
      | {k1: 1, 'k2':true}                                   | mVal         |
      | ()                                                   | vVal         |
      | ('vid')                                              | vVal         |
      | (:t)                                                 | vVal         |
      | (:t{}:t)                                             | vVal         |
      | ('vid':t)                                            | vVal         |
      | ('vid':t:t)                                          | vVal         |
      | ('vid':t{p1:0,p2:' '})                               | vVal         |
      | ('vid':t{p1:0,p2:' '}:t{})                           | vVal         |
      | [:e]                                                 | eVal         |
      | [@-1]                                                | eVal         |
      | ['1'->'2']                                           | eVal         |
      | [:e{}]                                               | eVal         |
      | [:e{p1:0,p2:true}]                                   | eVal         |
      | [:e@0{p1:0,p2:true}]                                 | eVal         |
      | [:e{p1:0,p2:true}]                                   | eVal         |
      | [:e@-1{p1:0,p2:true}]                                | eVal         |
      | <()>                                                 | pVal         |
      | <()-->()<--()>                                       | pVal         |
      | <('v1':t{})>                                         | pVal         |
      | <('v1':t{})-[:e1{}]->('v2':t{})<-[:e2{}]-('v3':t{})> | pVal         |
