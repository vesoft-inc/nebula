Feature: Test space steps

  Scenario: Test space steps
    Given an empty graph
    And create a space with following options:
      | partition_num  | 9                |
      | replica_factor | 1                |
      | vid_type       | FIXED_STRING(30) |
    And import "nba" csv data
    And having executed:
      """
      CREATE TAG IF NOT EXISTS `test_tag`(name string)
      """
    And wait 3 seconds
    When executing query:
      """
      MATCH (v:player{name: "Tim Duncan"})
      RETURN v.name AS Name
      """
    Then the result should be, in any order:
      | Name         |
      | "Tim Duncan" |
    When executing query:
      """
      MATCH (v:player{name: "Tim Duncan"})
      RETURN v.name AS Name
      """
    Then the result should include:
      | Name         |
      | "Tim Duncan" |
    When executing query:
      """
      SHOW HOSTS
      """
    Then the execution should be successful
    When executing query:
      """
      YIELD $-.id AS id
      """
    Then a SemanticError should be raised at runtime: `$-.id', not exist prop `id'
    Then drop the used space
