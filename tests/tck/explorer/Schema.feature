Feature: Create space as another space

  Scenario: clone space explorer
    # Space
    Given an empty graph
    And create a space with following options:
      | partition_num  | 9                |
      | replica_factor | 1                |
      | vid_type       | FIXED_STRING(20) |
    When executing query:
      """
      CREATE tag `test_Tag@@##--  __` (`string->int@@##  --` string NULL)
      """
    Then the execution should be successful
    When executing query:
      """
      ALTER tag `test_Tag@@##--  __` ADD (`test_Tag@@##-- __` string NULL)
      """
    Then the execution should be successful
    When executing query:
      """
      ALTER tag `test_Tag@@##--  __` ADD (`int_( )!@#$ $%` int NULL)
      """
    Then the execution should be successful
    When executing query:
      """
      ALTER tag `test_Tag@@##--  __` CHANGE (`string->int@@##  --` int NULL)
      """
    Then the execution should be successful
    When executing query:
      """
      CREATE edge `test_edge##$$ ()^*` (`int->string!@#$ *&!@$` int NULL  )
      """
    Then the execution should be successful
    When executing query:
      """
      ALTER edge `test_edge##$$ ()^*` ADD (`string!@#$ *&!@$` string NULL)
      """
    Then the execution should be successful
    When executing query:
      """
      ALTER edge `test_edge##$$ ()^*` ADD (`int_!@#$ *&!@$` int NULL)
      """
    Then the execution should be successful
    When executing query:
      """
      ALTER edge `test_edge##$$ ()^*` CHANGE (`int->string!@#$ *&!@$` string NULL)
      """
    Then the execution should be successful
    When executing query:
      """
      CREATE TAG INDEX `test_index` on `test_Tag@@##--  __`(`test_Tag@@##-- __`(10), `int_( )!@#$ $%`)
      """
    Then the execution should be successful
    When executing query:
      """
      DROP tag INDEX `test_index`
      """
    Then the execution should be successful
    When executing query:
      """
      ALTER edge `test_edge##$$ ()^*` DROP (`int->string!@#$ *&!@$`)
      """
    Then the execution should be successful
    When executing query:
      """
      DROP EDGE `test_edge##$$ ()^*`
      """
    When executing query:
      """
      ALTER tag `test_Tag@@##--  __` DROP (`string->int@@##  --`)
      """
    Then the execution should be successful
    When executing query:
      """
      DROP TAG `test_Tag@@##--  __`
      """
    Then the execution should be successful
    Then drop the used space
