Feature: Submit job space requirements

  Scenario: submit job require space
    Given an empty graph
    When executing query:
      """
      SUBMIT JOB COMPACT;
      """
    Then a SemanticError should be raised at runtime:
    When executing query:
      """
      SUBMIT JOB FLUSH;
      """
    Then a SemanticError should be raised at runtime:
    When executing query:
      """
      SUBMIT JOB STATS;
      """
    Then a SemanticError should be raised at runtime:
    When executing query:
      """
      REBUILD TAG INDEX not_exists_index;
      """
    Then a SemanticError should be raised at runtime:
    When executing query:
      """
      REBUILD EDGE INDEX not_exists_index;
      """
    Then a SemanticError should be raised at runtime:
