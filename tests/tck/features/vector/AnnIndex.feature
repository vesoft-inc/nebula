Feature: Ann Index Tests

  Background:
    Given an empty graph
    And create a space with following options:
      | partition_num  | 1                |
      | replica_factor | 1                |
      | vid_type       | FIXED_STRING(30) |
      | charset        | utf8             |
      | collate        | utf8_bin         |

  Scenario: Test Create Tag Ann Index
    # Create tags with vector properties
    When executing query:
      """
      CREATE TAG player_vector(name string, age int, embedding vector(3));
      CREATE TAG team_vector(name string, location vector(3));
      """
    Then the execution should be successful
    And wait 6 seconds
    # Insert test data with vector properties - 40 players
    When executing query:
      """
      INSERT VERTEX
        player_vector(name, age, embedding)
      VALUES
        "LeBron James": ("LeBron James", 39, vector(0.91, 0.82, 0.87)),
        "Stephen Curry": ("Stephen Curry", 36, vector(0.85, 0.95, 0.78)),
        "Kevin Durant": ("Kevin Durant", 35, vector(0.88, 0.90, 0.85)),
        "Kobe Bryant": ("Kobe Bryant", 41, vector(0.92, 0.88, 0.94)),
        "Michael Jordan": ("Michael Jordan", 60, vector(0.98, 0.96, 0.99)),
        "Magic Johnson": ("Magic Johnson", 64, vector(0.89, 0.92, 0.86)),
        "Larry Bird": ("Larry Bird", 67, vector(0.87, 0.89, 0.91)),
        "Shaquille O'Neal": ("Shaquille O'Neal", 52, vector(0.94, 0.85, 0.89)),
        "Tim Duncan": ("Tim Duncan", 48, vector(0.86, 0.88, 0.92)),
        "Kareem Abdul-Jabbar": ("Kareem Abdul-Jabbar", 76, vector(0.93, 0.91, 0.88)),
        "Wilt Chamberlain": ("Wilt Chamberlain", 63, vector(0.96, 0.84, 0.87)),
        "Bill Russell": ("Bill Russell", 89, vector(0.84, 0.95, 0.93)),
        "Hakeem Olajuwon": ("Hakeem Olajuwon", 61, vector(0.89, 0.87, 0.90)),
        "David Robinson": ("David Robinson", 58, vector(0.85, 0.86, 0.88)),
        "Charles Barkley": ("Charles Barkley", 60, vector(0.88, 0.83, 0.85)),
        "Karl Malone": ("Karl Malone", 60, vector(0.87, 0.84, 0.86)),
        "John Stockton": ("John Stockton", 61, vector(0.82, 0.94, 0.89)),
        "Oscar Robertson": ("Oscar Robertson", 85, vector(0.90, 0.91, 0.87)),
        "Jerry West": ("Jerry West", 85, vector(0.88, 0.89, 0.85)),
        "Elgin Baylor": ("Elgin Baylor", 87, vector(0.86, 0.87, 0.83)),
        "Giannis Antetokounmpo": ("Giannis Antetokounmpo", 29, vector(0.91, 0.85, 0.89)),
        "Luka Doncic": ("Luka Doncic", 25, vector(0.87, 0.90, 0.84)),
        "Jayson Tatum": ("Jayson Tatum", 26, vector(0.84, 0.86, 0.82)),
        "Joel Embiid": ("Joel Embiid", 30, vector(0.89, 0.83, 0.87)),
        "Nikola Jokic": ("Nikola Jokic", 29, vector(0.86, 0.91, 0.88)),
        "Kawhi Leonard": ("Kawhi Leonard", 33, vector(0.88, 0.84, 0.90)),
        "Paul George": ("Paul George", 34, vector(0.85, 0.87, 0.83)),
        "Jimmy Butler": ("Jimmy Butler", 34, vector(0.83, 0.85, 0.88)),
        "Damian Lillard": ("Damian Lillard", 34, vector(0.86, 0.92, 0.81)),
        "Russell Westbrook": ("Russell Westbrook", 35, vector(0.89, 0.88, 0.84)),
        "James Harden": ("James Harden", 34, vector(0.87, 0.90, 0.82)),
        "Chris Paul": ("Chris Paul", 39, vector(0.81, 0.93, 0.87)),
        "Kyrie Irving": ("Kyrie Irving", 32, vector(0.88, 0.91, 0.79)),
        "Anthony Davis": ("Anthony Davis", 31, vector(0.86, 0.82, 0.89)),
        "Klay Thompson": ("Klay Thompson", 34, vector(0.84, 0.89, 0.83)),
        "Draymond Green": ("Draymond Green", 34, vector(0.79, 0.85, 0.91)),
        "Brook Lopez": ("Brook Lopez", 36, vector(0.82, 0.80, 0.85)),
        "Al Horford": ("Al Horford", 38, vector(0.80, 0.83, 0.87)),
        "Kyle Lowry": ("Kyle Lowry", 38, vector(0.78, 0.86, 0.84)),
        "DeRozan": ("DeRozan", 34, vector(0.85, 0.84, 0.81));
      """
    Then the execution should be successful
    When executing query:
      """
      INSERT VERTEX
        team_vector(name, location)
      VALUES
        "Lakers": ("Lakers", vector(0.85, 0.90, 0.78)),
        "Warriors": ("Warriors", vector(0.82, 0.95, 0.88)),
        "Celtics": ("Celtics", vector(0.90, 0.87, 0.92)),
        "Heat": ("Heat", vector(0.88, 0.83, 0.85)),
        "Bulls": ("Bulls", vector(0.91, 0.89, 0.84)),
        "Spurs": ("Spurs", vector(0.87, 0.85, 0.90)),
        "Nets": ("Nets", vector(0.83, 0.88, 0.86)),
        "76ers": ("76ers", vector(0.86, 0.82, 0.89)),
        "Nuggets": ("Nuggets", vector(0.84, 0.91, 0.87)),
        "Bucks": ("Bucks", vector(0.89, 0.86, 0.83)),
        "Suns": ("Suns", vector(0.81, 0.84, 0.88)),
        "Clippers": ("Clippers", vector(0.88, 0.87, 0.82)),
        "Mavs": ("Mavs", vector(0.85, 0.89, 0.86)),
        "Jazz": ("Jazz", vector(0.83, 0.85, 0.87)),
        "Trail Blazers": ("Trail Blazers", vector(0.80, 0.86, 0.84)),
        "Kings": ("Kings", vector(0.82, 0.84, 0.81)),
        "Hawks": ("Hawks", vector(0.79, 0.83, 0.85)),
        "Knicks": ("Knicks", vector(0.87, 0.81, 0.89)),
        "Raptors": ("Raptors", vector(0.85, 0.88, 0.83)),
        "Magic": ("Magic", vector(0.78, 0.82, 0.86));
      """
    Then the execution should be successful
    # Test create single tag ann index - IVF type
    When executing query:
      """
      CREATE TAG ANNINDEX player_embedding_ivf_index ON player_vector::(embedding) {ANNINDEX_TYPE:"IVF", DIM:3, METRIC_TYPE:L2, NLIST:2, TRAINSIZE:2};
      """
    Then the execution should be successful
    # Test create single tag ann index - HNSW type
    When executing query:
      """
      CREATE TAG ANNINDEX team_location_hnsw_index ON team_vector::(location) {ANNINDEX_TYPE:"HNSW", DIM:3, METRIC_TYPE:L2, MAXDEGREE:16, EFCONSTRUCTION:200, MAXELEMENTS:1000};
      """
    Then the execution should be successful
    # Test IF NOT EXISTS
    When executing query:
      """
      CREATE TAG ANNINDEX IF NOT EXISTS player_embedding_ivf_index ON player_vector::(embedding) {ANNINDEX_TYPE:"IVF", DIM:3, METRIC_TYPE:L2, NLIST:8, TRAINSIZE:3};
      """
    Then the execution should be successful
    # Test duplicate index creation should fail
    When executing query:
      """
      CREATE TAG ANNINDEX player_embedding_ivf_index ON player_vector::(embedding) {ANNINDEX_TYPE:"IVF", DIM:3, METRIC_TYPE:L2, NLIST:2, TRAINSIZE:2};
      """
    Then a ExecutionError should be raised at runtime:
    # Test create index on non-existent tag should fail
    When executing query:
      """
      CREATE TAG ANNINDEX non_exist_index ON non_exist_tag::(embedding) {ANNINDEX_TYPE:"IVF", DIM:3, METRIC_TYPE:L2, NLIST:2, TRAINSIZE:2};
      """
    Then a ExecutionError should be raised at runtime:
    # Test create index on non-existent property should fail
    When executing query:
      """
      CREATE TAG ANNINDEX non_exist_property_index ON player_vector::(non_exist_property) {ANNINDEX_TYPE:"IVF", DIM:3, METRIC_TYPE:L2, NLIST:2, TRAINSIZE:2};
      """
    Then a ExecutionError should be raised at runtime:
    # Test create index on non-vector property should fail
    When executing query:
      """
      CREATE TAG ANNINDEX non_vector_property_index ON player_vector::(name) {ANNINDEX_TYPE:"IVF", DIM:3, METRIC_TYPE:L2, NLIST:2, TRAINSIZE:2};
      """
    Then a ExecutionError should be raised at runtime:
    # Test create index with wrong dimension should fail
    When executing query:
      """
      CREATE TAG ANNINDEX wrong_dim_index ON player_vector::(embedding) {ANNINDEX_TYPE:"IVF", DIM:64, METRIC_TYPE:L2, NLIST:8, TRAINSIZE:3};
      """
    Then a ExecutionError should be raised at runtime:
    And wait 6 seconds
    # Test SHOW TAG ANN INDEXES
    When executing query:
      """
      SHOW TAG ANNINDEXES;
      """
    Then the result should contain:
      | Index Name                   | By Tag            | Columns                     |
      | "player_embedding_ivf_index" | ["player_vector"] | ["player_vector.embedding"] |
      | "team_location_hnsw_index"   | ["team_vector"]   | ["team_vector.location"]    |
    # Test SHOW TAG ANN INDEXES BY tag
    When executing query:
      """
      SHOW TAG ANNINDEXES BY player_vector;
      """
    Then the result should contain:
      | Index Name                   | Columns                     |
      | "player_embedding_ivf_index" | ["player_vector.embedding"] |
    # Test DESCRIBE TAG ANN INDEX
    When executing query:
      """
      DESCRIBE TAG ANNINDEX player_embedding_ivf_index;
      """
    Then the result should be, in any order:
      | Field                     | Type        |
      | "player_vector.embedding" | "vector(3)" |
    # Test SHOW CREATE TAG ANN INDEX
    When executing query:
      """
      SHOW CREATE TAG ANNINDEX player_embedding_ivf_index;
      """
    Then the execution should be successful
    # Test Drop TAG ANN INDEX
    When executing query:
      """
      DROP TAG ANNINDEX player_embedding_ivf_index;
      """
    Then the execution should be successful
    When executing query:
      """
      SHOW TAG ANNINDEXES;
      """
    Then the result should be, in any order:
      | Index Name                 | By Tag          | Columns                  |
      | "team_location_hnsw_index" | ["team_vector"] | ["team_vector.location"] |
    When executing query:
      """
      DROP TAG ANNINDEX team_location_hnsw_index;
      """
    Then the execution should be successful
    When executing query:
      """
      SHOW TAG ANNINDEXES;
      """
    Then the result should be, in any order:
      | Index Name | By Tag | Columns |

  Scenario: Test Create Edge Ann Index
    # Create edges with vector properties
    When executing query:
      """
      CREATE EDGE player_relation(relation_type string, similarity vector(3));
      CREATE EDGE team_interaction(interaction_type string, weight vector(3));
      """
    Then the execution should be successful
    And wait 6 seconds
    # Insert test data with vector properties - 40 edges
    When executing query:
      """
      INSERT EDGE
        player_relation(relation_type, similarity)
      VALUES
        "LeBron James" -> "Anthony Davis": ("teammate", vector(0.88, 0.85, 0.90)),
        "Stephen Curry" -> "Klay Thompson": ("teammate", vector(0.92, 0.95, 0.85)),
        "Kevin Durant" -> "Stephen Curry": ("former_teammate", vector(0.90, 0.88, 0.92)),
        "Kobe Bryant" -> "Shaquille O'Neal": ("former_teammate", vector(0.95, 0.92, 0.94)),
        "Michael Jordan" -> "Scottie Pippen": ("teammate", vector(0.98, 0.96, 0.99)),
        "Magic Johnson" -> "Kareem Abdul-Jabbar": ("teammate", vector(0.89, 0.92, 0.86)),
        "Larry Bird" -> "Kevin McHale": ("teammate", vector(0.87, 0.89, 0.91)),
        "Tim Duncan" -> "Tony Parker": ("teammate", vector(0.86, 0.88, 0.92)),
        "LeBron James" -> "Dwyane Wade": ("former_teammate", vector(0.93, 0.91, 0.88)),
        "Stephen Curry" -> "Kevin Durant": ("former_teammate", vector(0.90, 0.87, 0.89)),
        "Giannis Antetokounmpo" -> "Khris Middleton": ("teammate", vector(0.85, 0.86, 0.88)),
        "Luka Doncic" -> "Kristaps Porzingis": ("former_teammate", vector(0.82, 0.84, 0.81)),
        "Jayson Tatum" -> "Jaylen Brown": ("teammate", vector(0.86, 0.87, 0.83)),
        "Joel Embiid" -> "James Harden": ("teammate", vector(0.84, 0.82, 0.86)),
        "Nikola Jokic" -> "Jamal Murray": ("teammate", vector(0.88, 0.91, 0.85)),
        "Kawhi Leonard" -> "Paul George": ("teammate", vector(0.87, 0.84, 0.90)),
        "Jimmy Butler" -> "Bam Adebayo": ("teammate", vector(0.83, 0.85, 0.88)),
        "Damian Lillard" -> "CJ McCollum": ("former_teammate", vector(0.81, 0.86, 0.84)),
        "Russell Westbrook" -> "Kevin Durant": ("former_teammate", vector(0.89, 0.88, 0.84)),
        "James Harden" -> "Chris Paul": ("former_teammate", vector(0.87, 0.90, 0.82)),
        "Chris Paul" -> "Blake Griffin": ("former_teammate", vector(0.85, 0.83, 0.87)),
        "Kyrie Irving" -> "LeBron James": ("former_teammate", vector(0.88, 0.91, 0.79)),
        "Anthony Davis" -> "Russell Westbrook": ("teammate", vector(0.84, 0.80, 0.83)),
        "Klay Thompson" -> "Draymond Green": ("teammate", vector(0.89, 0.86, 0.91)),
        "Brook Lopez" -> "Giannis Antetokounmpo": ("teammate", vector(0.82, 0.84, 0.85)),
        "Al Horford" -> "Jayson Tatum": ("teammate", vector(0.80, 0.83, 0.87)),
        "Kyle Lowry" -> "DeMar DeRozan": ("former_teammate", vector(0.78, 0.86, 0.84)),
        "DeRozan" -> "Zach LaVine": ("teammate", vector(0.85, 0.84, 0.81)),
        "LeBron James" -> "Kevin Love": ("former_teammate", vector(0.86, 0.82, 0.88)),
        "Stephen Curry" -> "Andre Iguodala": ("former_teammate", vector(0.83, 0.87, 0.85)),
        "Kevin Durant" -> "Russell Westbrook": ("former_teammate", vector(0.91, 0.85, 0.89)),
        "Kobe Bryant" -> "Pau Gasol": ("former_teammate", vector(0.87, 0.90, 0.84)),
        "Michael Jordan" -> "Dennis Rodman": ("teammate", vector(0.89, 0.83, 0.91)),
        "Magic Johnson" -> "James Worthy": ("teammate", vector(0.84, 0.88, 0.86)),
        "Larry Bird" -> "Robert Parish": ("teammate", vector(0.82, 0.85, 0.89)),
        "Tim Duncan" -> "Manu Ginobili": ("teammate", vector(0.88, 0.86, 0.92)),
        "Shaquille O'Neal" -> "Dwyane Wade": ("former_teammate", vector(0.85, 0.89, 0.83)),
        "Hakeem Olajuwon" -> "Clyde Drexler": ("teammate", vector(0.87, 0.84, 0.88)),
        "David Robinson" -> "Tim Duncan": ("former_teammate", vector(0.83, 0.86, 0.85)),
        "Charles Barkley" -> "Kevin Johnson": ("former_teammate", vector(0.81, 0.84, 0.82));
      """
    Then the execution should be successful
    When executing query:
      """
      INSERT EDGE
        team_interaction(interaction_type, weight)
      VALUES
        "Lakers" -> "Warriors": ("rival", vector(0.85, 0.90, 0.78)),
        "Warriors" -> "Clippers": ("conference", vector(0.82, 0.88, 0.86)),
        "Celtics" -> "Lakers": ("historic_rival", vector(0.90, 0.87, 0.92)),
        "Heat" -> "Celtics": ("conference", vector(0.88, 0.83, 0.85)),
        "Bulls" -> "Pistons": ("historic_rival", vector(0.91, 0.89, 0.84)),
        "Spurs" -> "Lakers": ("conference", vector(0.87, 0.85, 0.90)),
        "Nets" -> "76ers": ("division", vector(0.83, 0.88, 0.86)),
        "Nuggets" -> "Lakers": ("conference", vector(0.84, 0.91, 0.87)),
        "Bucks" -> "Celtics": ("conference", vector(0.89, 0.86, 0.83)),
        "Suns" -> "Lakers": ("division", vector(0.81, 0.84, 0.88)),
        "Clippers" -> "Lakers": ("city_rival", vector(0.88, 0.87, 0.82)),
        "Mavs" -> "Spurs": ("division", vector(0.85, 0.89, 0.86)),
        "Jazz" -> "Nuggets": ("division", vector(0.83, 0.85, 0.87)),
        "Trail Blazers" -> "Warriors": ("division", vector(0.80, 0.86, 0.84)),
        "Kings" -> "Warriors": ("division", vector(0.82, 0.84, 0.81)),
        "Hawks" -> "Heat": ("conference", vector(0.79, 0.83, 0.85)),
        "Knicks" -> "Nets": ("city_rival", vector(0.87, 0.81, 0.89)),
        "Raptors" -> "Celtics": ("conference", vector(0.85, 0.88, 0.83)),
        "Magic" -> "Heat": ("division", vector(0.78, 0.82, 0.86)),
        "Lakers" -> "Celtics": ("finals_rival", vector(0.92, 0.95, 0.89));
      """
    Then the execution should be successful
    # Test create single edge ann index - IVF type
    When executing query:
      """
      CREATE EDGE ANNINDEX player_similarity_ivf_index ON player_relation::(similarity) {ANNINDEX_TYPE:"IVF", DIM:3, METRIC_TYPE:L2, NLIST:2, TRAINSIZE:2};
      """
    Then the execution should be successful
    # Test create single edge ann index - HNSW type
    When executing query:
      """
      CREATE EDGE ANNINDEX team_weight_hnsw_index ON team_interaction::(weight) {ANNINDEX_TYPE:"HNSW", DIM:3, METRIC_TYPE:L2, MAXDEGREE:16, EFCONSTRUCTION:200, MAXELEMENTS:1000};
      """
    Then the execution should be successful
    # Test IF NOT EXISTS
    When executing query:
      """
      CREATE EDGE ANNINDEX IF NOT EXISTS player_similarity_ivf_index ON player_relation::(similarity) {ANNINDEX_TYPE:"IVF", DIM:3, METRIC_TYPE:L2, NLIST:2, TRAINSIZE:2};
      """
    Then the execution should be successful
    # Test duplicate index creation should fail
    When executing query:
      """
      CREATE EDGE ANNINDEX player_similarity_ivf_index ON player_relation::(similarity) {ANNINDEX_TYPE:"IVF", DIM:3, METRIC_TYPE:L2, NLIST:2, TRAINSIZE:2};
      """
    Then a ExecutionError should be raised at runtime:
    # Test create index on non-existent edge should fail
    When executing query:
      """
      CREATE EDGE ANNINDEX non_exist_index ON non_exist_edge::(similarity) {ANNINDEX_TYPE:"IVF", DIM:3, METRIC_TYPE:L2, NLIST:2, TRAINSIZE:2};
      """
    Then a ExecutionError should be raised at runtime:
    # Test create index on non-existent property should fail
    When executing query:
      """
      CREATE EDGE ANNINDEX non_exist_property_index ON player_relation::(non_exist_property) {ANNINDEX_TYPE:"IVF", DIM:3, METRIC_TYPE:L2, NLIST:2, TRAINSIZE:2};
      """
    Then a ExecutionError should be raised at runtime:
    # Test create index on non-vector property should fail
    When executing query:
      """
      CREATE EDGE ANNINDEX non_vector_property_index ON player_relation::(relation_type) {ANNINDEX_TYPE:"IVF", DIM:3, METRIC_TYPE:L2, NLIST:2, TRAINSIZE:2};
      """
    Then a ExecutionError should be raised at runtime:
    # Test create index with wrong dimension should fail
    When executing query:
      """
      CREATE EDGE ANNINDEX wrong_dim_index ON player_relation::(similarity) {ANNINDEX_TYPE:"IVF", DIM:128, METRIC_TYPE:L2, NLIST:2, TRAINSIZE:2};
      """
    Then a ExecutionError should be raised at runtime:
    And wait 6 seconds
    # Test SHOW EDGE ANN INDEXES
    When executing query:
      """
      SHOW EDGE ANNINDEXES;
      """
    Then the result should contain:
      | Index Name                    | By Edge            | Columns                        |
      | "player_similarity_ivf_index" | ["player_relation"]  | ["player_relation.similarity"] |
      | "team_weight_hnsw_index"      | ["team_interaction"] | ["team_interaction.weight"]    |
    # Test SHOW EDGE ANN INDEXES BY edge
    When executing query:
      """
      SHOW EDGE ANNINDEXES BY player_relation;
      """
    Then the result should contain:
      | Index Name                    | Columns                        |
      | "player_similarity_ivf_index" | ["player_relation.similarity"] |
    # Test DESCRIBE EDGE ANN INDEX
    When executing query:
      """
      DESCRIBE EDGE ANNINDEX player_similarity_ivf_index;
      """
    Then the result should be, in any order:
      | Field        | Type        |
      | "player_relation.similarity" | "vector(3)" |
    # Test SHOW CREATE EDGE ANN INDEX
    When executing query:
      """
      SHOW CREATE EDGE ANNINDEX player_similarity_ivf_index;
      """
    Then the execution should be successful

    # Test Drop Edge Ann Index
    When executing query:
      """
      DROP EDGE ANNINDEX player_similarity_ivf_index;
      """
    Then the execution should be successful
    When executing query:
      """
      SHOW EDGE ANNINDEXES;
      """
    Then the result should be, in any order:
      | Index Name               | By Edge            | Columns                     |
      | "team_weight_hnsw_index" | ["team_interaction"] | ["team_interaction.weight"] |
    When executing query:
      """
      DROP EDGE ANNINDEX team_weight_hnsw_index;
      """
    Then the execution should be successful
    When executing query:
      """
      SHOW EDGE ANNINDEXES;
      """
    Then the result should be, in any order:
      | Index Name | By Edge | Columns |
    Then drop the used space
