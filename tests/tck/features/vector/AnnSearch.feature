Feature: Ann Search Tests

  Background:
    Given an empty graph
    And create a space with following options:
      | partition_num  | 1                |
      | replica_factor | 1                |
      | vid_type       | FIXED_STRING(30) |
      | charset        | utf8             |
      | collate        | utf8_bin         |

  Scenario: Test Ann Search
    # Create tags with vector properties
    When executing query:
      """
      CREATE TAG player_vector(name string, age int, embedding vector(3));
      CREATE TAG coach_vector(name string, experience int, embedding vector(3));
      """
    Then the execution should be successful
    And wait 6 seconds
    # Insert test data - players
    When executing query:
      """
      INSERT VERTEX
        player_vector(name, age, embedding)
      VALUES
        "LeBron James": ("LeBron James", 39, vector(0.91, 0.82, 0.87)),
        "Stephen Curry": ("Stephen Curry", 36, vector(0.85, 0.95, 0.78)),
        "Kevin Durant": ("Kevin Durant", 35, vector(0.88, 0.90, 0.85)),
        "Michael Jordan": ("Michael Jordan", 60, vector(0.98, 0.96, 0.99)),
        "Kobe Bryant": ("Kobe Bryant", 41, vector(0.92, 0.88, 0.94)),
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
        "Klay Thompson": ("Klay Thompson", 34, vector(0.84, 0.89, 0.83));
      """
    Then the execution should be successful
    # Insert test data - coaches
    When executing query:
      """
      INSERT VERTEX
        coach_vector(name, experience, embedding)
      VALUES
        "Phil Jackson": ("Phil Jackson", 30, vector(0.92, 0.88, 0.95)),
        "Gregg Popovich": ("Gregg Popovich", 35, vector(0.89, 0.91, 0.87)),
        "Pat Riley": ("Pat Riley", 28, vector(0.90, 0.86, 0.92)),
        "Red Auerbach": ("Red Auerbach", 25, vector(0.88, 0.93, 0.89)),
        "Don Nelson": ("Don Nelson", 31, vector(0.85, 0.87, 0.91)),
        "Chuck Daly": ("Chuck Daly", 22, vector(0.87, 0.89, 0.86)),
        "Jerry Sloan": ("Jerry Sloan", 26, vector(0.86, 0.90, 0.88)),
        "Larry Brown": ("Larry Brown", 29, vector(0.88, 0.85, 0.90)),
        "Lenny Wilkens": ("Lenny Wilkens", 32, vector(0.84, 0.88, 0.87)),
        "Steve Kerr": ("Steve Kerr", 15, vector(0.91, 0.89, 0.86)),
        "Erik Spoelstra": ("Erik Spoelstra", 15, vector(0.89, 0.86, 0.93)),
        "Doc Rivers": ("Doc Rivers", 24, vector(0.86, 0.89, 0.91)),
        "Rick Carlisle": ("Rick Carlisle", 21, vector(0.87, 0.88, 0.90)),
        "Mike D'Antoni": ("Mike D'Antoni", 19, vector(0.85, 0.92, 0.88)),
        "George Karl": ("George Karl", 27, vector(0.88, 0.87, 0.89)),
        "Mike Budenholzer": ("Mike Budenholzer", 10, vector(0.84, 0.90, 0.87)),
        "Nick Nurse": ("Nick Nurse", 5, vector(0.86, 0.88, 0.92)),
        "Tom Thibodeau": ("Tom Thibodeau", 12, vector(0.83, 0.89, 0.88)),
        "Rick Adelman": ("Rick Adelman", 23, vector(0.87, 0.85, 0.90)),
        "Rudy Tomjanovich": ("Rudy Tomjanovich", 13, vector(0.89, 0.87, 0.91)),
        "Dick Motta": ("Dick Motta", 25, vector(0.85, 0.88, 0.86)),
        "Bill Fitch": ("Bill Fitch", 25, vector(0.86, 0.87, 0.89)),
        "Mike Fratello": ("Mike Fratello", 17, vector(0.84, 0.86, 0.88)),
        "Hubie Brown": ("Hubie Brown", 15, vector(0.87, 0.89, 0.85)),
        "Jack Ramsay": ("Jack Ramsay", 21, vector(0.88, 0.86, 0.90)),
        "Monty Williams": ("Monty Williams", 8, vector(0.85, 0.91, 0.87)),
        "Taylor Jenkins": ("Taylor Jenkins", 4, vector(0.83, 0.88, 0.89)),
        "Willie Green": ("Willie Green", 2, vector(0.82, 0.87, 0.86)),
        "Michael Malone": ("Michael Malone", 10, vector(0.86, 0.89, 0.88)),
        "Darvin Ham": ("Darvin Ham", 1, vector(0.81, 0.85, 0.87));
      """
    Then the execution should be successful
    # Create ann index for both tags
    When executing query:
      """
      CREATE TAG ANNINDEX multi_embedding_ivf_index ON player_vector,coach_vector::(embedding) {ANNINDEX_TYPE:"IVF", DIM:3, METRIC_TYPE:L2, NLIST:2, TRAINSIZE:2};
      """
    Then the execution should be successful
    And wait 6 seconds
    # Test ann search across multiple tags with IVF
    When executing query:
      """
      MATCH (v:player_vector:coach_vector)
      RETURN v.player_vector.name AS player_name,
             v.coach_vector.name AS coach_name,
             labels(v) AS labels,
             v.embedding AS embedding,
             euclidean(vector(0.90, 0.85, 0.88), v.embedding) AS distance
      ORDER BY euclidean(vector(0.90, 0.85, 0.88), v.embedding)
      APPROXIMATE
      LIMIT 3
      OPTIONS {ANNINDEX_TYPE:'IVF', METRIC_TYPE:L2, NPROBE:2};
      """
    Then the result should be, in any order:
      | player_name             | coach_name    | labels            | embedding                                                        | distance             |
      | "Giannis Antetokounmpo" | NULL          | ["player_vector"] | vector(0.91,0.85,0.89) | 0.014142164283650742 |
      | "Joel Embiid"           | NULL          | ["player_vector"] | vector(0.89,0.83,0.87) | 0.02449492273469006  |
      | NULL                    | "Larry Brown" | ["coach_vector"]  | vector(0.88,0.85,0.90) | 0.028284244273478854 |
    # Drop the ann index
    When executing query:
      """
      DROP TAG ANNINDEX multi_embedding_ivf_index;
      """
    Then the execution should be successful
    And wait 3 seconds
    # Create separate ann index for player_vector
    When executing query:
      """
      CREATE TAG ANNINDEX player_embedding_ivf_index ON player_vector::(embedding) {ANNINDEX_TYPE:"IVF", DIM:3, METRIC_TYPE:L2, NLIST:2, TRAINSIZE:2};
      """
    Then the execution should be successful
    And wait 6 seconds
    # Test ann search on player_vector only
    When executing query:
      """
      MATCH (v:player_vector)
      RETURN v,
             euclidean(vector(0.90, 0.85, 0.88), v.embedding) AS distance
      ORDER BY euclidean(vector(0.90, 0.85, 0.88), v.embedding)
      APPROXIMATE
      LIMIT 1
      OPTIONS {ANNINDEX_TYPE:'IVF', METRIC_TYPE:L2, NPROBE:2};
      """
    Then the result should be, in any order:
      | v                                                                                                                                                             | distance             |
      | ("Giannis Antetokounmpo" :player_vector{age: 29, embedding: vector(0.91,0.85,0.89), name: "Giannis Antetokounmpo"}) | 0.014142164283650742 |
    # Drop player ann index
    When executing query:
      """
      DROP TAG ANNINDEX player_embedding_ivf_index;
      """
    Then the execution should be successful
    And wait 3 seconds
    # Create separate ann index for coach_vector
    When executing query:
      """
      CREATE TAG ANNINDEX coach_embedding_ivf_index ON coach_vector::(embedding) {ANNINDEX_TYPE:"IVF", DIM:3, METRIC_TYPE:L2, NLIST:2, TRAINSIZE:2};
      """
    Then the execution should be successful
    And wait 6 seconds
    # Test ann search on coach_vector only
    When executing query:
      """
      MATCH (v:coach_vector)
      RETURN v,
             euclidean(vector(0.90, 0.85, 0.88), v.embedding) AS distance
      ORDER BY euclidean(vector(0.90, 0.85, 0.88), v.embedding)
      APPROXIMATE
      LIMIT 1
      OPTIONS {ANNINDEX_TYPE:'IVF', METRIC_TYPE:L2, NPROBE:2};
      """
    Then the result should be, in any order:
      | v                                                                                                                                               | distance             |
      | ("Larry Brown" :coach_vector{embedding: vector(0.88,0.85,0.90), experience: 29, name: "Larry Brown"}) | 0.028284244273478854 |
    # Drop coach ann index
    When executing query:
      """
      DROP TAG ANNINDEX coach_embedding_ivf_index;
      """
    Then the execution should be successful
    Then drop the used space
