# Copyright (c) 2021 vesoft inc. All rights reserved.
#
# This source code is licensed under Apache 2.0 License.
Feature: LDBC Business Intelligence Workload - Read

  Background:
    Given a graph with space named "ldbc-v0.3.3"

  Scenario: 1. Posting summary
    When executing query:
    """
    MATCH (message:Message)
    WHERE toInteger(message.creationDate) < 20110721220000000
    WITH count(message) AS totalMessageCountInt
    WITH toFloat(totalMessageCountInt) AS totalMessageCount
    MATCH (message:Message)
    WHERE toInteger(message.creationDate) < 20110721220000000
      AND message.content IS NOT NULL
    WITH
      totalMessageCount,
      message,
      toInteger(message.creationDate)/10000000000000 AS year
    WITH
      totalMessageCount,
      year,
      `Comment` IN tags(message) AS isComment,
      CASE
        WHEN message.length <  40 THEN 0
        WHEN message.length <  80 THEN 1
        WHEN message.length < 160 THEN 2
        ELSE                           3
      END AS lengthCategory,
      count(message) AS messageCount,
      floor(avg(message.length)) AS averageMessageLength,
      sum(message.length) AS sumMessageLength
    RETURN
      year,
      isComment,
      lengthCategory,
      messageCount,
      averageMessageLength,
      sumMessageLength,
      messageCount / totalMessageCount AS percentageOfMessages
    ORDER BY
      year DESC,
      isComment ASC,
      lengthCategory ASC
    """
    Then the result should be, in order:
    | year | isComment | lengthCategory | messageCount | averageMessageLength | sumMessageLength | percentageOfMessages |

  Scenario: 2. Top tags for country, age, gender, time
    When executing query:
    """
    MATCH
      (country:Country)<-[:IS_PART_OF]-(:City)<-[:IS_LOCATED_IN]-(person:Person)
      <-[:HAS_CREATOR]-(message:Message)-[:HAS_TAG]->(`tag`:`Tag`)
    WHERE message.creationDate >= "20091231230000000"
      AND message.creationDate <= "20101107230000000"
      AND (id(country) == "Ethiopia" OR id(country) == "Belarus")
    WITH
      country.name AS countryName,
      toInteger(message.creationDate)/100000000000%100 AS month,
      person.gender AS gender,
      floor((20130101 - person.birthday) / 10000 / 5.0) AS ageGroup,
      `tag`.name AS tagName,
      message
    WITH
      countryName, month, gender, ageGroup, tagName, count(message) AS messageCount
    WHERE messageCount > 100
    RETURN
      countryName,
      month,
      gender,
      ageGroup,
      tagName,
      messageCount
    ORDER BY
      messageCount DESC,
      tagName ASC,
      ageGroup ASC,
      gender ASC,
      month ASC,
      countryName ASC
    LIMIT 100
    """
    Then the result should be, in order:
    | countryName | month | gender | ageGroup | tagName | messageCount |

  @skip
  Scenario: 3. Tag evolution
    # TODO: Need an index on tag `Tag`, and fix the expr rewrite bug on toInteger(message1.creationDate)/100000000000%100
    When executing query:
    """
    WITH
      2010 AS year1,
      10 AS month1,
      2010 + toInteger(10 / 12.0) AS year2,
      10 % 12 + 1 AS month2
    MATCH (`tag`:`Tag`)
    OPTIONAL MATCH (message1:Message)-[:HAS_TAG]->(`tag`)
      WHERE toInteger(message1.creationDate)/10000000000000   == year1
        AND toInteger(message1.creationDate)/100000000000%100 == month1
    WITH year2, month2, `tag`, count(message1) AS countMonth1
    OPTIONAL MATCH (message2:Message)-[:HAS_TAG]->(`tag`)
      WHERE toInteger(message2.creationDate)/10000000000000   == year2
        AND toInteger(message2.creationDate)/100000000000%100 == month2
    WITH
      `tag`,
      countMonth1,
      count(message2) AS countMonth2
    RETURN
      `tag`.name,
      countMonth1,
      countMonth2,
      abs(countMonth1-countMonth2) AS diff
    ORDER BY
      diff DESC,
      `tag`.name ASC
    LIMIT 100
    """
    Then the result should be, in any order:

  Scenario:  4. Popular topics in a country 
    When executing query:
    """
    MATCH
      (country:Country)<-[:IS_PART_OF]-(:City)<-[:IS_LOCATED_IN]-
      (person:Person)<-[:HAS_MODERATOR]-(forum:Forum)-[:CONTAINER_OF]->
      (post:Post)-[:HAS_TAG]->(:`Tag`)-[:HAS_TYPE]->(:TagClass {name: "MusicalArtist"})
    WHERE id(country) == "Burma"
    RETURN
      forum.id AS forumId,
      forum.title,
      forum.creationDate,
      person.id,
      count(DISTINCT post) AS postCount
    ORDER BY
      postCount DESC,
      forumId ASC
    LIMIT 20
    """
    Then the result should be, in order:
    | forumId | forum.title | forum.creationDate | person.id | postCount |

  Scenario: 5. Top posters in a country
    When executing query:
    """
    MATCH
      (country:Country)<-[:IS_PART_OF]-(:City)<-[:IS_LOCATED_IN]-
      (person:Person)<-[:HAS_MEMBER]-(forum:Forum)
    WHERE id(country) == "Belarus"
    WITH forum, count(person) AS numberOfMembers, forum.id AS forumId
    ORDER BY numberOfMembers DESC, forumId ASC
    LIMIT 100
    WITH collect(forum) AS popularForums
    UNWIND popularForums AS forum
    MATCH
      (forum)-[:HAS_MEMBER]->(person:Person)
    OPTIONAL MATCH
      (person)<-[:HAS_CREATOR]-(post:Post)<-[:CONTAINER_OF]-(popularForum:Forum)
    WHERE popularForum IN popularForums
    RETURN
      person.id AS personId,
      person.firstName,
      person.lastName,
      person.creationDate,
      count(DISTINCT post) AS postCount
    ORDER BY
      postCount DESC,
      personId ASC
    LIMIT 100
    """
    Then the result should be, in order:
    | personId | person.firstName | person.lastName | person.creationDate | postCount |

  Scenario: 6. Most active posters of a given topic
    When executing query:
    """
    MATCH (`tag`:`Tag`)<-[:HAS_TAG]-(message:Message)-[:HAS_CREATOR]->(person:Person)
    WHERE id(`tag`) == "Abbas_I_of_Persia"
    OPTIONAL MATCH (:Person)-[like:LIKES]->(message)
    OPTIONAL MATCH (message)<-[:REPLY_OF]-(comment:`Comment`)
    WITH person, count(DISTINCT like) AS likeCount, count(DISTINCT comment) AS replyCount, count(DISTINCT message) AS messageCount
    RETURN
      person.id AS personId,
      replyCount,
      likeCount,
      messageCount,
      1*messageCount + 2*replyCount + 10*likeCount AS score
    ORDER BY
      score DESC,
      personId ASC
    LIMIT 100
    """
    Then the result should be, in order:
    | personId | replyCount | likeCount | messageCount | score |

  Scenario: 7. Most authoritative users on a given topic
    When executing query:
    """
    MATCH (`tag`:`Tag`)
    WHERE id(`tag`) == "Arnold_Schwarzenegger"
    MATCH (`tag`)<-[:HAS_TAG]-(message1:Message)-[:HAS_CREATOR]->(person1:Person)
    MATCH (`tag`)<-[:HAS_TAG]-(message2:Message)-[:HAS_CREATOR]->(person1)
    OPTIONAL MATCH (message2)<-[:LIKES]-(person2:Person)
    OPTIONAL MATCH (person2)<-[:HAS_CREATOR]-(message3:Message)<-[like:LIKES]-(p3:Person)
    RETURN
      person1.id AS person1Id,
      count(DISTINCT like) AS authorityScore
    ORDER BY
      authorityScore DESC,
      person1Id ASC
    LIMIT 100
    """
    Then the result should be, in order:
    | person1Id | authorityScore |

  Scenario: 8. Related topics
    # NOTICE: I had rewrite the original query
    # TODO: WHERE NOT (comment)-[:HAS_TAG]->(tag)
    When executing query:
    """
    MATCH
      (`tag`:`Tag`)<-[:HAS_TAG]-(message:Message),
      (message)<-[:REPLY_OF]-(comment:`Comment`)-[:HAS_TAG]->(relatedTag:`Tag`)
    WHERE id(`tag`) == "Genghis_Khan" AND NOT `tag` == relatedTag
    RETURN
      relatedTag.name AS relatedTagName,
      count(DISTINCT comment) AS count
    ORDER BY
      count DESC,
      relatedTagName ASC
    LIMIT 100
    """
    Then the result should be, in order:
    | relatedTagName | count |

  Scenario: 9. Forum with related tags
    When executing query:
    """
    MATCH
      (forum:Forum)-[:HAS_MEMBER]->(person:Person)
    WITH forum, count(person) AS members
    WHERE members > 200
    MATCH
      (forum)-[:CONTAINER_OF]->(post1:Post)-[:HAS_TAG]->
      (:`Tag`)-[:HAS_TYPE]->(:TagClass {name: "BaseballPlayer"})
    WITH forum, count(DISTINCT post1) AS count1
    MATCH
      (forum)-[:CONTAINER_OF]->(post2:Post)-[:HAS_TAG]->
      (:`Tag`)-[:HAS_TYPE]->(:TagClass {name: "ChristianBishop"})
    WITH forum, count1, count(DISTINCT post2) AS count2
    RETURN
      forum.id AS forumId,
      count1,
      count2,
      abs(count2-count1) AS diff
    ORDER BY
      diff DESC,
      forumId ASC
    LIMIT 100
    """
    Then a SyntaxError should be raised at runtime:

  @skip
  Scenario: 10. Central person for a tag
    # TODO: 100 * length([(`tag`)<-[interest:HAS_INTEREST]-(friend) | interest])
    When executing query:
    """
    MATCH (`tag`:`Tag`)
    WHERE id(`tag`) == "John_Rhys-Davies"
    OPTIONAL MATCH (`tag`)<-[interest:HAS_INTEREST]-(person:Person)
    WITH `tag`, collect(person) AS interestedPersons
    OPTIONAL MATCH (`tag`)<-[:HAS_TAG]-(message:Message)-[:HAS_CREATOR]->(person:Person)
             WHERE message.creationDate > "20120122000000000"
    WITH `tag`, interestedPersons + collect(person) AS persons
    UNWIND persons AS person
    WITH DISTINCT `tag`, person
    WITH
      `tag`,
      person,
      100 * length([(`tag`)<-[interest:HAS_INTEREST]-(person) | interest])
        + length([(`tag`)<-[:HAS_TAG]-(message:Message)-[:HAS_CREATOR]->(person) WHERE message.creationDate > $date | message])
      AS score
    OPTIONAL MATCH (person)-[:KNOWS]-(friend)
    WITH
      person,
      score,
      100 * length([(`tag`)<-[interest:HAS_INTEREST]-(friend) | interest])
        + length([(`tag`)<-[:HAS_TAG]-(message:Message)-[:HAS_CREATOR]->(friend) WHERE message.creationDate > $date | message])
      AS friendScore
    RETURN
      person.id,
      score,
      sum(friendScore) AS friendsScore
    ORDER BY
      score + friendsScore DESC,
      person.id ASC
    LIMIT 100
    """
    Then a SyntaxError should be raised at runtime:

  @skip
  Scenario: 11. Unrelated replies
    # TODO: WHERE NOT (message)-[:HAS_TAG]->(:Tag)<-[:HAS_TAG]-(reply)
    When executing query:
    """
    WITH ['also', 'Pope', 'that', 'James', 'Henry', 'one', 'Green'] AS blacklist
    MATCH
      (country:Country {name: "Germany"})<-[:IS_PART_OF]-(:City)<-[:IS_LOCATED_IN]-
      (person:Person)<-[:HAS_CREATOR]-(reply:Comment)-[:REPLY_OF]->(message:Message),
      (reply)-[:HAS_TAG]->(tag:Tag)
    WHERE NOT (message)-[:HAS_TAG]->(:Tag)<-[:HAS_TAG]-(reply)
      AND size([word IN blacklist WHERE reply.content CONTAINS word | word]) = 0
    OPTIONAL MATCH
      (:Person)-[like:LIKES]->(reply)
    RETURN
      person.id,
      tag.name,
      count(DISTINCT like) AS countLikes,
      count(DISTINCT reply) AS countReplies
    ORDER BY
      countLikes DESC,
      person.id ASC,
      tag.name ASC
    LIMIT 100
    """
    Then a SyntaxError should be raised at runtime:

  Scenario: 12. Trending posts
    # NOTICE: Need a tag index on Message:creationDate
    When executing query:
    """
    MATCH
      (message:Message)-[:HAS_CREATOR]->(creator:Person),
      (message)<-[like:LIKES]-(:Person)
    WHERE message.creationDate > 20110721220000000
    WITH message, creator, count(like) AS likeCount
    WHERE likeCount > 400
    RETURN
      message.id AS messageId,
      message.creationDate,
      creator.firstName,
      creator.lastName,
      likeCount
    ORDER BY
      likeCount DESC,
      messageId ASC
    LIMIT 100
    """
    Then the result should be, in order:
    | messageId | message.creationDate | creator.firstName | creator.lastName | likeCount |

  Scenario: 13. Popular tags per month in a country
    When executing query:
    """
    MATCH (country:Country)<-[:IS_LOCATED_IN]-(message:Message)
    WHERE id(country) == "Burma"
    OPTIONAL MATCH (message)-[:HAS_TAG]->(`tag`:`Tag`)
    WITH
      toInteger(message.creationDate)/10000000000000   AS year,
      toInteger(message.creationDate)/100000000000%100 AS month,
      message,
      `tag`
    WITH year, month, count(message) AS popularity, `tag`, `tag`.name AS tagName
    ORDER BY popularity DESC, tagName ASC
    WITH
      year,
      month,
      collect([`tag`.name, popularity]) AS popularTags
    WITH
      year,
      month,
      [popularTag IN popularTags WHERE popularTag[0] IS NOT NULL] AS popularTags
    RETURN
      year,
      month,
      popularTags[0..5] AS topPopularTags
    ORDER BY
      year DESC,
      month ASC
    LIMIT 100
    """
    Then the result should be, in order:
    | year | month | topPopularTags |

  Scenario: 14. Top thread initiators
    # TODO: [:REPLY_OF*0..]
    When executing query:
    """
    MATCH (person:Person)<-[:HAS_CREATOR]-(post:Post)<-[:REPLY_OF*0..100]-(reply:Message)
    WHERE  reply.creationDate >= "20120531220000000"
      AND reply.creationDate <= "20120630220000000"
    WITH person, post, reply
    WHERE post.creationDate >= "20120531220000000"
      AND  post.creationDate <= "20120630220000000"
    RETURN
      person.id AS personId,
      person.firstName,
      person.lastName,
      count(DISTINCT post) AS threadCount,
      count(DISTINCT reply) AS messageCount
    ORDER BY
      messageCount DESC,
      personId ASC
    LIMIT 100
    """
    Then the result should be, in any order:
    | personId | person.firstName | person.lastName | threadCount | messageCount |

  Scenario: 15. Social normals
    When executing query:
    """
    MATCH
      (country:Country)
    WHERE id(country) == "Burma"
    MATCH
      (country)<-[:IS_PART_OF]-(:City)<-[:IS_LOCATED_IN]-(person1:Person)
    OPTIONAL MATCH
      (country)<-[:IS_PART_OF]-(:City)<-[:IS_LOCATED_IN]-(friend1:Person),
      (person1)-[:KNOWS]-(friend1)
    WITH country, person1, count(friend1) AS friend1Count
    WITH country, avg(friend1Count) AS socialNormalFloat
    WITH country, floor(socialNormalFloat) AS socialNormal
    MATCH
      (country)<-[:IS_PART_OF]-(:City)<-[:IS_LOCATED_IN]-(person2:Person)
    OPTIONAL MATCH
      (country)<-[:IS_PART_OF]-(:City)<-[:IS_LOCATED_IN]-(friend2:Person)-[:KNOWS]-(person2)
    WITH country, person2, count(friend2) AS friend2Count, socialNormal
    WHERE friend2Count == socialNormal
    RETURN
      person2.id AS person2Id,
      friend2Count AS count
    ORDER BY
      person2Id ASC
    LIMIT 100
    """
    Then the result should be, in order:
    | person2Id | count |

  Scenario: 16. Experts in social circle
    When executing query:
    """
    MATCH
      (n:Person)-[:KNOWS*3..5]-(person:Person)
    WHERE id(n) == "19791209310731"
    WITH DISTINCT person
    MATCH
      (person)-[:IS_LOCATED_IN]->(:City)-[:IS_PART_OF]->(:Country {name: "Pakistan"}),
      (person)<-[:HAS_CREATOR]-(message:Message)-[:HAS_TAG]->(:`Tag`)-[:HAS_TYPE]->
      (:TagClass {name: "MusicalArtist"})
    MATCH
      (message)-[:HAS_TAG]->(`tag`:`Tag`)
    RETURN
      person.id AS personId,
      `tag`.name AS tagName,
      count(DISTINCT message) AS messageCount
    ORDER BY
      messageCount DESC,
      tagName ASC,
      personId ASC
    LIMIT 100
    """
    Then the result should be, in order:
    | personId | tagName | messageCount |

  Scenario: 17. Friend triangles
    When executing query:
    """
    MATCH (country:Country)
    WHERE id(country) == "Spain"
    MATCH (a:Person)-[:IS_LOCATED_IN]->(:City)-[:IS_PART_OF]->(country)
    MATCH (b:Person)-[:IS_LOCATED_IN]->(:City)-[:IS_PART_OF]->(country)
    MATCH (c:Person)-[:IS_LOCATED_IN]->(:City)-[:IS_PART_OF]->(country)
    MATCH (a)-[:KNOWS]-(b), (b)-[:KNOWS]-(c), (c)-[:KNOWS]-(a)
    WHERE a.id < b.id
      AND b.id < c.id
    RETURN count(*) AS count
    """
    Then the result should be, in any order:
    | count |

  Scenario: 18. How many persons have a given number of messages
    # NOTICE: Need a index on person
    # TODO: [:REPLY_OF*0..]
    When executing query:
    """
    MATCH (person:Person)
    OPTIONAL MATCH (person)<-[:HAS_CREATOR]-(message:Message)-[:REPLY_OF*0..100]->(post:Post)
    WHERE message.content IS NOT NULL
      AND message.length < 20
      AND message.creationDate > "20110722000000000"
      AND post.language IN ["ar"]
    WITH
      person,
      count(message) AS messageCount
    RETURN
      messageCount,
      count(person) AS personCount
    ORDER BY
      personCount DESC,
      messageCount DESC
    """
    Then the result should be, in order:
    | messageCount | personCount |

  Scenario: 19. Stranger’s interaction
    # NOTICE: A big rewritten, have to test the correctness
    When executing query:
    """
    MATCH
      (tagClass:TagClass)<-[:HAS_TYPE]-(:`Tag`)<-[:HAS_TAG]-
      (forum1:Forum)-[:HAS_MEMBER]->(stranger:Person)
    WHERE id(tagClass) == "MusicalArtist"
    WITH DISTINCT stranger
    MATCH
      (tagClass:TagClass)<-[:HAS_TYPE]-(:`Tag`)<-[:HAS_TAG]-
      (forum2:Forum)-[:HAS_MEMBER]->(stranger)
    WHERE id(tagClass) == "OfficeHolder"
    WITH DISTINCT stranger
    MATCH
      (person:Person)<-[:HAS_CREATOR]-(comment:`Comment`)-[:REPLY_OF*100]->(message:Message)-[:HAS_CREATOR]->(stranger)
    OPTIONAL MATCH (person)-[knows:KNOWS]-(stranger)
    OPTIONAL MATCH (message)-[replyOf:REPLY_OF*100]->(:Message)-[hasCreator:HAS_CREATOR]->(stranger)
    WHERE person.birthday > "19890101"
      AND person <> stranger
      AND knows IS NULL
      AND (replyOf IS NULL OR hasCreator IS NULL)
    RETURN
      person.id AS personId,
      count(DISTINCT stranger) AS strangersCount,
      count(comment) AS interactionCount
    ORDER BY
      interactionCount DESC,
      personId ASC
    LIMIT 100
    """
    Then the result should be, in order:
    | personId | strangersCount | interactionCount |

  Scenario: 20. High-level topics
    When executing query:
    """
    MATCH
      (tagClass:TagClass)<-[:IS_SUBCLASS_OF*0..100]-
      (:TagClass)<-[:HAS_TYPE]-(tag:Tag)<-[:HAS_TAG]-(message:Message)
    WHERE id(tagClass) IN ['Writer', 'Single', 'Country']
    RETURN
      tagClass.name AS tagClassName,
      count(DISTINCT message) AS messageCount
    ORDER BY
      messageCount DESC,
      tagClassName ASC
    LIMIT 100
    """
    Then the result should be, in order:
    | tagClassName | messageCount |
