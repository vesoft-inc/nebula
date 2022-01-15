# Copyright (c) 2021 vesoft inc. All rights reserved.
#
# This source code is licensed under Apache 2.0 License.
Feature: LDBC Business Intelligence Workload - Read

  Background:
    Given a graph with space named "ldbc_v0_3_3"

  Scenario: 1. Posting summary
    When executing query:
      """
      MATCH (message:Message)
      WHERE message.Message.creationDate < "20110721220000000"
      WITH count(message) AS totalMessageCountInt
      WITH toFloat(totalMessageCountInt) AS totalMessageCount
      MATCH (message:Message)
      WHERE message.Message.creationDate < "20110721220000000"
        AND message.Message.content IS NOT NULL
      WITH
        totalMessageCount,
        message,
        toInteger(message.Message.creationDate)/10000000000000 AS year
      WITH
        totalMessageCount,
        year,
        "Comment" IN tags(message) AS isComment,
        CASE
          WHEN message.Message.length <  40 THEN 0
          WHEN message.Message.length <  80 THEN 1
          WHEN message.Message.length < 160 THEN 2
          ELSE                           3
        END AS lengthCategory,
        count(message) AS messageCount,
        floor(avg(message.Message.length)) AS averageMessageLength,
        sum(message.Message.length) AS sumMessageLength
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
      WHERE message.Message.creationDate >= "20091231230000000"
        AND message.Message.creationDate <= "20101107230000000"
        AND (id(country) == "Ethiopia" OR id(country) == "Belarus")
      WITH
        country.Country.name AS countryName,
        toInteger(message.Message.creationDate)/100000000000%100 AS month,
        person.Person.gender AS gender,
        floor((20130101 - person.Person.birthday) / 10000 / 5.0) AS ageGroup,
        `tag`.`Tag`.name AS tagName,
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

  Scenario: 4. Popular topics in a country
    When executing query:
      """
      MATCH
        (country:Country)<-[:IS_PART_OF]-(:City)<-[:IS_LOCATED_IN]-
        (person:Person)<-[:HAS_MODERATOR]-(forum:Forum)-[:CONTAINER_OF]->
        (post:Post)-[:HAS_TAG]->(:`Tag`)-[:HAS_TYPE]->(:TagClass {name: "MusicalArtist"})
      WHERE id(country) == "Burma"
      RETURN
        forum.Forum.id AS forumId,
        forum.Forum.title AS forumTitle,
        forum.Forum.creationDate AS forumCreationDate,
        person.Person.id AS personId,
        count(DISTINCT post) AS postCount
      ORDER BY
        postCount DESC,
        forumId ASC
      LIMIT 20
      """
    Then the result should be, in order:
      | forumId | forumTitle | forumCreationDate | personId | postCount |

  Scenario: 5. Top posters in a country
    When executing query:
      """
      MATCH
        (country:Country)<-[:IS_PART_OF]-(:City)<-[:IS_LOCATED_IN]-
        (person:Person)<-[:HAS_MEMBER]-(forum:Forum)
      WHERE id(country) == "Belarus"
      WITH forum, count(person) AS numberOfMembers, forum.Forum.id AS forumId
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
        person.Person.id AS personId,
        person.Person.firstName AS personFirstName,
        person.Person.lastName AS personLastName,
        person.Person.creationDate AS personCreationDate,
        count(DISTINCT post) AS postCount
      ORDER BY
        postCount DESC,
        personId ASC
      LIMIT 100
      """
    Then the result should be, in order:
      | personId | personFirstName | personLastName | personCreationDate | postCount |

  Scenario: 6. Most active posters of a given topic
    When executing query:
      """
      MATCH (`tag`:`Tag`)<-[:HAS_TAG]-(message:Message)-[:HAS_CREATOR]->(person:Person)
      WHERE id(`tag`) == "Abbas_I_of_Persia"
      OPTIONAL MATCH (:Person)-[like:LIKES]->(message)
      OPTIONAL MATCH (message)<-[:REPLY_OF]-(comment:`Comment`)
      WITH person, count(DISTINCT like) AS likeCount, count(DISTINCT comment) AS replyCount, count(DISTINCT message) AS messageCount
      RETURN
        person.Person.id AS personId,
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
        person1.Person.id AS person1Id,
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
        relatedTag.`Tag`.name AS relatedTagName,
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
        forum.Forum.id AS forumId,
        count1,
        count2,
        abs(count2-count1) AS diff
      ORDER BY
        diff DESC,
        forumId ASC
      LIMIT 100
      """
    Then the result should be, in order:
      | forumId | count1 | count2 | diff |

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
    When executing query:
      """
      MATCH
        (message:Message)-[:HAS_CREATOR]->(creator:Person),
        (message)<-[like:LIKES]-(:Person)
      WHERE message.Message.creationDate > "20110721220000000"
      WITH message, creator, count(like) AS likeCount
      WHERE likeCount > 400
      RETURN
        message.Message.id AS messageId,
        message.Message.creationDate AS messageCreationDate,
        creator.Person.firstName AS creatorFirstName,
        creator.Person.lastName AS creatorLastName,
        likeCount
      ORDER BY
        likeCount DESC,
        messageId ASC
      LIMIT 100
      """
    Then the result should be, in order:
      | messageId | messageCreationDate | creatorFirstName | creatorLastName | likeCount |

  Scenario: 13. Popular tags per month in a country
    When executing query:
      """
      MATCH (country:Country)<-[:IS_LOCATED_IN]-(message:Message)
      WHERE id(country) == "Burma"
      OPTIONAL MATCH (message)-[:HAS_TAG]->(`tag`:`Tag`)
      WITH
        toInteger(message.Message.creationDate)/10000000000000   AS year,
        toInteger(message.Message.creationDate)/100000000000%100 AS month,
        message,
        `tag`
      WITH year, month, count(message) AS popularity, `tag`, `tag`.`Tag`.name AS tagName
      ORDER BY popularity DESC, tagName ASC
      WITH
        year,
        month,
        collect([`tag`.`Tag`.name, popularity]) AS popularTags
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
      WHERE  reply.Message.creationDate >= "20120531220000000"
        AND reply.Message.creationDate <= "20120630220000000"
      WITH person, post, reply
      WHERE post.Post.creationDate >= "20120531220000000"
        AND  post.Post.creationDate <= "20120630220000000"
      RETURN
        person.Person.id AS personId,
        person.Person.firstName AS personFirstName,
        person.Person.lastName AS personLastName,
        count(DISTINCT post) AS threadCount,
        count(DISTINCT reply) AS messageCount
      ORDER BY
        messageCount DESC,
        personId ASC
      LIMIT 100
      """
    Then the result should be, in any order:
      | personId | personFirstName | personLastName | threadCount | messageCount |

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
        person2.Person.id AS person2Id,
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
        person.Person.id AS personId,
        `tag`.`Tag`.name AS tagName,
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
      WHERE a.Person.id < b.Person.id
        AND b.Person.id < c.Person.id
      RETURN count(*) AS count
      """
    Then the result should be, in any order:
      | count |
      | 0     |

  Scenario: 18. How many persons have a given number of messages
    # TODO: [:REPLY_OF*0..]
    When executing query:
      """
      MATCH (person:Person)
      OPTIONAL MATCH (person)<-[:HAS_CREATOR]-(message:Message)-[:REPLY_OF*0..100]->(post:Post)
      WHERE message.Message.content IS NOT NULL
        AND message.Message.length < 20
        AND message.Message.creationDate > "20110722000000000"
        AND post.Post.language IN ["ar"]
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
      WHERE person.Person.birthday > "19890101"
        AND person <> stranger
        AND knows IS NULL
        AND (replyOf IS NULL OR hasCreator IS NULL)
      RETURN
        person.Person.id AS personId,
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
        (:TagClass)<-[:HAS_TYPE]-(`tag`:`Tag`)<-[:HAS_TAG]-(message:Message)
      WHERE id(tagClass) IN ['Writer', 'Single', 'Country']
      RETURN
        tagClass.TagClass.name AS tagClassName,
        count(DISTINCT message) AS messageCount
      ORDER BY
        messageCount DESC,
        tagClassName ASC
      LIMIT 100
      """
    Then the result should be, in order:
      | tagClassName | messageCount |
