# Copyright (c) 2021 vesoft inc. All rights reserved.
#
# This source code is licensed under Apache 2.0 License.
Feature: LDBC Interactive Workload - Short Reads

  Background:
    Given a graph with space named "ldbc_v0_3_3"

  Scenario: 1. Friends with certain name
    When executing query:
      """
      MATCH (n:Person)-[:IS_LOCATED_IN]->(p:City)
      WHERE id(n)=="person-933"
      RETURN
        n.Person.firstName AS firstName,
        n.Person.lastName AS lastName,
        n.Person.birthday AS birthday,
        n.Person.locationIP AS locationIP,
        n.Person.browserUsed AS browserUsed,
        id(p) AS cityId,
        n.Person.gender AS gender,
        n.Person.creationDate AS creationDate
      """
    Then the result should be, in any order:
      | firstName | lastName | birthday | locationIP | browserUsed | cityId | gender | creationDate |

  Scenario: 2. Recent messages of a person
    # TODO: [:REPLY_OF*0..] is not supported, instead by [:REPLY_OF*0..100] for now
    When executing query:
      """
      MATCH (n:Person)<-[:HAS_CREATOR]-(m)-[:REPLY_OF*0..100]->(p:Post)
      WHERE id(n)=="person-4139"
      MATCH (p)-[:HAS_CREATOR]->(c)
      RETURN id(m) as messageId, 
        CASE exists(m.Post.creationDate) 
          WHEN true THEN 
            CASE m.Post.content != ""
              WHEN true THEN m.Post.content 
              ELSE m.Post.imageFile 
            END 
          ELSE m.`Comment`.content 
        END AS messageContent, 
        CASE exists(m.Post.creationDate) 
          WHEN true THEN m.Post.creationDate 
          ELSE m.`Comment`.creationDate 
        END AS messageCreationDate, 
        id(p) AS originalPostId, 
        id(c) AS originalPostAuthorId, 
        c.Person.firstName as originalPostAuthorFirstName, 
        c.Person.lastName AS originalPostAuthorLastName 
      ORDER BY messageCreationDate DESC 
      LIMIT 10
      """
    Then the result should be, in any order:
      | messageId | messageContent | messageCreationDate | originalPostId | originalPostAuthorId | originalPostAuthorFirstName | originalPostAuthorLastName |

  Scenario: 3. Friends of a person
    When executing query:
      """
      MATCH (n:Person)-[r:KNOWS]-(friend)
      WHERE id(n) == "person-4139"
      RETURN
        id(friend) AS personId,
        friend.Person.firstName AS firstName,
        friend.Person.lastName AS lastName,
        r.creationDate AS friendshipCreationDate
      ORDER BY friendshipCreationDate DESC, personId ASC
      """
    Then the result should be, in any order:
      | personId | firstName | lastName | friendshipCreationDate |

  Scenario: 4. Content of a message
    When executing query:
      """
      MATCH (m)
      WHERE id(m) == "comment-1236950581249"
      RETURN
        CASE exists(m.Post.creationDate) 
          WHEN true THEN m.Post.creationDate 
          ELSE m.`Comment`.creationDate 
        END as messageCreationDate, 
        CASE exists(m.Post.creationDate) 
          WHEN true THEN 
            CASE m.Post.content != ""
              WHEN true THEN m.Post.content 
              ELSE m.Post.imageFile 
            END 
          ELSE m.`Comment`.content 
        END AS messageContent
      """
    Then the result should be, in any order:
      | messageCreationDate | messageContent |

  Scenario: 5. Given a Message, retrieve its author
    When executing query:
      """
      MATCH (m)-[:HAS_CREATOR]->(p:Person)
      WHERE id(m) == "comment-1236950581249"
      RETURN
        id(p) AS personId,
        p.Person.firstName AS firstName,
        p.Person.lastName AS lastName
      """
    Then the result should be, in any order:
      | personId | firstName | lastName |

  Scenario: 6. Forum of a message
    # TODO: [:REPLY_OF*0..] is not supported, instead by [:REPLY_OF*0..100] for now
    When executing query:
      """
      MATCH (m)-[:REPLY_OF*0..100]->(p:Post)<-[:CONTAINER_OF]-(f:Forum)-[:HAS_MODERATOR]->(mod:Person)
      WHERE id(m) == "comment-1236950581249"
      RETURN
        id(f) AS forumId,
        f.Forum.title AS forumTitle,
        id(mod) AS moderatorId,
        mod.Person.firstName AS moderatorFirstName,
        mod.Person.lastName AS moderatorLastName
      """
    Then the result should be, in any order:
      | forumId | forumTitle | moderatorId | moderatorFirstName | moderatorLastName |

  Scenario: 7. Replies of a message
    # Notice: Comment is a keyword, instead by `Comment`
    When executing query:
      """
      MATCH (m)<-[:REPLY_OF]-(c:`Comment`)-[:HAS_CREATOR]->(p:Person)
      WHERE id(m) == "post-2061584302084"
      OPTIONAL MATCH (m)-[:HAS_CREATOR]->(a:Person)-[r:KNOWS]-(p)
      RETURN
        id(c) AS commentId,
        c.`Comment`.content AS commentContent,
        c.`Comment`.creationDate AS commentCreationDate,
        id(p) AS replyAuthorId,
        p.Person.firstName AS replyAuthorFirstName,
        p.Person.lastName AS replyAuthorLastName,
        CASE r
          WHEN null THEN false
          ELSE true
        END AS replyAuthorKnowsOriginalMessageAuthor
      ORDER BY commentCreationDate DESC, replyAuthorId
      """
    Then the result should be, in any order:
      | commentId | commentContent | commentCreationDate | replyAuthorId | replyAuthorFirstName | replyAuthorLastName | replyAuthorKnowsOriginalMessageAuthor |
