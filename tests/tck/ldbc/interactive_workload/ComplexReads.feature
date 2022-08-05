# Copyright (c) 2021 vesoft inc. All rights reserved.
#
# This source code is licensed under Apache 2.0 License.
Feature: LDBC Interactive Workload - Complex Reads

  Background:
    Given a graph with space named "ldbc_v0_3_3"

  Scenario: 1. Friends with certain name
    # {"personId":"4398046511333", "firstName":"Jose"}
    When executing query:
      """
      MATCH p=(person:Person)-[:KNOWS*1..3]-(friend:Person {firstName: "Jose"})
      WHERE id(person) == "4398046511333"
      AND person <> friend
      WITH friend, length(p) AS distance
      LIMIT 20
      MATCH (friend)-[:IS_LOCATED_IN]->(friendCity:Place)
      OPTIONAL MATCH (friend)-[studyAt:STUDY_AT]->(uni:Organisation)-[:IS_LOCATED_IN]->(uniCity:Place)
      WITH
        friend,
        collect(
          CASE uni.Organisation.name
            WHEN null THEN null
            ELSE [uni.Organisation.name, studyAt.classYear, uniCity.Place.name]
          END
        ) AS unis,
        friendCity,
        distance
      OPTIONAL MATCH (friend)-[workAt:WORK_AT]->(company:Organisation)-[:IS_LOCATED_IN]->(companyCountry:Place)
      WITH
        friend,
        collect(
          CASE company.Organisation.name
            WHEN null THEN null
            ELSE [company.Organisation.name, workAt.workFrom, companyCountry.Place.name]
          END
        ) AS companies,
        unis,
        friendCity,
        distance
      RETURN
        friend.Person.id AS friendId,
        friend.Person.lastName AS friendLastName,
        distance AS distanceFromPerson,
        friend.Person.birthday AS friendBirthday,
        friend.Person.creationDate AS friendCreationDate,
        friend.Person.gender AS friendGender,
        friend.Person.browserUsed AS friendBrowserUsed,
        friend.Person.locationIP AS friendLocationIp,
        friend.Person.email AS friendEmails,
        friend.Person.speaks AS friendLanguages,
        friendCity.Place.name AS friendCityName,
        unis AS friendUniversities,
        companies AS friendCompanies
      ORDER BY distanceFromPerson ASC, friendLastName ASC
      LIMIT 20
      """
    Then the result should be, in any order:
      | friendId | friendLastName | distanceFromPerson | friendBirthday | friendCreationDate | friendGender | friendBrowserUsed | friendLocationIp | friendEmails | friendLanguages | friendCityName | friendUniversities | friendCompanies |

  Scenario: 2. Recent messages by your friends
    # {"personId":"10995116278009", "maxDate":"1287230400000"}
    When executing query:
      """
      MATCH (n:Person)-[:KNOWS]-(friend:Person)<-[:HAS_CREATOR]-(message:Message)
      WHERE id(n) == "10995116278009" and message.Message.creationDate <= 1287230400000
      RETURN
        friend.Person.id AS personId,
        friend.Person.firstName AS personFirstName,
        friend.Person.lastName AS personLastName,
        toInteger(message.Message.id) AS messageId,
        CASE exists(message.Message.content)
          WHEN true THEN message.Message.content
          ELSE message.Message.imageFile
        END AS messageContent,
        message.Message.creationDate AS messageCreationDate
      ORDER BY messageCreationDate DESC, messageId ASC
      LIMIT 20
      """
    Then the result should be, in any order:
      | personId | personFirstName | personLastName | messageId | messageContent | messageCreationDate |

  Scenario: 3. Friends and friends of friends that have been to given countries
    # TODO enhance order by
    # {"personId":"6597069766734","countryXName":"Angola","countryYName":"Colombia","startDate":"1275393600000","endDate":"1277812800000"}
    When executing query:
      """
      MATCH (person:Person)-[:KNOWS*1..2]-(friend:Person)<-[:HAS_CREATOR]-(messageX:Message),
      (messageX)-[:IS_LOCATED_IN]->(countryX:Place)
      WHERE
        id(person) == "6597069766734"
        AND not(person==friend)
        AND not((friend)-[:IS_LOCATED_IN]->()-[:IS_PART_OF]->(countryX))
        AND id(countryX)=="Angola" AND messageX.Message.creationDate>="1275393600000"
        AND messageX.Message.creationDate<"1277812800000"
      WITH friend, count(DISTINCT messageX) AS xCount
      MATCH (friend)<-[:HAS_CREATOR]-(messageY:Message)-[:IS_LOCATED_IN]->(countryY:Place)
      WHERE
        id(countryY)=="Colombia"
        AND not((friend)-[:IS_LOCATED_IN]->()-[:IS_PART_OF]->(countryY))
        AND messageY.Message.creationDate>="$startDate"
        AND messageY.Message.creationDate<"$endDate"
      WITH
        friend.Person.id AS personId,
        friend.Person.firstName AS personFirstName,
        friend.Person.lastName AS personLastName,
        xCount,
        count(DISTINCT messageY) AS yCount
      RETURN
        personId,
        personFirstName,
        personLastName,
        xCount,
        yCount,
        xCount + yCount AS count
      ORDER BY count DESC, personId ASC
      LIMIT 20
      """
    Then the result should be, in any order:
      | personId | personFirstName | personLastName | xCount | yCount | count |

  Scenario: 4. New topics
    # {"personId":"4398046511333","startDate":"1275350400000","endDate":"1277856000000"}
    When executing query:
      """
      MATCH (person:Person)-[:KNOWS]-(:Person)<-[:HAS_CREATOR]-(post:Post)-[:HAS_TAG]->(`tag`:`Tag`)
      WHERE id(person) == "4398046511333" AND post.Post.creationDate >= "1275350400000"
         AND post.Post.creationDate < "1277856000000"
      WITH person, count(post) AS postsOnTag, `tag`
      OPTIONAL MATCH (person)-[:KNOWS]-()<-[:HAS_CREATOR]-(oldPost:Post)-[:HAS_TAG]->(`tag`)
      WHERE oldPost.Post.creationDate < $startDate
      WITH person, postsOnTag, `tag`, count(oldPost) AS cp
      WHERE cp == 0
      RETURN
        `tag`.`Tag`.name AS tagName,
        sum(postsOnTag) AS postCount
      ORDER BY postCount DESC, tagName ASC
      """
    Then the result should be, in any order:
      | tagName | postCount |

  Scenario: 5. New groups
    # {"personId":"6597069766734","minDate":"1288612800000"}
    When executing query:
      """
      MATCH (person:Person)-[:KNOWS*1..2]-(friend:Person)<-[membership:HAS_MEMBER]-(forum:Forum)
      WHERE id(person) == "6597069766734" AND membership.joinDate>"1288612800000"
          AND not(person==friend)
      WITH DISTINCT friend, forum
      OPTIONAL MATCH (friend)<-[:HAS_CREATOR]-(post:Post)<-[:CONTAINER_OF]-(forum)
      WITH forum, count(post) AS postCount
      RETURN
        toInteger(forum.Forum.id) AS forumId,
        forum.Forum.title AS forumTitle,
        postCount
      ORDER BY postCount DESC, forumId ASC
      LIMIT 20
      """
    Then the result should be, in any order:
      | forumId | forumTitle | postCount |

  Scenario: 6. Tag co-occurrence
    # {"personId":"4398046511333","tagName":"Carl_Gustaf_Emil_Mannerheim"}
    When executing query:
      """
      MATCH
        (person:Person)-[:KNOWS*1..2]-(friend:Person),
        (friend)<-[:HAS_CREATOR]-(friendPost:Post)-[:HAS_TAG]->(knownTag:`Tag` {name:"$tagName"})
      WHERE id(person) == "4398046511333" AND not(person==friend)
      MATCH (friendPost)-[:HAS_TAG]->(commonTag:`Tag`)
      WHERE not(commonTag==knownTag)
      WITH DISTINCT commonTag, knownTag, friend
      MATCH (commonTag)<-[:HAS_TAG]-(commonPost:Post)-[:HAS_TAG]->(knownTag)
      WHERE (commonPost)-[:HAS_CREATOR]->(friend)
      RETURN
        commonTag.`Tag`.name AS tagName,
        count(commonPost) AS postCount
      ORDER BY postCount DESC, tagName ASC
      LIMIT 10
      """
    Then the result should be, in any order:
      | tagName | postCount |

  Scenario: 7. Recent likers
    # TODO enhance order by
    # personId: 4398046511268
    When executing query:
      """
      MATCH (person:Person)<-[:HAS_CREATOR]-(message:Message)<-[like:LIKES]-(liker:Person)
      WHERE id(person) == "4398046511268"
      WITH liker, message, like.creationDate AS likeTime, person, toInteger(message.Message.id) AS messageId
      ORDER BY likeTime DESC, messageId ASC
      WITH
        liker,
        head(collect({msg: message, likeTime: likeTime})) AS latestLike,
        person
      RETURN
        toInteger(liker.Person.id) AS personId,
        liker.Person.firstName AS personFirstName,
        liker.Person.lastName AS personLastName,
        latestLike.likeTime AS likeCreationDate,
        latestLike.msg.id AS messageId,
        CASE exists(latestLike.msg.content)
          WHEN true THEN latestLike.msg.content
          ELSE latestLike.msg.imageFile
        END AS messageContent,
        latestLike.msg.creationDate AS messageCreationDate,
        not((liker)-[:KNOWS]-(person)) AS isNew
      ORDER BY likeCreationDate DESC, personId ASC
      LIMIT 20
      """
    Then the result should be, in any order:
      | personId | personFirstName | personLastName | likeCreationDate | messageId | messageContent | messageCreationDate | isNew |

  Scenario: 8. Recent replies
    When executing query:
      """
      MATCH
        (start:Person)<-[:HAS_CREATOR]-(:Message)<-[:REPLY_OF]-(comment:`Comment`)-[:HAS_CREATOR]->(person:Person)
      WHERE id(start) == ""
      RETURN
        person.Person.id AS personId,
        person.Person.firstName AS personFirstName,
        person.Person.lastName AS personLastName,
        comment.`Comment`.creationDate AS commentCreationDate,
        toInteger(comment.`Comment`.id) AS commentId,
        comment.`Comment`.content AS commentContent
      ORDER BY commentCreationDate DESC, commentId ASC
      LIMIT 20
      """
    Then the result should be, in any order:
      | personId | personFirstName | personLastName | commentCreationDate | commentId | commentContent |

  Scenario: 9. Recent messages by friends or friends of friends
    When executing query:
      """
      MATCH (n:Person)-[:KNOWS*1..2]-(friend:Person)<-[:HAS_CREATOR]-(message:Message)
      WHERE id(n) == "" AND message.Message.creationDate < "$maxDate"
      RETURN DISTINCT
        friend.Person.id AS personId,
        friend.Person.firstName AS personFirstName,
        friend.Person.lastName AS personLastName,
        toInteger(message.Message.id) AS messageId,
        CASE exists(message.Message.content)
          WHEN true THEN message.Message.content
          ELSE message.Message.imageFile
        END AS messageContent,
        message.Message.creationDate AS messageCreationDate
      ORDER BY messageCreationDate DESC, messageId ASC
      LIMIT 20
      """
    Then the result should be, in any order:
      | personId | personFirstName | personLastName | messageId | messageContent | messageCreationDate |

  Scenario: 10. Friend recommendation
    # TODO support local defined variable in expression
    # personId: 4398046511333, moth:5
    When executing query:
      """
      MATCH (person:Person)-[:KNOWS*2..2]-(friend)-[:IS_LOCATED_IN]->(city:City)
      WHERE id(person)=="4398046511333" AND
            NOT friend==person
      OPTIONAL MATCH p = (friend)-[:KNOWS]-(person)
      WITH person, city, friend, datetime({epochMillis: friend.Person.birthday}) as birthday, p
      WHERE  p IS NULL AND
             ((birthday.month==5 AND birthday.day>=21) OR
             (birthday.month==(5%12)+1 AND birthday.day<22))
      WITH DISTINCT friend, city, person
      OPTIONAL MATCH (friend)<-[:HAS_CREATOR]-(post:Post)
      OPTIONAL MATCH p = (post)-[:HAS_TAG]->()<-[:HAS_INTEREST]-(person)
      WITH friend, city, post, person, p
      WITH friend, city, collect(post) AS posts, person, collect(p) AS paths
      WITH friend,
           city,
           size(posts) AS postCount,
           size([p IN paths WHERE p IS NOT NULL ]) AS commonPostCount
      RETURN friend.Person.id AS personId,
             friend.Person.firstName AS personFirstName,
             friend.Person.lastName AS personLastName,
             commonPostCount - (postCount - commonPostCount) AS commonInterestScore,
             friend.Person.gender AS personGender,
             city.City.name AS personCityName
      ORDER BY commonInterestScore DESC, personId ASC
      LIMIT 10
      """
    Then the result should be, in any order:
      | personId | personFirstName | personLastName | commonInterestScore | personGender | personCityName |

  Scenario: 11. Job referral
    When executing query:
      """
      MATCH (person:Person)-[:KNOWS*1..2]-(friend:Person)
      WHERE id(person) == "" AND not(person==friend)
      WITH DISTINCT friend
      MATCH (friend)-[workAt:WORK_AT]->(company:Organisation)-[:IS_LOCATED_IN]->(:Place {name:"$countryName"})
      WHERE workAt.workFrom < $workFromYear
      RETURN
        toInteger(friend.Person.id) AS personId,
        friend.Person.firstName AS personFirstName,
        friend.Person.lastName AS personLastName,
        company.Organisation.name AS organizationName,
        workAt.workFrom AS organizationWorkFromYear
      ORDER BY organizationWorkFromYear ASC, personId ASC, organizationName DESC
      LIMIT 10
      """
    Then the result should be, in any order:
      | personId | personFirstName | personLastName | organizationName | organizationWorkFromYear |

  Scenario: 12. Expert search
    # TODO: [:IS_SUBCLASS_OF*0..]
    When executing query:
      """
      MATCH (n:Person)-[:KNOWS]-(friend:Person)<-[:HAS_CREATOR]-(`comment`:`Comment`)-[:REPLY_OF]->(:Post)-[:HAS_TAG]->(`tag`:`Tag`),
        (`tag`)-[:HAS_TYPE]->(tagClass:TagClass)-[:IS_SUBCLASS_OF*0..100]->(baseTagClass:TagClass)
      WHERE id(n)=="" AND (tagClass.TagClass.name == "$tagClassName" OR baseTagClass.TagClass.name == "$tagClassName")
      RETURN
        toInteger(friend.Person.id) AS personId,
        friend.Person.firstName AS personFirstName,
        friend.Person.lastName AS personLastName,
        collect(DISTINCT `tag`.`Tag`.name) AS tagNames,
        count(DISTINCT `comment`) AS replyCount
      ORDER BY replyCount DESC, personId ASC
      LIMIT 20
      """
    Then the result should be, in any order:
      | personId | personFirstName | personLastName | tagNames | replyCount |

  @skip
  Scenario: 13. Single shortest path
    # TODO: shortestPath
    When executing query:
      """
      MATCH (person1:Person {id:$person1Id}), (person2:Person {id:$person2Id})
      OPTIONAL MATCH path = shortestPath((person1)-[:KNOWS*]-(person2))
      RETURN
      CASE path IS NULL
        WHEN true THEN -1
        ELSE length(path)
      END AS shortestPathLength;
      """
    Then a SyntaxError should be raised at runtime:

  Scenario: 14. Trusted connection paths
    # TODO: allShortestPaths
    # Modification: ldbc_snb_interactive_impls 0.3.4 modify `length` function to `size` function,
    # for the `length` don't accept LIST arguments again
    When executing query:
      """
      MATCH `path` = allShortestPaths((person1:Person {id:"123"})-[:KNOWS*..15]-(person2:Person {id:"321"}))
      WITH nodes(`path`) AS pathNodes
      RETURN
        [n IN pathNodes | n.id] AS personIdsInPath,
        reduce(weight=0.0, idx IN range(1,size(pathNodes)-1) | [prev IN [pathNodes[idx-1]] | [curr IN [pathNodes[idx]] | weight + size((curr)<-[:HAS_CREATOR]-(:`Comment`)-[:REPLY_OF]->(:Post)-[:HAS_CREATOR]->(prev))*1.0 + size((prev)<-[:HAS_CREATOR]-(:`Comment`)-[:REPLY_OF]->(:Post)-[:HAS_CREATOR]->(curr))*1.0 + size((prev)-[:HAS_CREATOR]-(:`Comment`)-[:REPLY_OF]-(:`Comment`)-[:HAS_CREATOR]-(curr))*0.5] ][0][0]) AS pathWight
      ORDER BY pathWight DESC
      """
    Then the result should be, in any order:
      | personIdsInPath | pathWight |
