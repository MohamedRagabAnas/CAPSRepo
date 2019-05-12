package org.opencypher.spark.ragabexamples

class Queries {


  

    /// Q1  BI ///


    val  Q1 =
      """
        |
        |MATCH (message:Message)
        |WHERE message.creationDate < '2011-12-16T21:16:23.620+0000'
        |WITH count(message) AS totalMessageCountInt
        |WITH toFloat(totalMessageCountInt) AS totalMessageCount
        |MATCH (message:Message)
        |WHERE message.creationDate < '2011-12-16T21:16:23.620+0000'
        |  AND message.content IS NOT NULL
        |WITH
        |  totalMessageCount,
        |  message,
        |  toInteger(substring(message.creationDate,0,4) ) AS year
        |WITH
        |  totalMessageCount,
        |  year,
        |  message:Comment AS isComment,
        |  CASE
        |    WHEN message.length <  40 THEN 0
        |    WHEN message.length <  80 THEN 1
        |    WHEN message.length < 160 THEN 2
        |    ELSE                           3
        |  END AS lengthCategory,
        |  count(message) AS messageCount,
        |  floor(avg(message.length)) AS averageMessageLength,
        |  sum(message.length) AS sumMessageLength
        |RETURN
        |  year,
        |  isComment,
        |  lengthCategory,
        |  messageCount,
        |  averageMessageLength,
        |  sumMessageLength,
        |  messageCount / totalMessageCount AS percentageOfMessages
        |ORDER BY
        |  year DESC,
        |  isComment ASC,
        |lengthCategory ASC
      """.stripMargin



    /// Q2  BI ///


    val  Q2 = 
      """
          |MATCH
          |  (country:Country)<-[:ispartof]-(:City)<-[:islocatedin]-(person:Person)
          |  <-[:hascreator]-(message:Message)-[:hastag]->(tag:Tag)
          |WHERE message.creationDate >= '2010-11-21T14:49:12.199+0000'
          |  AND message.creationDate <= '2012-02-23T07:01:47.331+0000'
          |  AND (country.name = 'Japan' OR country.name = 'Pakistan')
          |WITH
          |  country.name AS countryName,
          |  message.creationDate AS month,
          |  person.gender AS gender,
          |  tag.name AS tagName,
          |  message
          |WITH
          |  countryName, month, gender,tagName, count(message) AS messageCount
          |WHERE messageCount >= 1
          |RETURN
          |  countryName,
          |  month,
          |  gender,
          |  tagName,
          |  messageCount
          |ORDER BY
          |  messageCount DESC,
          |  tagName ASC,
          |  gender ASC,
          |  month ASC,
          |  countryName ASC
          |LIMIT 100
        """.stripMargin  

 /// Q3  BI ///

    val  Q3 = 
    """
      |WITH
      |  2010 AS year1,
      |  10 AS month1,
      |  2010 + toInteger(10/ 12.0) AS year2,
      |  10 % 12 + 1 AS month2
      |MATCH (tag:Tag)
      |OPTIONAL MATCH (message1:Message)-[:hastag]->(tag)
      |  WHERE  toInteger( substring(message1.creationDate,0,4) ) = year1
      |    AND  toInteger( substring(message1.creationDate,5,2) ) = month1
      |WITH year2, month2, tag, count(message1) AS countMonth1

      |OPTIONAL MATCH (message2:Message)-[:hastag]->(tag)
      |  WHERE toInteger(substring(message2.creationDate,0,4) )   = year2
      |    AND toInteger(substring(message2.creationDate,5,2) )   = month2
      |WITH
      |  tag,
      |  countMonth1,
      |  count(message2) AS countMonth2
      |RETURN
      |  tag.name,
      |  countMonth1,
      |  countMonth2,
      |  abs(countMonth1-countMonth2) AS diff
      |ORDER BY
      |  diff DESC,
      |  tag.name ASC
      |LIMIT 100
    """.stripMargin  


    /// Q4  BI 

    val  Q4 = 
      """
        |
        |MATCH
        |  (:Country)<-[:ispartof]-(:City)<-[:islocatedin]-
        |  (person:Person)<-[:hasmoderator]-(forum:Forum)-[:containerof]->
        |  (post:Post)-[:hastag]->(:Tag)-[:hastype]->(:Tagclass)
        |RETURN
        |  forum.title,
        |  forum.creationDate,
        |  person.firstName,
        |  count(DISTINCT post) AS postCount
        |ORDER BY
        |  postCount DESC,
        |  forum.title ASC
        |LIMIT 20
      """.stripMargin  


    /// Q5  BI ///

    val  Q5 = 
      """
        |
        |MATCH
        |  (:Country {name: 'Japan'})<-[:ispartof]-(:City)<-[:islocatedin]-
        |  (person:Person)<-[:hasmember]-(forum:Forum)
        |WITH forum, count(person) AS numberOfMembers
        |ORDER BY numberOfMembers DESC, forum.id ASC
        |LIMIT 100
        |WITH collect(forum) AS popularForums
        |UNWIND popularForums AS forum
        |MATCH
        |  (forum)-[:hasmember]->(person:Person)
        |OPTIONAL MATCH
        |  (person)<-[:hascreator]-(post:Post)<-[:containerof]-(popularForum:Forum)
        |WHERE popularForum IN popularForums
        |RETURN
        |  person.firstName,
        |  person.lastName,
        |  person.creationDate,
        |  count(DISTINCT post) AS postCount
        |ORDER BY
        |  postCount DESC,
        |  person.firstName ASC
        |LIMIT 100
      """.stripMargin  


    /// Q6  BI ///

    val  Q6 = 
      """
        |
        |MATCH
        | (tag:Tag {name: 'Adolf_Hitler'})<-[:hastag]-(message:Message)-[:hascreator]->(person:Person)
        |OPTIONAL MATCH (:Person)-[like:likes]->(message)
        |OPTIONAL MATCH (message)<-[:replyof]-(comment:Comment)
        |WITH person, count(DISTINCT like) AS likeCount, count(DISTINCT comment) AS replyCount, count(DISTINCT message) AS messageCount
        |RETURN
        |  person.firstName,
        |  replyCount,
        |  likeCount,
        |  messageCount,
        |  1*messageCount + 2*replyCount + 10*likeCount AS score
        |ORDER BY
        |  score DESC,
        |  person.firstName ASC
        |LIMIT 100
      """.stripMargin  


    /// Q7  BI ///

    val  Q7 = 
      """
        |
        |MATCH
        | (tag:Tag {name: 'Adolf_Hitler'})
        |MATCH (tag)<-[:hastag]-(message1:Message)-[:hascreator]->(person1:Person)
        |MATCH (tag)<-[:hastag]-(message2:Message)-[:hascreator]->(person1)
        |OPTIONAL MATCH (message2)<-[:likes]-(person2:Person)
        |OPTIONAL MATCH (person2)<-[:hascreator]-(message3:Message)<-[like:likes]-(p3:Person)
        |RETURN
        |  person1.firstName,
        |  count(DISTINCT like) AS authorityScore
        |ORDER BY
        |  authorityScore DESC,
        |  person1.firstName ASC
        |LIMIT 100
      """.stripMargin  


    /// Q8  BI ///

    val  Q8 = 
      """
        |
        |MATCH
        |  (tag:Tag {name: 'Adolf_Hitler'})<-[:hastag]-(message:Message),
        |  (message)<-[:replyof]-(comment:Comment)-[:hastag]->(relatedTag:Tag)
        |WHERE NOT (comment)-[:hastag]->(tag)
        |RETURN
        |  relatedTag.name,
        |  count(DISTINCT comment) AS count
        |ORDER BY
        |  count DESC,
        |  relatedTag.name ASC
        |LIMIT 100
      """.stripMargin  


    /// Q9  BI ///

    val  Q9 = 
      """
        |MATCH
        |  (forum:Forum)-[:hasmember]->(person:Person)
        |WITH forum, count(person) AS members
        |WHERE members >= 10
        |MATCH
        |  (forum)-[:containerof]->(post1:Post)-[:hastag]->
        |  (:Tag)-[:hastype]->(:Tagclass {name:'Actor'})
        |WITH forum, count(DISTINCT post1) AS count1
        |MATCH
        |  (forum)-[:containerof]->(post2:Post)-[:hastag]->
        |  (:Tag)-[:hastype]->(:Tagclass {name:'TennisPlayer'})
        |WITH forum, count1, count(DISTINCT post2) AS count2
        |RETURN
        |  forum.title,
        |  count1,
        |  count2
        |ORDER BY
        |  abs(count2-count1) DESC,
        |  forum.title ASC
        |LIMIT 100
      """
      .stripMargin  




    //Q 10 BI/////   has the the problem of not storing nodes inside lists

    // Q11 Lists Comprehension is not supported by CAPS



    //Q 12 BI/////

    val  Q12 =  """
      |MATCH
      |  (message:Message)-[:hascreator]->(creator:Person),
      |  (message)<-[like:likes]-(:Person)
      |WHERE message.creationDate > '2010-11-21T14:49:12.199+0000'
      |WITH message, creator, count(like) AS likeCount
      |WHERE likeCount > 20
      |RETURN
      |  message.content,
      |  message.creationDate,
      |  creator.firstName,
      |  creator.lastName,
      |  likeCount
      |ORDER BY
      |  likeCount DESC,
      |  message.content ASC
      |LIMIT 100
    """.stripMargin  


    // BI Q13 Working with some workarounds  :)

  val  Q13 = """MATCH
        |    (:Country {name:'Germany'})<-[:islocatedin]-(message:Message)
        |    OPTIONAL MATCH (message)-[:hastag]->(tag:Tag)
        |    WITH
        |      toInteger(substring(message.creationDate,0,4) )   AS year,
        |      toInteger(substring(message.creationDate,5,2) )   AS month,
        |      message,
        |      tag
        |    WITH year, month, count(message) AS popularity, tag
        |    ORDER BY popularity DESC, tag.name ASC
        |    WITH
        |      year,
        |      month,
        |      collect([tag.name, popularity]) AS popularTags
        |    WITH
        |      year,
        |      month,popularTags
        |      RETURN
        |      year,
        |      month,
        |      popularTags[1] AS topPopularTags1, popularTags[2] AS topPopularTags2 , popularTags[3] AS topPopularTags3, popularTags[4] AS topPopularTags4 , popularTags[5]  AS topPopularTags5
        |    ORDER BY
        |      year DESC,
        |      month ASC
        |    LIMIT 100
    """.stripMargin  




    // BI Q14
    // Not Working because of the unbound-variable [:replyof*0..] not already supported by CAPS
    // But working with removing the unbound-variable [:replyof*0..] to be just direct relationship [:replyof]
    val  Q14 = """
      |MATCH (person:Person)<-[:hascreator]-(post:Post)<-[:replyof]-(reply:Message)
      |WHERE  post.creationDate >= '2010-11-21T14:49:12.199+0000'
      |  AND  post.creationDate <= '2012-02-23T07:01:47.331+0000'
      |  AND reply.creationDate >= '2010-11-21T14:49:12.199+0000'
      |  AND reply.creationDate <= '2012-02-23T07:01:47.331+0000'
      |RETURN
      |  person.firstName,
      |  person.lastName,
      |  count(DISTINCT post) AS threadCount,
      |  count(DISTINCT reply) AS messageCount
      |ORDER BY
      |  messageCount DESC,
      |  person.firstName ASC
      |LIMIT 100
    """.stripMargin  




    // BI  Q15 ///////////// Working!! :)

  val  Q15 =  """
      |MATCH
      |  (country:Country {name:'India'})
      |MATCH
      |  (country)<-[:ispartof]-(:City)<-[:islocatedin]-(person1:Person)
      |OPTIONAL MATCH
      |  (country)<-[:ispartof]-(:City)<-[:islocatedin]-(friend1:Person),
      |  (person1)-[:knows]-(friend1)
      |WITH country, person1, count(friend1) AS friend1Count
      |WITH country, avg(friend1Count) AS socialNormalFloat
      |WITH country, floor(socialNormalFloat) AS socialNormal
      |MATCH
      |  (country)<-[:ispartof]-(:City)<-[:islocatedin]-(person2:Person)
      |OPTIONAL MATCH
      |  (country)<-[:ispartof]-(:City)<-[:islocatedin]-(friend2:Person)-[:knows]-(person2)
      |WITH country, person2, count(friend2) AS friend2Count, socialNormal
      |WHERE friend2Count = socialNormal
      |RETURN
      |  person2.firstName,
      |  friend2Count AS count
      |ORDER BY
      |  person2.firstName ASC
      |LIMIT 100
    """.stripMargin  



    // BI Q16 --> has path Queries which are currently supported in CAPS ! may be in the future.



    // BI Q17 //////////////////////

  val  Q17 =  """
      |MATCH (country:Country )
      |MATCH (a:Person)-[:islocatedin]->(:City)-[:ispartof]->(country)
      |MATCH (b:Person)-[:islocatedin]->(:City)-[:ispartof]->(country)
      |MATCH (c:Person)-[:islocatedin]->(:City)-[:ispartof]->(country)
      |MATCH (a)-[:knows]-(b), (b)-[:knows]-(c), (c)-[:knows]-(a)
      |WHERE a.id < b.id
      |  AND b.id < c.id
      |RETURN count(*) AS count
    """.stripMargin  



    // BI Q18
    // Not Working because of the unbound-variable [:replyof*0..] not already supported by CAPS
    // But working with removing the unbound-variable [:replyof*0..] to be just direct relationship [:replyof]

  val  Q18 =  """
      |MATCH (person:Person)
      |OPTIONAL MATCH (person)<-[:hascreator]-(message:Message)-[:replyof]->(post:Post)
      |WHERE message.content IS NOT NULL
      |  AND message.length < 10
      |  AND message.creationDate > '2010-11-21T14:49:12.199+0000'
      |  AND post.language IN ['ar']
      |WITH
      |  person,
      |  count(message) AS messageCount
      |RETURN
      |  messageCount,
      |  count(person) AS personCount
      |ORDER BY
      |  personCount DESC,
      |messageCount DESC
    """.stripMargin  



  // BI Q19
  // Not Working because of the unbound-variable [:replyof*0..] not already supported by CAPS
  // But working with removing the unbound-variable [:replyof*0..] to be just direct relationship [:replyof]



  val  Q19 = """
    |MATCH
    |  (:Tagclass {name:'Actor'})<-[:hastype]-(:Tag)<-[:hastag]-
    |  (forum1:Forum)-[:hasmember]->(stranger:Person)
    |WITH DISTINCT stranger
    |MATCH
    |  (:Tagclass {name:'TennisPlayer'})<-[:hastype]-(:Tag)<-[:hastag]-
    |  (forum2:Forum)-[:hasmember]->(stranger)
    |WITH DISTINCT stranger
    |MATCH
    |  (person:Person)<-[:hascreator]-(comment:Comment)-[:replyof]->(message:Message)-[:hascreator]->(stranger)
    |WHERE person.birthday > '1984-05-11 00:00:00'
    |  AND person <> stranger
    |  AND NOT (person)-[:knows]-(stranger)
    |  AND NOT (message)-[:replyof]->(:Message)-[:hascreator]->(stranger)
    |RETURN
    |  person.firstName,
    |  count(DISTINCT stranger) AS strangersCount,
    |  count(comment) AS interactionCount
    |ORDER BY
    |  interactionCount DESC,
    |  person.firstName ASC
    |LIMIT 100
  """.stripMargin  





    // BI Q20
    // Not Working because of the unbound-variable [:issubclassof*0..] not already supported by CAPS
    // But working with removing the unbound-variable [:issubclassof*0..] to be just direct relationship [:issubclassof]


  val  Q20 =
    """
      |UNWIND ['MusicalWork','PopulatedPlace', 'Cleric' , 'Agent' ] AS tagClassName
      |MATCH
      |  (tagClass:Tagclass {name: tagClassName})<-[:issubclassof]-
      |  (:Tagclass)<-[:hastype]-(tag:Tag)<-[:hastag]-(message:Message)
      |RETURN
      |  tagClass.name,
      |  count(DISTINCT message) AS messageCount
      |ORDER BY
      |  messageCount DESC,
      |  tagClass.name ASC
      |LIMIT 100
    """.stripMargin  




    //BI Q 21
    // Working but need to check it again because zombie in the return is null ?!!!!

    val  Q21 =
    """
      |MATCH (country:Country {name:'China'})
      |WITH
      |  country,
      |   toInteger(substring('2012-02-23T07:01:47.331+0000',0,4) )   AS endDateYear,
      |   toInteger(substring('2012-02-23T07:01:47.331+0000',5,2) ) AS endDateMonth
      |MATCH
      |  (country)<-[:ispartof]-(:City)<-[:islocatedin]-(zombie:Person)
      |OPTIONAL MATCH
      |  (zombie)<-[:hascreator]-(message:Message)
      |WHERE zombie.creationDate  < '2012-02-23T07:01:47.331+0000'
      |  AND message.creationDate < '2012-02-23T07:01:47.331+0000'
      |WITH
      |  country,
      |  zombie,
      |  endDateYear,
      |  endDateMonth,
      |  toInteger(substring(zombie.creationDate, 0, 4))  AS zombieCreationYear,
      |  toInteger(substring(zombie.creationDate, 5, 2)) AS zombieCreationMonth,
      |  count(message) AS messageCount
      |WITH
      |  country,
      |  zombie,
      |  12 * (endDateYear  - zombieCreationYear)
      |     + (endDateMonth - zombieCreationMonth)
      |     + 1 AS months,
      |  messageCount
      |WHERE messageCount / months < 1
      |WITH
      |  country,
      |  collect(zombie) AS zombies
      |UNWIND zombies AS zombie
      |OPTIONAL MATCH
      |  (zombie)<-[:hascreator]-(message:Message)<-[:likes]-(likerZombie:Person)
      |WHERE likerZombie IN zombies
      |WITH
      |  zombie,
      |  count(likerZombie) AS zombieLikeCount
      |OPTIONAL MATCH
      |  (zombie)<-[:hascreator]-(message:Message)<-[:likes]-(likerPerson:Person)
      |WHERE likerPerson.creationDate < '2012-02-23T07:01:47.331+0000'
      |WITH
      |  zombie,
      |  zombieLikeCount,
      |  count(likerPerson) AS totalLikeCount
      |RETURN
      |  zombie.firstName,
      |  zombieLikeCount,
      |  totalLikeCount,
      |  CASE totalLikeCount
      |    WHEN 0 THEN 0.0
      |    ELSE zombieLikeCount / toFloat(totalLikeCount)
      |  END AS zombieScore
      |ORDER BY
      |  zombieScore DESC,
      |  zombie.firstName ASC
      |LIMIT 100
    """.stripMargin  



  // BI  Q22
  // It's very very expensive query :( But still Running 

  val  Q22 =
    """
 |MATCH
 |  (country1:Country{name:'China'})<-[:ispartof]-(city1:City)<-[:islocatedin]-(person1:Person),
 |  (country2:Country{name:'Germany'})<-[:ispartof]-(city2:City)<-[:islocatedin]-(person2:Person)
 |WITH person1, person2, city1, 0 AS score
 |// subscore 1
 |OPTIONAL MATCH (person1)<-[:hascreator]-(c:Comment)-[:replyof]->(:Message)-[:hascreator]->(person2)
 |WITH DISTINCT person1, person2, city1, score + (CASE c WHEN null THEN 0 ELSE  4 END) AS score
 |// subscore 2
 |OPTIONAL MATCH (person1)<-[:hascreator]-(m:Message)<-[:replyof]-(:Comment)-[:hascreator]->(person2)
 |WITH DISTINCT person1, person2, city1, score + (CASE m WHEN null THEN 0 ELSE  1 END) AS score
 |// subscore 3
 |OPTIONAL MATCH (person1)-[k:knows]-(person2)
 |WITH DISTINCT person1, person2, city1, score + (CASE k WHEN null THEN 0 ELSE 15 END) AS score
 |// subscore 4
 |OPTIONAL MATCH (person1)-[:likes]->(m:Message)-[:hascreator]->(person2)
 |WITH DISTINCT person1, person2, city1, score + (CASE m WHEN null THEN 0 ELSE 10 END) AS score
 |// subscore 5
 |OPTIONAL MATCH (person1)<-[:hascreator]-(m:Message)<-[:likes]-(person2)
 |WITH DISTINCT person1, person2, city1, score + (CASE m WHEN null THEN 0 ELSE  1 END) AS score
 |// preorder
 |ORDER BY
 |  city1.name ASC,
 |  score DESC,
 |  person1.firstName ASC,
 |  person2.firstName ASC
 |WITH
 |  city1,
 |  // using a list might be faster, but the browser query editor does not like it
 |  collect({score: score, person1: person1, person2: person2})[0] AS top
 |RETURN
 |
 |  top.person1.firstName,
 |  city1.name,
 |  top.score
 |ORDER BY
 |  top.score DESC,
 |  top.person1.id ASC,
 |top.person2.id ASC
      """.stripMargin





  //Q23 Running :)

  val  Q23=
    """
      |MATCH
      |  (home:Country{name:'Germany'})<-[:ispartof]-(:City)<-[:islocatedin]-
      |  (:Person)<-[:hascreator]-(message:Message)-[:islocatedin]->(destination:Country)
      |WHERE home <> destination
      |WITH
      |  message,
      |  destination,
      |  toInteger(substring(message.creationDate, 5, 2)) AS month
      |RETURN
      |  count(message) AS messageCount,
      |  destination.name,
      |  month
      |ORDER BY
      |  messageCount DESC,
      |  destination.name ASC,
      |  month ASC
      |LIMIT 100
    """.stripMargin



  val  Q24 =
    """
      |MATCH (:Tagclass {name:'Single'})<-[:hastype]-(:Tag)<-[:hastag]-(message:Message)
      |WITH DISTINCT message
      |MATCH (message)-[:islocatedin]->(:Country)-[:ispartof]->(continent:Continent)
      |OPTIONAL MATCH (message)<-[like:likes]-(:Person)
      |WITH
      |  message,
      |  toInteger(substring(message.creationDate, 0, 4))  AS year,
      |  toInteger(substring(message.creationDate, 5, 2)) AS month,
      |  like,
      |  continent
      |RETURN
      |  count(DISTINCT message) AS messageCount,
      |  count(like) AS likeCount,
      |  year,
      |  month,
      |  continent.name
      |ORDER BY
      |  year ASC,
      |  month ASC,
      |  continent.name DESC
      |LIMIT 100
    """.stripMargin  



  //Q25 weighted paths are not currently supported by CAPS



}
