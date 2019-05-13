package org.opencypher.spark.MacroBenchmarking

class QueriesAtomicLevel {



  //All node Scan Query
  val Q1= "MATCH (m:Message)  RETURN m"



  //Predicates Adding
  val Q2= "MATCH (m:Message {browserUsed:'Opera'}) RETURN m"

  // Adding more predicates
  val Q3="MATCH (m:Message {browserUsed:'Opera'}) WHERE m.length > 10  RETURN m"

  //Projection 1 column Adding, with predicates
  val Q4= "MATCH (m:Message {browserUsed:'Opera'}) RETURN m.length"


  //Projection 2 columns Adding, with predicates
  val Q5= "MATCH (m:Message {browserUsed:'Opera'}) RETURN m.creationDate,m.length"


  //Projection 3  columns Adding, with predicates
  val Q6= "MATCH (m:Message {browserUsed:'Opera'}) RETURN m.creationDate,m.length,m.locationIP"



  //Projection 1 column Adding, without predicates
  val Q7= "MATCH (m:Message) RETURN m.length"



  //Projection 2 columns Adding, without predicates
  val Q8= "MATCH (m:Message) RETURN m.creationDate,m.length"


  //Projection 3  columns Adding, without predicates
  val Q9= "MATCH (m:Message) RETURN m.creationDate,m.length,m.locationIP"



  //Expand with one join

  val Q10="MATCH (message:Message) -[:hastag]-> (tag:Tag) RETURN message,tag"


  //Expand with two joins

  val Q11="MATCH (message:Message)-[:hastag]->(tag:Tag)-[:hastype]->(tagclass:Tagclass) RETURN message,tag,tagclass"


  //Expand with three joins

  val Q12=
    """MATCH (message:Message) -[:hastag]-> (tag:Tag)-[:hastype]->(tagclass:Tagclass), (message)-[:hascreator]->(person:Person)
      |RETURN person,message,tag,tagclass
    """.stripMargin


  //Expand with four joins

  val Q13=
    """
      |MATCH (message:Message) -[:hastag]-> (tag:Tag)-[:hastype]->(tagclass:Tagclass),
      |(message)-[:hascreator]->(person:Person)-[:islocatedin]->(city:City)
      |RETURN person,city, message,tag,tagclass
    """.stripMargin


  //Expand with five joins

  val Q14=
    """
      |MATCH (message:Message) -[:hastag]-> (tag:Tag)-[:hastype]->(tagclass:Tagclass),
      |(message)-[:hascreator]->(person:Person)-[:islocatedin]->(city:City)-[:ispartof]->(country:Country)
      |RETURN person,city, country, message,tag,tagclass
    """.stripMargin


  //Expand with six joins

  val Q15=
    """
      |MATCH (message:Message) -[:hastag]-> (tag:Tag)-[:hastype]->(tagclass:Tagclass),
      |(message)-[:hascreator]-(person:Person)-[:islocatedin]->(city:City)-[:ispartof]->(country:Country)-[:ispartof]->(continent:Continent)
      |RETURN person,city, country, continent,message,tag,tagclass
    """.stripMargin


  // Expand with six joins + Sorting by one variable

  val Q16=
    """
      |MATCH (message:Message) -[:hastag]-> (tag:Tag)-[:hastype]->(tagclass:Tagclass),
      |(message)-[:hascreator]-(person:Person)-[:islocatedin]->(city:City)-[:ispartof]->(country:Country)-[:ispartof]->(continent:Continent)
      |RETURN person,city, country, continent,message,tag,tagclass
      |ORDER BY person.firstName ASC
    """.stripMargin




  // Expand with six joins + Sorting by two variable

  val Q17=
    """
      |MATCH (message:Message) -[:hastag]-> (tag:Tag)-[:hastype]->(tagclass:Tagclass),
      |(message)-[:hascreator]-(person:Person)-[:islocatedin]->(city:City)-[:ispartof]->(country:Country)-[:ispartof]->(continent:Continent)
      |RETURN person,city, country, continent,message,tag,tagclass
      |ORDER BY person.firstName ASC, city.name DESC
    """.stripMargin


  // Expand with six joins + Sorting by three variable

  val Q18=
    """
      |MATCH (message:Message) -[:hastag]-> (tag:Tag)-[:hastype]->(tagclass:Tagclass),
      |(message)-[:hascreator]-(person:Person)-[:islocatedin]->(city:City)-[:ispartof]->(country:Country)-[:ispartof]->(continent:Continent)
      |RETURN person,city, country, continent,message,tag,tagclass
      |ORDER BY person.firstName ASC, city.name DESC, country.name ASC
    """.stripMargin




}
