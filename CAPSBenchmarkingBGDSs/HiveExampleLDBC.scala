/*
 * Copyright (c) 2016-2019 "Neo4j Sweden, AB" [https://neo4j.com]
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 * Attribution Notice under the terms of the Apache License 2.0
 *
 * This work was created by the collective efforts of the openCypher community.
 * Without limiting the terms of Section 6, any Derivative Work that is not
 * approved by the public consensus process of the openCypher Implementers Group
 * should not be described as “Cypher” (and Cypher® is a registered trademark of
 * Neo4j Inc.) or as "openCypher". Extensions by implementers or prototypes or
 * proposals for change that have been documented or implemented should only be
 * described as "implementation extensions to Cypher" or as "proposed changes to
 * Cypher that are not yet approved by the openCypher community".
 */

package org.opencypher.spark.ragabexamples


import java.io.{BufferedWriter, FileWriter}
import java.util.UUID


import org.apache.spark.sql.SparkSession
import org.opencypher.okapi.api.graph.Namespace
import org.opencypher.okapi.impl.util.Measurement
import org.opencypher.spark.api.io.sql.SqlDataSourceConfig
import org.opencypher.spark.api.{CAPSSession, GraphSources}
import org.opencypher.spark.util.ConsoleApp


object HiveExampleLDBC extends ConsoleApp {


  def writeToFile(p: String, s: String): Unit = {
    val pw = new BufferedWriter(new FileWriter(p,true))
    try pw.write(s+"\n") finally pw.close()
  }


  val hiveResultsFilePath ="/home/cloudera/CAPSBenchMarking/Results/hiveresults.txt"
  val queries= new Queries()



  implicit val resourceFolder: String = "/ldbc"  //resources 'ldbc' folder

  implicit  val sparkSession = SparkSession
    .builder()
    .master("local[*]")
    .config("hive.metastore.uris","thrift://quickstart.cloudera:9083")
    .enableHiveSupport()
    .appName(s"caps-local-${UUID.randomUUID()}")
    .getOrCreate()

  implicit val capssession:CAPSSession= CAPSSession.create(sparkSession)

  
    val sqlGraphSource = GraphSources.sql(resource("ddl/LDBCNew.ddl").getFile)
      .withSqlDataSourceConfigs(
        "HIVE" -> SqlDataSourceConfig.Hive
      )


    capssession.registerSource(Namespace("LDBCGraphNS"), sqlGraphSource)
    val myLDBCGraph =capssession.catalog.graph("LDBCGraphNS." + "LDBCGraph")



  val timeQ1 =Measurement.time(myLDBCGraph.cypher(queries.Q1).records.show)
  writeToFile(hiveResultsFilePath,"Q1:"+timeQ1.toString())

  val timeQ2 =Measurement.time(myLDBCGraph.cypher(queries.Q2).records.show)
  writeToFile(hiveResultsFilePath,"Q2:"+timeQ2.toString())

  val timeQ3 =Measurement.time(myLDBCGraph.cypher(queries.Q3).records.show)
  writeToFile(hiveResultsFilePath,"Q3:"+timeQ3.toString())

  val timeQ4 =Measurement.time(myLDBCGraph.cypher(queries.Q4).records.show)
  writeToFile(hiveResultsFilePath,"Q4:"+timeQ4.toString())

  val timeQ5 =Measurement.time(myLDBCGraph.cypher(queries.Q5).records.show)
  writeToFile(hiveResultsFilePath,"Q5:"+timeQ5.toString())

  val timeQ6 =Measurement.time(myLDBCGraph.cypher(queries.Q6).records.show)
  writeToFile(hiveResultsFilePath,"Q6:"+timeQ6.toString())

  val timeQ7 =Measurement.time(myLDBCGraph.cypher(queries.Q7).records.show)
  writeToFile(hiveResultsFilePath,"Q7:"+timeQ7.toString())

  val timeQ8 =Measurement.time(myLDBCGraph.cypher(queries.Q8).records.show)
  writeToFile(hiveResultsFilePath,"Q8:"+timeQ8.toString())

  val timeQ9 =Measurement.time(myLDBCGraph.cypher(queries.Q9).records.show)
  writeToFile(hiveResultsFilePath,"Q9:"+timeQ9.toString())

  val timeQ12 =Measurement.time(myLDBCGraph.cypher(queries.Q12).records.show)
  writeToFile(hiveResultsFilePath,"Q12:"+timeQ12.toString())

  val timeQ13 =Measurement.time(myLDBCGraph.cypher(queries.Q13).records.show)
  writeToFile(hiveResultsFilePath,"Q13:"+timeQ13.toString())

  val timeQ14 =Measurement.time(myLDBCGraph.cypher(queries.Q14).records.show)
  writeToFile(hiveResultsFilePath,"Q14:"+timeQ14.toString())

  val timeQ15 =Measurement.time(myLDBCGraph.cypher(queries.Q15).records.show)
  writeToFile(hiveResultsFilePath,"Q15:"+timeQ15.toString())

  val timeQ17 =Measurement.time(myLDBCGraph.cypher(queries.Q17).records.show)
  writeToFile(hiveResultsFilePath,"Q17:"+timeQ17.toString())

  val timeQ18 =Measurement.time(myLDBCGraph.cypher(queries.Q18).records.show)
  writeToFile(hiveResultsFilePath,"Q18:"+timeQ18.toString())

  val timeQ19 =Measurement.time(myLDBCGraph.cypher(queries.Q19).records.show)
  writeToFile(hiveResultsFilePath,"Q19:"+timeQ19.toString())

  val timeQ20 =Measurement.time(myLDBCGraph.cypher(queries.Q20).records.show)
  writeToFile(hiveResultsFilePath,"Q20:"+timeQ20.toString())

  val timeQ21 =Measurement.time(myLDBCGraph.cypher(queries.Q21).records.show)
  writeToFile(hiveResultsFilePath,"Q21:"+timeQ21.toString())

  val timeQ22 =Measurement.time(myLDBCGraph.cypher(queries.Q22).records.show)
  writeToFile(hiveResultsFilePath,"Q22:"+timeQ22.toString())

  val timeQ23 =Measurement.time(myLDBCGraph.cypher(queries.Q23).records.show)
  writeToFile(hiveResultsFilePath,"Q23:"+timeQ23.toString())

  val timeQ24 =Measurement.time(myLDBCGraph.cypher(queries.Q24).records.show)
  writeToFile(hiveResultsFilePath,"Q24:"+timeQ24.toString())


}




