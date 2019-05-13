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

package org.opencypher.spark.MacroBenchmarking

import java.io.{BufferedWriter, FileWriter}

import org.opencypher.okapi.api.graph.Namespace
import org.opencypher.okapi.impl.util.Measurement
import org.opencypher.spark.api.{CAPSSession, GraphSources}
import org.opencypher.spark.util.ConsoleApp

object HDFSParquetLDBCMacroExample extends ConsoleApp{


  println("This example Demo tries to evaluate different queries of LDBC against HDFS Parquet data source")


  def writeToFile(p: String, s: String): Unit = {
    val pw = new BufferedWriter(new FileWriter(p,true))
    try pw.write(s+"\n") finally pw.close()
  }


  val parquetResultsFilePath ="/home/cloudera/CAPSBenchMarking/ResultsAtomicLevel/Parquetresults.txt"
  val queries= new QueriesAtomicLevel


  // 1) Create CAPS session
  implicit val capsSession: CAPSSession = CAPSSession.local()





  val parquetPGDS = GraphSources.fs("hdfs://quickstart:8020/user/cloudera/CAPS/parquet").parquet
  capsSession.registerSource(Namespace("LDBCGraphNSHDFSParquet"), parquetPGDS)

  val parquetGraph =capsSession.catalog.graph("LDBCGraphNSHDFSParquet.myLDBCGraphParquet")


  
  val timeQ1 =Measurement.time(parquetGraph.cypher(queries.Q1).records.show)
  writeToFile(parquetResultsFilePath,"Q1:"+timeQ1.toString())

  val timeQ2 =Measurement.time(parquetGraph.cypher(queries.Q2).records.show)
  writeToFile(parquetResultsFilePath,"Q2:"+timeQ2.toString())

  val timeQ3 =Measurement.time(parquetGraph.cypher(queries.Q3).records.show)
  writeToFile(parquetResultsFilePath,"Q3:"+timeQ3.toString())

  val timeQ4 =Measurement.time(parquetGraph.cypher(queries.Q4).records.show)
  writeToFile(parquetResultsFilePath,"Q4:"+timeQ4.toString())

  val timeQ5 =Measurement.time(parquetGraph.cypher(queries.Q5).records.show)
  writeToFile(parquetResultsFilePath,"Q5:"+timeQ5.toString())

  val timeQ6 =Measurement.time(parquetGraph.cypher(queries.Q6).records.show)
  writeToFile(parquetResultsFilePath,"Q6:"+timeQ6.toString())

  val timeQ7 =Measurement.time(parquetGraph.cypher(queries.Q7).records.show)
  writeToFile(parquetResultsFilePath,"Q7:"+timeQ7.toString())

  val timeQ8 =Measurement.time(parquetGraph.cypher(queries.Q8).records.show)
  writeToFile(parquetResultsFilePath,"Q8:"+timeQ8.toString())

  val timeQ9 =Measurement.time(parquetGraph.cypher(queries.Q9).records.show)
  writeToFile(parquetResultsFilePath,"Q9:"+timeQ9.toString())

  val timeQ10 =Measurement.time( parquetGraph.cypher(queries.Q10).records.show)
  writeToFile(parquetResultsFilePath,"Q10:"+timeQ10.toString())

  val timeQ11 =Measurement.time( parquetGraph.cypher(queries.Q11).records.show)
  writeToFile(parquetResultsFilePath,"Q11:"+timeQ11.toString())


  val timeQ12 =Measurement.time( parquetGraph.cypher(queries.Q12).records.show)
  writeToFile(parquetResultsFilePath,"Q12:"+timeQ12.toString())

  val timeQ13 =Measurement.time( parquetGraph.cypher(queries.Q13).records.show)
  writeToFile(parquetResultsFilePath,"Q13:"+timeQ13.toString())

  val timeQ14 =Measurement.time( parquetGraph.cypher(queries.Q14).records.show)
  writeToFile(parquetResultsFilePath,"Q14:"+timeQ14.toString())


  val timeQ15 =Measurement.time( parquetGraph.cypher(queries.Q15).records.show)
  writeToFile(parquetResultsFilePath,"Q15:"+timeQ15.toString())

  val timeQ16 =Measurement.time( parquetGraph.cypher(queries.Q16).records.show)
  writeToFile(parquetResultsFilePath,"Q16:"+timeQ16.toString())

  val timeQ17 =Measurement.time( parquetGraph.cypher(queries.Q17).records.show)
  writeToFile(parquetResultsFilePath,"Q17:"+timeQ17.toString())

  val timeQ18 =Measurement.time( parquetGraph.cypher(queries.Q18).records.show)
  writeToFile(parquetResultsFilePath,"Q18:"+timeQ18.toString())


}
