# Morpheus Performance Benchmarking with differnt Property Graph Backends.

The goal of this  expriment is to Benchmark the performance of Morpheus or as it was known Cypher for Apache Spark (CAPS) ; or it is called currently 'Morpheus', on top of its supported property graph storage backends. In this Benchamrking expriment, we are running 21 BI queries (valid to run) in Morpheus on top  of different storage backends (BGDSs): Neo4j, Hive, and HDFS(CSV,Parquet,ORC).

As we can seen from tables and figures that Hive has the worst performance in general for running most of the queries even they are not that complex. HDFS Backends in general outperform Neo4j  and Hive. In particular Parquet format in HDFS has the best performance, it outperforms ORC and CSV in most cases of running the queries.



## Quick Setup

1- Generate the LDBC dataset using the LDBC data generator: https://github.com/ldbc/ldbc_snb_datagen 
- for this we have this generator on seperated machine that doesn't have Hadoop, as it needs Hadoop 2.6.
we have this on machine 'Centos Machine 4'.

2- The data will be generated with the SF that is configured in the data generator in CSV.

3- For Loading LDBC Data into neo4j as property graphs:
- I investigated this project (ldbc-snb-impls) to load the data (generated as csv files) in the format that the neo4j import tool expects.
- Created a jar file that take as input the directory of the LDBC data and generate the new data into the output given directory.
- This project has generated the data that will create a property graph, it will be unfortunately different from the one that is in Graph DDL.


4-	Created python scripts to pre-process (Normalize) the data generated from this project to adhere with the LDBC property graph of Hive:

- normalize place to City, Country, continent
- normalize organization to company and university
- normalize person_islocatedin_place to person_islocatedin_city
- normalize comment_islocatedin_place to comment_islocatedin_country
- normalize organisation_islocatedin_place to university_islocatedin_city
- normalize organisation_islocatedin_place to company_islocatedin_country
- normalize person_studyat_organisation to  person_studyat_university
- normalize person_workat_organisation to  person_workat_company
-normalize place_ispartof_place to  city_ispartof_country and country_ispartof_continent

5- Prepare the import script (neo4j admin import tool) for all nodes and relationships to load them into a neo4j graph DB.

6- Load the data to neo4j and check the newly created schema.

7- Solved the issue of Converting the GMT DateTime for Morpheus Cypher into Months and Years. (a lot of queries depend on this conversion).

8- Morpheus- LDBC Hive schema (we want to have a schema in hive similar to neo4j and hdfs): 

- Uploading the raw generated LDBC-CSV files with:
- Converting the DateTime to GMT file format.
- Using the Person preprocessed file prepared for neo4j import (correcting its header to original, removing the TYPE column).

9-	Used the LDBC hive example of Morpheus to create LDBC graph, and used this to generate the LDBC graph out of it:








