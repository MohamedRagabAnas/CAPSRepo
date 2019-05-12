# CAPS Performance Benchmarking with differnt Property Graph Backends.

The goal of this  expriment is to Benchmark the performance of Cypher for Apache Spark (CAPS) ; or it is called currently 'Morpheus', on top of its supported property graph storage backends. In this Benchamrking expriment, we are running 21 BI queries (valid to run) in CAPS on top  of different storage backends (BGDSs): Neo4j, Hive, and HDFS(CSV,Parquet,ORC).

As we can seen from tables and figures that Hive has the worst performance in general for running most of the queries even they are not that complex. HDFS Backends in general outperform Neo4j  and Hive. In particular Parquet format in HDFS has the best performance, it outperforms ORC and CSV in most cases of running the queries.
