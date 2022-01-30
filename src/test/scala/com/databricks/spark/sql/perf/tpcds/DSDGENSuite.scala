package com.databricks.spark.sql.perf.tpcds

import org.apache.spark.sql.SparkSession
import org.scalatest.FunSuite

class DSDGENSuite extends FunSuite {

  test("TEST generate all files to hdfs") {
    val spark = SparkSession.getActiveSession.get

    import com.databricks.spark.sql.perf.tpcds.DSDGEN
    val resultLocation = "hdfs://bipcluster08/bip/hive_warehouse/tpcds/databricks_output_test"
    val dsdgen = new DSDGEN("/tmp/tpcds/DSGen-software-code-3.2.0rc1/tools")
    dsdgen.generateAll(spark.sparkContext, resultLocation, 2, "2")
  }

  test("TEST generate files to hdfs finally") {
    val spark = SparkSession.getActiveSession.get

    import com.databricks.spark.sql.perf.tpcds.DSDGEN
    val resultLocation = "hdfs://bipcluster08/bip/hive_warehouse/tpcds/databricks_output_test"
    val dsdgen = new DSDGEN("/tmp/tpcds/DSGen-software-code-3.2.0rc1/tools")
    dsdgen.generate(spark.sparkContext, resultLocation, 2, "2")
  }
}
