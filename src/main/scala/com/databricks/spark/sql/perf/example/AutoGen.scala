package com.databricks.spark.sql.perf.example

import com.databricks.spark.sql.perf.{Benchmark, ExperimentRun}
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.functions.{col, substring}
import org.codehaus.jackson.JsonNode
import org.codehaus.jackson.map.ObjectMapper

import java.util


case class DataQuality(name: String, tables: Seq[String], result: Option[Long])

object AutoGenAndRunTest {
  val mapper = new ObjectMapper()

  def parseCrcSum(experimentRunNode: JsonNode, name2Result: util.HashMap[String, String]) = {
    val resultsNode = experimentRunNode.get("results")
    if (resultsNode.isArray) {
      resultsNode.forEach(result => {
        val nameNode = result.get("name")
//        val tablesNode = result.get("tables")
        val resultNode = result.get("result")
        name2Result.put(nameNode.toString, resultNode.toString)
      })
    } else {
      println("resultsNode is not an array node, failed!")
    }
  }

  def main(args: Array[String]): Unit = {
    val dbName = "vip_tpcDs_databricks"
    val dbLocation = "hdfs://bipcluster08/bip/hive_warehouse/tpcds/databricks_big_data"
    val resultLocation = "hdfs://bipcluster08/bip/hive_warehouse/tpcds/databricks_output_big_data"

    // todo 安装tpc_ds 暂时最好手动安装tpcds
    import com.databricks.spark.sql.perf.utils.Utils
    Utils.installTpcds()

    // 生成数据
    import com.databricks.spark.sql.perf.tpcds.GenTPCDSData
    //    val arg = "-d /home/hdfs/xuefei/tpcds/DSGen-software-code-3.2.0rc1/tools -s 1 -l hdfs://bipcluster08/bip/hive_warehouse/tpcds/databricks -f ORC -n 1 -t web_returns"
    val genData = s"-d /tmp/tpcds/DSGen-software-code-3.2.0rc1/tools -s 20 -l ${dbLocation} -f ORC -n 20 -o true"
    GenTPCDSData.main(genData.split(" "))

    // 生成db和table
    // todo 去掉必须要有-d的限制
    val dbParam = s"-l $dbLocation -f ORC -b $dbName -a genTable -d identifier -s 1 "
    GenTPCDSData.main(dbParam.split(" "))

    // 测试queries,data quality
    val name2Result = new util.HashMap[String, String]()
    val experiment = runTest(SparkSession.getActiveSession.get, dbName, resultLocation, true)
    val experimentRun = parseResult(SparkSession.getActiveSession.get, resultLocation, experiment)
    parseCrcSum(experimentRun, name2Result)
    println(name2Result)
  }


  def runTest(spark: SparkSession, databaseName: String,
              resultLocation: String, dataQuality: Boolean = false) = {
    import com.databricks.spark.sql.perf.tpcds.TPCDS
    val tpcds = new TPCDS (sqlContext = spark.sqlContext)
    val iterations = 1 // how many iterations of queries to run.
    val queries =
      if (dataQuality) {
        println("start check dataQuality!")
        tpcds.tpcds2_4Queries.map(q => q.checkResult) // queries to run.
      } else tpcds.tpcds2_4Queries
    val timeout = 24*60*60 // timeout, in seconds.
    // Run:
    spark.sql(s"use $databaseName")
    val experiment = tpcds.runExperiment(
      queries,
      iterations = iterations,
      resultLocation = resultLocation,
      forkThread = true)
    experiment.waitForFinish(timeout)
    experiment
  }

  def parseResult(spark: SparkSession,
                  resultLocation: String,
                  experiment: Benchmark.ExperimentStatus) = {
    // Get all experiments results.
    import org.apache.spark.sql.types._
    val schema = StructType(Array(
      StructField("configuration", StringType),
      StructField("iteration", IntegerType),
      StructField("results", ArrayType(StructType(Array(
        StructField("name", StringType),
        StructField("mode", StringType),
        StructField("parameters", MapType(StringType, StringType)),
        StructField("joinTypes", ArrayType(StringType)),
        StructField("tables", ArrayType(StringType)),
        StructField("parsingTime", DoubleType),
        StructField("analysisTime", DoubleType),
        StructField("optimizationTime", DoubleType),
        StructField("planningTime", DoubleType),
        StructField("executionTime", DoubleType),
        StructField("result", LongType),
        StructField("breakDown", ArrayType(StructType(
          Array(
            StructField("nodeName", StringType),
            StructField("nodeNameWithArgs", StringType),
            StructField("index", IntegerType),
            StructField("children", ArrayType(IntegerType)),
            StructField("executionTime", DoubleType),
            StructField("delta", DoubleType)
          )
        ))),
        StructField("queryExecution", StringType),
        StructField("failure", StructType(Array(StructField("className", StringType), StructField("message", StringType)))),
        StructField("mlResult", ArrayType(StructType(Array(
          StructField("metricName", StringType),
          StructField("metricValue", DoubleType),
          StructField("isLargerBetter", BooleanType)
        )))),
        StructField("benchmarkId", StringType)
      )))),
      StructField("tags", StringType),
      StructField("timestamp", LongType)
    ))

    val resultL = "hdfs://bipcluster08/bip/hive_warehouse/tpcds/databricks_output/timestamp=1643172874743/*"
    val resultTable = spark.read.schema(schema).json(resultL)
    resultTable.createOrReplaceTempView("sqlPerformance")
    val result = spark.sql("select results from sqlPerformance ").collectAsList()
    val firstRow = result.get(0)

    val experimentRunNode = mapper.readTree(firstRow.prettyJson)
    // todo 保存结果，且默认不展示执行计划
    experimentRunNode
  }



  def testFile(spark: SparkSession): Unit = {

    spark.sparkContext.parallelize(1 to 4, 4).map { i =>
      new java.io.File("/tmp/tpcds/DSGen-software-code-3.2.0rc1/tools/dsdgen").exists
    }.collect()
  }

}
