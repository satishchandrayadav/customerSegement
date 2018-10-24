package com.mycompany.utils

import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.types.{LongType, StringType, StructField, StructType}

object jobStatistics {

  def getJobStatistics(programName: String, inputCount: Long, insertCount: Long, status: String, loadDate: String,
                       jobMetricsWritePath: String) {
    val createDataForDF = Seq(
      Row(programName, inputCount, insertCount, status, loadDate)
    )
    val TableSchema = List(
      StructField("programName", StringType, true),
      StructField("inputCount", LongType, true),
      StructField("insertCount", LongType, true),
      StructField("status", StringType, true),
      StructField("loadDate", StringType, true)
    )

    val spark: SparkSession = SparkSession.builder()
      .appName("Customer Segment")
      .master("local[*]")
      .config("log4j.configuration", "file:///root/spark-2.3.0-bin-hadoop2.7/conf/log4j.propertiessome-value")
      /*Since Hive is not installed, this property is not required. */
      /*.enableHiveSupport() /*to enable support for Hive */ */
      .getOrCreate()

    val sqlContext = spark.sqlContext

    val JobMetricsDF = spark.createDataFrame(
      spark.sparkContext.parallelize(createDataForDF),
      StructType(TableSchema)
    )
    val outputData = JobMetricsDF.coalesce(1).write.mode("append").option("header", "true")
      .csv(jobMetricsWritePath)


  }


}
