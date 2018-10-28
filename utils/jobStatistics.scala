package com.mycompany.utils

import java.sql.Timestamp
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.types._

object jobStatistics {

  val spark: SparkSession = SparkSession.builder()
    .appName("Customer Segment")
    .master("local[*]")
    .config("log4j.configuration", "file:///root/spark-2.3.0-bin-hadoop2.7/conf/log4j.propertiessome-value")
    /*Since Hive is not installed, this property is not required. */
    /*.enableHiveSupport() /*to enable support for Hive */ */
    .getOrCreate()

  val sqlContext = spark.sqlContext


  def elapsedTime (startDate :Timestamp):String = {

    var diff = endDate.getTime() - startDate.getTime()
    var diffSeconds = diff / 1000 % 60
    var diffMinutes = diff / (60 * 1000) % 60
    var diffHours = diff / (60 * 60 * 1000) % 24
    var diffDays = diff / (24 * 60 * 60 * 1000)
    var duration = diffHours + ":" + diffMinutes + ":" + diffSeconds

    return duration
  }


  val endDate: Timestamp = new Timestamp(System.currentTimeMillis)

  def getJobStatistics(programName: String, inputCount: Long, insertCount: Long, status: String, loadDate: String,
                       jobMetricsWritePath: String,startDate :Timestamp, endDate :Timestamp, elapsedTime: String) {

    val createDataForDF = Seq(
      Row(programName, inputCount, insertCount, status, loadDate, startDate, endDate, elapsedTime)
    )
    val TableSchema = List(
      StructField("programName", StringType, true),
      StructField("inputCount", LongType, true),
      StructField("insertCount", LongType, true),
      StructField("status", StringType, true),
      StructField("loadDate", StringType, true),
      StructField("startDate", TimestampType , true),
      StructField("endDate", TimestampType , true),
      StructField("elapsedTime", StringType, true)

    )

    val JobMetricsDF = spark.createDataFrame(
      spark.sparkContext.parallelize(createDataForDF),
      StructType(TableSchema)
    )
    val outputData = JobMetricsDF.coalesce(1).write.mode("append").option("header", "true")
      .csv(jobMetricsWritePath)

  }


}
