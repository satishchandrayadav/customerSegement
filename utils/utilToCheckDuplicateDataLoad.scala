package com.mycompany.utils
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import java.util.{Calendar, Date, Properties}


object utilToCheckDuplicateDataLoad  {

  def getMaxLoadDate(programName:String, path:String , loadDate :String ) : Any =  {

    val TableSchema = StructType(Array(
      StructField("programName", StringType, true),
      StructField("inputCount", LongType, true),
      StructField("insertCount", IntegerType, true),
      StructField("status", StringType, true),
      StructField("loadDate", StringType, true)
    ))

    val spark: SparkSession = SparkSession.builder()
      .appName("Customer Segment")
      .master("local[*]")
      .config("log4j.configuration", "file:///root/spark-2.3.0-bin-hadoop2.7/conf/log4j.propertiessome-value")
      /*Since Hive is not installed, this property is not required. */
      /*.enableHiveSupport() /*to enable support for Hive */ */
      .getOrCreate()

    val sqlContext = spark.sqlContext

    val jobMetricsDf = spark.read.format("csv").schema(TableSchema).load(path)
    jobMetricsDf.createOrReplaceTempView("CheckLoadDate")


    val maxLoadDate  = sqlContext.sql(s""" select max(loadDate)  from
           CheckLoadDate where programName  =  "${programName}" """).first().get(0)

     print(s"satishyadav: ${maxLoadDate}")
    println(loadDate)

    if (loadDate ==  maxLoadDate ) {println("Duplicate data exist")
      throw error("Duplicate data exist") }
    else println("This is else statement")

  }

}
