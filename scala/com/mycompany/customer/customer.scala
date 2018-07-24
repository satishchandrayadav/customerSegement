package com.mycompany.customer

import com.mycompany.utils.InitSpark
import org.apache.spark.sql.DataFrame

class customer extends InitSpark {

  val customerSourceDataPath: String = spark.read.option("multiline", value = true)
    .json("/customerSegment/src/main/scala/com/mycompany/config/customerConfig.json")
    .select(s"${deployment_environment}.tables.customer_detail.table_location")
    .rdd
    .collect()
    .mkString(" ")
    .replaceAll("[\\[\\]]", "")


  val customerSavePath: String = spark.read.option("multiline", value = true)
    .json("/customerSegment/src/main/scala/com/mycompany/config/customerConfig.json")
    .select(s"${deployment_environment}.tables.customer_dim.table_location")
    .rdd
    .collect()
    .mkString(" ")
    .replaceAll("[\\[\\]]", "")


  println(s"input table path : $customerSourceDataPath")
  println(s"input table path : $customerSavePath")



  val jobMetricsWritePath = spark.read.option("multiline",true)
    .json("/customerSegment/src/main/scala/com/mycompany/config/customerConfig.json")
    .select(s"${deployment_environment}.tables.job_metrics.table_location")
    .rdd
    .collect()
    .mkString(" ")
    .replaceAll("[\\[\\]]","")


  def readSourceData (inputFile :String) : DataFrame = {
    val sourceData = reader.csv(inputFile)
    sourceData.cache()

  }


  //  close spark application
//  close

  elapsedTime

  //    mailer.send(envelope)

}

