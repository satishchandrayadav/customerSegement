package com.mycompany

import java.io.File
import java.sql.Date
import java.text.SimpleDateFormat

import com.mycompany.utils.InitSpark

class country extends InitSpark {
  val countrySourceDataPath = spark.read.option("multiline",true)
    .json("/customerSegment/src/main/scala/com/mycompany/config/customerConfig.json")
    .select(s"${deployment_environment}.tables.country_detail.table_location")
    .rdd
    .collect()
    .mkString(" ")
    .replaceAll("[\\[\\]]","")


  val countryDimSavePath = spark.read.option("multiline",true)
    .json("/customerSegment/src/main/scala/com/mycompany/config/customerConfig.json")
    .select(s"${deployment_environment}.tables.country_dim.table_location")
    .rdd
    .collect()
    .mkString(" ")
    .replaceAll("[\\[\\]]","")

  val jobMetricsWritePath = spark.read.option("multiline",true)
    .json("/customerSegment/src/main/scala/com/mycompany/config/customerConfig.json")
    .select(s"${deployment_environment}.tables.job_metrics.table_location")
    .rdd
    .collect()
    .mkString(" ")
    .replaceAll("[\\[\\]]","")


  var loadDate = new Date(2018,12,1)


  val files = List(countrySourceDataPath)
  for ( i <- files)
    if (new File(i).exists()) {
      println(s"$i exist")
    } else {
      println(s"$i file does not exist")
      System.exit(1)
    }





  println(s"input table path : $countrySourceDataPath")
  println(s"output table path : $countryDimSavePath")


  val countrySourceData = reader.csv(countrySourceDataPath)
      countrySourceData.cache()


  //    close



  elapsedTime
  //    mailer.send(envelope)
}

