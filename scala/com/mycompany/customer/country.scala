package com.mycompany

import java.io.File
import org.apache.spark.SparkContext._

import com.mycompany.utils.InitSpark

class country extends InitSpark {
  val sourceDataPath = spark.read.option("multiline",true)
    .json("/customerSegment/src/main/scala/com/mycompany/config/customerConfig.json")
    .select(s"${deployment_environment}.tables.country_detail.table_location")
    .rdd
    .collect()
    .mkString(" ")
    .replaceAll("[\\[\\]]","")

  val savePath = spark.read.option("multiline",true)
    .json("/customerSegment/src/main/scala/com/mycompany/config/customerConfig.json")
    .select(s"${deployment_environment}.tables.country_dim.table_location")
    .rdd
    .collect()
    .mkString(" ")
    .replaceAll("[\\[\\]]","")

  val files = List(sourceDataPath)
  for ( i <- files)
    if (new File(i).exists()) {
      println(s"$i exist")
    } else {
      println(s"$i file does not exist")
      System.exit(1)
    }





  println(s"input table path : $sourceDataPath")
  println(s"output table path : $savePath")


  val sourceData = reader.csv(sourceDataPath)

  val outputData = sourceData.write.mode("overwrite").option("header", "true")
    .csv(savePath)
    //    close



  elapsedTime

  //    mailer.send(envelope)
}

