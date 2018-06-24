package com.mycompany

import com.mycompany.utils.InitSpark

class product extends InitSpark {

  val sourceDataPath = spark.read.option("multiline",true)
    .json("/customerSegment/src/main/scala/com/mycompany/config/customerConfig.json")
    .select(s"${deployment_environment}.tables.product_detail.table_location")
    .rdd
    .collect()
    .mkString(" ")
    .replaceAll("[\\[\\]]","")

  val savePath = spark.read.option("multiline",true)
    .json("/customerSegment/src/main/scala/com/mycompany/config/customerConfig.json")
    .select(s"${deployment_environment}.tables.product_dim.table_location")
    .rdd
    .collect()
    .mkString(" ")
    .replaceAll("[\\[\\]]","")


  println(s"input table path : $sourceDataPath")
  println(s"output table path : $savePath")


  val sourceData = reader.csv(sourceDataPath)
  val outputData = sourceData.write.mode("overwrite").option("header", "true")
    .csv(savePath)

}


/*
object product  {
  def main(args: Array[String]) = {
    val product = new product
  }
}*/
