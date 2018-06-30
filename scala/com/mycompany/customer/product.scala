package com.mycompany

import com.mycompany.utils.InitSpark

class product extends InitSpark {

  val productSourceDataPath = spark.read.option("multiline",true)
    .json("/customerSegment/src/main/scala/com/mycompany/config/customerConfig.json")
    .select(s"${deployment_environment}.tables.product_detail.table_location")
    .rdd
    .collect()
    .mkString(" ")
    .replaceAll("[\\[\\]]","")

  val productSavePath = spark.read.option("multiline",true)
    .json("/customerSegment/src/main/scala/com/mycompany/config/customerConfig.json")
    .select(s"${deployment_environment}.tables.product_dim.table_location")
    .rdd
    .collect()
    .mkString(" ")
    .replaceAll("[\\[\\]]","")


  println(s"input table path : $productSourceDataPath")
  println(s"output table path : $productSavePath")


  val productSourceData = reader.csv(productSourceDataPath)

}


/*
object product  {
  def main(args: Array[String]) = {
    val product = new product
  }
}*/
