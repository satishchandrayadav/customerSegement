package com.mycompany.customer

import com.mycompany.utils.InitSpark
import org.apache.spark.sql.DataFrame

class customer extends InitSpark {

  val sourceDataPath: String = spark.read.option("multiline", value = true)
    .json("/customerSegment/src/main/scala/com/mycompany/config/customerConfig.json")
    .select(s"${deployment_environment}.tables.customer_detail.table_location")
    .rdd
    .collect()
    .mkString(" ")
    .replaceAll("[\\[\\]]", "")


  val savePath: String = spark.read.option("multiline", value = true)
    .json("/customerSegment/src/main/scala/com/mycompany/config/customerConfig.json")
    .select(s"${deployment_environment}.tables.customer_dim.table_location")
    .rdd
    .collect()
    .mkString(" ")
    .replaceAll("[\\[\\]]", "")


  println(s"input table path : $sourceDataPath")
  println(s"input table path : $savePath")


  val sourceData: DataFrame = reader.csv(sourceDataPath)
  val outputData: Unit = sourceData.write.mode("overwrite").option("header", "true")
    .csv(savePath)

  //    sourceData.show(2)

  //  close spark application
  close


  elapsedTime

  //    mailer.send(envelope)

}


//uncomment the below code to run customer module independently

/*
object customer  {
 def main(args: Array[String]) = {
   val customer = new customer
 }
}*/
