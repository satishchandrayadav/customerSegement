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


  val customerSourceData: DataFrame = reader.csv(customerSourceDataPath)

  //  close spark application
//  close

  elapsedTime

  //    mailer.send(envelope)

}

