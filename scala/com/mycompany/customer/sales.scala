package com.mycompany

import java.io.File

import com.mycompany.utils.InitSpark
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._


class sales extends InitSpark {

  /** **********************Get table path from Config file start *************************/


  val saleSourceDataPath = spark.read.option("multiline", true)
    .json("/customerSegment/src/main/scala/com/mycompany/config/customerConfig.json")
    .select(s"${deployment_environment}.tables.sales_detail.table_location")
    .rdd
    .collect()
    .mkString(" ")
    .replaceAll("[\\[\\]]", "")


  val salesSavePath = spark.read.option("multiline", true)
    .json("/customerSegment/src/main/scala/com/mycompany/config/customerConfig.json")
    .select(s"${deployment_environment}.tables.sales_fact.table_location")
    .rdd
    .collect()
    .mkString(" ")
    .replaceAll("[\\[\\]]", "")

  val productDimPath = spark.read.option("multiline", true)
    .json("/customerSegment/src/main/scala/com/mycompany/config/customerConfig.json")
    .select(s"${deployment_environment}.tables.product_dim.table_location")
    .rdd
    .collect()
    .mkString(" ")
    .replaceAll("[\\[\\]]", "")

  val customerDimPath = spark.read.option("multiline", true)
    .json("/customerSegment/src/main/scala/com/mycompany/config/customerConfig.json")
    .select(s"${deployment_environment}.tables.customer_dim.table_location")
    .rdd
    .collect()
    .mkString(" ")
    .replaceAll("[\\[\\]]", "")

  val transactionThresholdPath = spark.read.option("multiline", true)
    .json("/customerSegment/src/main/scala/com/mycompany/config/customerConfig.json")
    .select(s"${deployment_environment}.tables.transaction_threshold.table_location")
    .rdd
    .collect()
    .mkString(" ")
    .replaceAll("[\\[\\]]", "")

  val monthDimPath = spark.read.option("multiline", true)
    .json("/customerSegment/src/main/scala/com/mycompany/config/customerConfig.json")
    .select(s"${deployment_environment}.tables.month_dim.table_location")
    .rdd
    .collect()
    .mkString(" ")
    .replaceAll("[\\[\\]]", "")

  val aggMonthlySalesPath = spark.read.option("multiline", true)
    .json("/customerSegment/src/main/scala/com/mycompany/config/customerConfig.json")
    .select(s"${deployment_environment}.tables.agg_monthly_sales.table_location")
    .rdd
    .collect()
    .mkString(" ")
    .replaceAll("[\\[\\]]", "")

  /** **********************Get table path from Config file end *************************/

  /*************************Check file existence Start***************************************/
  var filx = "star"
  val files = List(productDimPath,transactionThresholdPath,customerDimPath)
  var numfiles =  for ( i <- files)
    if (new File(i).exists()) {
      println(s"$i exist")
    }
    else  { var num = i
      println(s"$i file does not exist")
      var filx = num
      /*print(s"$numfiles")*/
    }

  print(s"NumFiles Name $filx")
  /*if ( numfiles != null) {
    /*System.exit(1)*/

  }*/
  /*************************Check file existence End ***************************************/


  /** **************************load tables start ************************************************/

  val sourceData = reader.csv(saleSourceDataPath)
  val productDim = reader.csv(productDimPath)
  val customerDim = reader.csv(customerDimPath)
  val monthDim = reader.csv(monthDimPath)
  val transactionThreshold = reader.csv(transactionThresholdPath)


  /** **************************load tables end ************************************************/

  def monthlyAggregateSalesByCustomer(): DataFrame = {

    val aggSalesData = sourceData.select("DT", "CUST_ID", "PROD_ID", "PROD_TYPE", "REVENUE")
      .groupBy("DT", "CUST_ID", "PROD_ID").agg(sum("REVENUE").alias("REVENUE"))


    val aggSalesDataByPeriod = aggSalesData.join(monthDim,
      aggSalesData.col("DT") === monthDim.col("Date")).
      select("DT", "CUST_ID", "PROD_ID", "REVENUE", "MONTH_ID").groupBy("CUST_ID", "PROD_ID", "MONTH_ID").
      agg(sum("REVENUE").alias("REVENUE")).select("CUST_ID", "PROD_ID", "REVENUE", "MONTH_ID")



    val aggSalesDataByProductGroup = aggSalesDataByPeriod.join(productDim, "PROD_ID").select("CUST_ID",
      "PROD_GROUP", "REVENUE", "MONTH_ID")

    val aggSalesDataByCustomer = aggSalesDataByProductGroup.join(customerDim, "CUST_ID").select("CUST_ID",
      "PROD_GROUP", "CUST_GROUP", "CUST_SEGMENT", "REVENUE", "MONTH_ID")

/*   val outputData = aggSalesDataByCustomer.coalesce(1).write.mode("overwrite").option("header", "true")
      .csv(salesSavePath)*/

    return aggSalesDataByCustomer

  }

  def calculateCustomerSegmentBasedOnTransVolume(): DataFrame = {
    val methodName = Thread.currentThread().getStackTrace()(1).getMethodName()
    println("executing methodName = " + methodName)
    val aggMonthlySales = reader.csv(aggMonthlySalesPath)

    transactionThreshold.createOrReplaceTempView("transThres")
    aggMonthlySales.createOrReplaceTempView("aggSalesByCust")

    val df  = sqlContext.sql(
      """select a.*,
                              case when a.REVENUE between b.THRESHOLD_RANGE1 and b.THRESHOLD_RANGE2
                              then b.CUSTOMER_TRANS_TYPE else "VHTV" end as CUSTOMER_TRANSACTION_SEGMENT
                                from aggSalesByCust a
                                left join transThres b
                                on
                                a.CUST_SEGMENT = b.CUSTOMER_SEGMENT
                                and
                              a.REVENUE between b.THRESHOLD_RANGE1 and b.THRESHOLD_RANGE2""")

  return df
  }

}
