package com.mycompany.drivercode

import com.mycompany.sales
import com.mycompany.utils.{onJobFailedNotification, onJobSuccessNotification, utilToWriteToCSVwdHeader}
import org.apache.spark.sql.{DataFrame}

import scala.util.{Failure, Success, Try}



object monthSalesDriverProgram  {

  def main(args: Array[String]) = {

    val salesObj = new sales
    val fileName = salesObj.saleSourceDataPath
    val programName = new Exception().getStackTrace.head.getFileName

    val salesObjStatus = Try {
      val inputData = s"${salesObj.saleSourceDataPath}"
      println(s"Name of the input file: ${inputData}")


      val saleSourceDataPath = salesObj.readSourceData(salesObj.saleSourceDataPath)

      val inputDataCount = saleSourceDataPath.count()
      println(s"Input file count : ${inputDataCount}")

    val salesAgg = salesObj.monthlyAggregateSalesByCustomer()
    val writemonthlyAggregateSalesByCustomer = utilToWriteToCSVwdHeader.writeToCSV(salesAgg :DataFrame ,salesObj.aggMonthlySalesPath)

    val customerSegmentbasedonTransac :DataFrame = salesObj.calculateCustomerSegmentBasedOnTransVolume()
    val writecustomerSegment = utilToWriteToCSVwdHeader.writeToCSV(customerSegmentbasedonTransac :DataFrame ,salesObj.salesSavePath)

      val writeCount = customerSegmentbasedonTransac.count()


      val status: String = "S"

      salesObj.getJobMetrics(programName: String, inputDataCount: Long, writeCount: Long, status: String,
        salesObj.loadDate: String,
        salesObj.jobMetricsWritePath: String)
    }

    salesObjStatus match {
      case Failure(thrown) => {
        Console.println("Failure: " + thrown)
        onJobFailedNotification.utilToSendJobFailedNotification(programName:String)

      }

      case Success(s) => {
        Console.println(s)
        onJobSuccessNotification.utilToSendJobSuccessNotification(programName)

      }
    }

  }

}