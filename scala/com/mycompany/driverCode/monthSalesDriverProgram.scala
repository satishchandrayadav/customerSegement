package com.mycompany.drivercode
import java.io.{FileNotFoundException, IOException}

import com.mycompany.sales
import com.mycompany.utils.{utilToCountInputData, utilToWriteToCSVwdHeader}
import org.apache.spark.sql.{AnalysisException, DataFrame}

import scala.util.control.NonFatal


object monthSalesDriverProgram  {

  def main(args: Array[String]) = {

    val salesObj = new sales
    val fileName = salesObj.saleSourceDataPath
    val programName = new Exception().getStackTrace.head.getFileName

    try {
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
    catch {

      case e: IOException => e.printStackTrace
      case e: IllegalArgumentException => println("illegal arg. exception");
      case e: AnalysisException => println(s"IO exception file not found ${fileName} ")

      /*case e: FileNotFoundException => println("Couldn't find that file.")*/
      case NonFatal(_) => println("Ooops. Much better, only the non fatal exceptions end up here.")

    }
    finally {
      println("Running finally block")
      salesObj.elapsedTime
      salesObj.closeSpark
    }

  }
}

