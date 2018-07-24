package com.mycompany.drivercode

import java.io.{FileNotFoundException, IOException}

import com.mycompany.customer.customer
import com.mycompany.utils.{utilToCountInputData, utilToWriteToCSVwdHeader}
import org.apache.spark.sql.AnalysisException

import scala.util.control.NonFatal

object customerDriveProgram {
  def main(args: Array[String]) = {

    val customerObj = new customer
    val fileName = customerObj.customerSourceDataPath
    val programName = new Exception().getStackTrace.head.getFileName

    try {
      val inputData = s"${customerObj.customerSourceDataPath}"
      println(s"Name of the insert file: ${inputData}")

      val inputDataCount = utilToCountInputData.getInputDataCount(inputData)

      val customerSourceData = customerObj.readSourceData(customerObj.customerSourceDataPath)


      val writeCustomerDim = utilToWriteToCSVwdHeader.writeToCSV(customerSourceData ,customerObj.customerSavePath)
      val writeCount = customerSourceData.count()


      val status: String = "S"

      customerObj.getJobMetrics(programName: String, inputDataCount: String, writeCount: Long, status: String,
        customerObj.loadDate: String,
        customerObj.jobMetricsWritePath: String)
    }
    catch {

      case e: IOException => e.printStackTrace
      case e: IllegalArgumentException => println("illegal arg. exception");
      case e: AnalysisException => println(s"IO exception file not found ${fileName} ")

      case e: FileNotFoundException => println("Couldn't find that file.")
      case NonFatal(_) => println("Ooops. Much better, only the non fatal exceptions end up here.")

    }
    finally {
      println("Running finally block")

    }

  }
}
