package com.mycompany.drivercode

import java.sql.Timestamp

import com.mycompany.customer.customer
import com.mycompany.utils.{jobStatistics, onJobFailedNotification, onJobSuccessNotification, utilToWriteToCSVwdHeader}

import scala.util.{Failure, Success, Try}

object customerDriverProgram {
  def main(args: Array[String]) = {

    val customerObj = new customer

    val customerSourceData = customerObj.readSourceData(customerObj.customerSourceDataPath)

    val inputDataCount = customerSourceData.count()

    val loadDate = customerObj.loadDate

    val fileName = customerObj.customerSourceDataPath
    val programName = new Exception().getStackTrace.head.getFileName

    val customerObjStatus = Try {
      val inputData = s"${customerObj.customerSourceDataPath}"
      println(s"Name of the insert file: ${inputData}")


      println(s"Input file count : ${inputDataCount}")


      val writeCustomerDim = utilToWriteToCSVwdHeader.writeToCSV(customerSourceData, customerObj.customerSavePath)
      val writeCount = customerSourceData.count()

      val elapsedTime = jobStatistics.elapsedTime(customerObj.startDate :Timestamp)

      val status = "S"

      jobStatistics.getJobStatistics(programName: String, inputDataCount: Long, writeCount:
        Long, status: String,
        loadDate: String,
        customerObj.jobMetricsWritePath: String, customerObj.startDate :Timestamp, jobStatistics.endDate :Timestamp,
        elapsedTime: String)
    }
    customerObjStatus match {
      case Failure(thrown) => {
        Console.println("Failure: " + thrown)
        onJobFailedNotification.utilToSendJobFailedNotification(programName: String)

        val status = "E"

        val insertCount = 0
        val elapsedTime = jobStatistics.elapsedTime(customerObj.startDate :Timestamp)

        jobStatistics.getJobStatistics(programName: String, inputDataCount: Long, insertCount:
          Long, status: String,
          loadDate: String,
          customerObj.jobMetricsWritePath: String,  customerObj.startDate: Timestamp, jobStatistics.endDate :Timestamp,
          elapsedTime: String)
      }

      case Success(s) => {
        Console.println(s)
        onJobSuccessNotification.utilToSendJobSuccessNotification(programName)

      }
    }

  }

}
