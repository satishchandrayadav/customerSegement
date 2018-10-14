package com.mycompany.drivercode

import com.mycompany.customer.customer
import com.mycompany.utils.{onJobFailedNotification, onJobSuccessNotification, utilToWriteToCSVwdHeader}
import scala.util.{Failure, Success, Try}

object customerDriverProgram {
  def main(args: Array[String]) = {

    val customerObj = new customer
    val fileName = customerObj.customerSourceDataPath
    val programName = new Exception().getStackTrace.head.getFileName

    val customerObjStatus = Try {
      val inputData = s"${customerObj.customerSourceDataPath}"
      println(s"Name of the insert file: ${inputData}")


      val customerSourceData = customerObj.readSourceData(customerObj.customerSourceDataPath)

      val inputDataCount = customerSourceData.count()
      println(s"Input file count : ${inputDataCount}")



      val writeCustomerDim = utilToWriteToCSVwdHeader.writeToCSV(customerSourceData ,customerObj.customerSavePath)
      val writeCount = customerSourceData.count()


      val status: String = "S"

      customerObj.getJobMetrics(programName: String, inputDataCount: Long, writeCount: Long, status: String,
        customerObj.loadDate: String,
        customerObj.jobMetricsWritePath: String)
    }
    customerObjStatus match {
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