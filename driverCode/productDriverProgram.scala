package com.mycompany.drivercode

import com.mycompany.product
import com.mycompany.utils._

import scala.util.{Failure, Success, Try}


object productDriverProgram {
  def main(args: Array[String]) = {
    val productObj = new product
    val loadDate = productObj.loadDate

    val fileName = productObj.productSourceDataPath
    val programName = new Exception().getStackTrace.head.getFileName

    val productSourceData = productObj.readSourceData(productObj.productSourceDataPath)

    val inputDataCount = productSourceData.count()

    val productObjStatus = Try {
      val inputData = s"${productObj.productSourceDataPath}"
      println(s"Name of the insert file: ${inputData}")

      val writeProductDim = utilToWriteToCSVwdHeader.writeToCSV(productSourceData, productObj.productSavePath)

      val writeCount = productSourceData.count()


      val status: String = "S"


      jobStatistics.getJobStatistics(programName: String, inputDataCount: Long, writeCount:
        Long, status: String,
        loadDate: String,
        productObj.jobMetricsWritePath: String)
    }

    productObjStatus match {
      case Failure(thrown) => {
        Console.println("Failure: " + thrown)
        onJobFailedNotification.utilToSendJobFailedNotification(programName: String)

        val status = "E"

        var insertCount = 0

        jobStatistics.getJobStatistics(programName: String, insertCount: Long, insertCount:
          Long, status: String,
          loadDate: String,
          productObj.jobMetricsWritePath: String)

      }

      case Success(s) => {
        Console.println(s)
        onJobSuccessNotification.utilToSendJobSuccessNotification(programName)

      }
    }

  }

}


