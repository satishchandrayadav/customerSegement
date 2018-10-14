package com.mycompany.drivercode

import com.mycompany.product
import com.mycompany.utils.{onJobFailedNotification, onJobSuccessNotification, utilToCountInputData, utilToWriteToCSVwdHeader}
import scala.util.{Failure, Success, Try}


object productDriverProgram {
  def main(args: Array[String]) = {
    val productObj = new product
    val fileName = productObj.productSourceDataPath
    val programName = new Exception().getStackTrace.head.getFileName

    val productObjStatus = Try {
      val inputData = s"${productObj.productSourceDataPath}"
      println(s"Name of the insert file: ${inputData}")



      val productSourceData = productObj.readSourceData(productObj.productSourceDataPath)

      val inputDataCount = productSourceData.count()

      val writeProductDim = utilToWriteToCSVwdHeader.writeToCSV(productSourceData ,productObj.productSavePath)

      val writeCount = productSourceData.count()


      val status: String = "S"

      productObj.getJobMetrics(programName: String, inputDataCount: Long, writeCount: Long, status: String,
        productObj.loadDate: String,
        productObj.jobMetricsWritePath: String)
  }

    productObjStatus match {
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


