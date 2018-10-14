package com.mycompany.drivercode
//

import com.mycompany.country
import com.mycompany.utils.{onJobFailedNotification, onJobSuccessNotification, utilToWriteToCSVwdHeader}
import scala.util.{Failure, Success, Try}

object countryDriverProgram {

  def main(args: Array[String]) = {

    val countryObj = new country

    val fileName = countryObj.countrySourceDataPath

    val programName :String = new Exception().getStackTrace.head.getFileName

    val countryObjStatus = Try {
      val inputData = s"${countryObj.countrySourceDataPath}"
      println(s"Name of the input file: ${inputData}")


      val countrySourceData = countryObj.readSourceData(countryObj.countrySourceDataPath)

      val inputDataCount = countrySourceData.count()
      println(s"Input file count : ${inputDataCount}")


      val writeCountryDim = utilToWriteToCSVwdHeader.writeToCSV(countrySourceData, countryObj.countryDimSavePath)


      val writeCount = countrySourceData.count()


      val status: String = "S"

      countryObj.getJobMetrics(programName: String, inputDataCount: Long, writeCount: Long, status: String,
        countryObj.loadDate: String,
        countryObj.jobMetricsWritePath: String)


    }

    countryObjStatus match {
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

