package com.mycompany.drivercode

import com.mycompany.utils._
import com.mycompany.country


import scala.util.{Failure, Success, Try}


object countryDriverProgram {

  def main(args: Array[String]): Unit = {

    val programName: String = new Exception().getStackTrace.head.getFileName

    print(s"${programName}")
    val countryObj = new country

    val loadDate = countryObj.loadDate

    val countrySourceData = countryObj.readSourceData(countryObj.countrySourceDataPath)

    val inputDataCount = countrySourceData.count()

    var countryObjStatus =

      Try {


        val fileName = countryObj.countrySourceDataPath

        val maxLoadDate = utilToCheckDuplicateDataLoad.getMaxLoadDate(programName, countryObj.jobMetricsWritePath,
          countryObj.loadDate)

        val inputData = s"${countryObj.countrySourceDataPath}"
        println(s"Name of the input file: ${inputData}")

        val countrySourceData = countryObj.readSourceData(countryObj.countrySourceDataPath)

        val inputDataCount = countrySourceData.count()
        println(s"Input file count : ${inputDataCount}")


        val writeCountryDim = utilToWriteToCSVwdHeader.writeToCSV(countrySourceData, countryObj.countryDimSavePath)

        val writeCount = countrySourceData.count()

        println(s"sat:${writeCount}")

        val status = "S"

        jobStatistics.getJobStatistics(programName: String, inputDataCount: Long, writeCount:
          Long, status: String,
          loadDate: String,
          countryObj.jobMetricsWritePath: String)
      }

    countryObjStatus match {
      case Failure(thrown) => {
        Console.println("Failure: " + thrown)
        onJobFailedNotification.utilToSendJobFailedNotification(programName: String)
        val status = "E"

        val insertCount = 0

        jobStatistics.getJobStatistics(programName: String, inputDataCount: Long, insertCount:
          Long, status: String,
          loadDate: String,
          countryObj.jobMetricsWritePath: String)

      }

      case Success(s) => {
        Console.println(s)

        onJobSuccessNotification.utilToSendJobSuccessNotification(programName)

      }
    }

  }

}

