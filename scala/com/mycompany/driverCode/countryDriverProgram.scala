package com.mycompany.drivercode

import scala.concurrent.Future
import scala.concurrent.ExecutionContext.Implicits.global
import scala.util.{Failure, Success}
import scala.util.Random

import java.sql.Date


import com.mycompany.country
import com.mycompany.utils.utilToWriteToCSVwdHeader
import com.mycompany.utils.utilToCountInputData

object countryDriverProgram {
  def main(args: Array[String]) = {

    val countryObj = new country

    val writeCountryDim = utilToWriteToCSVwdHeader.writeToCSV(countryObj.countrySourceData ,countryObj.countryDimSavePath)

    val writeCount = countryObj.countrySourceData.count()

    val inputData = s"${countryObj.countrySourceDataPath}"
        println(s"Name of the insert file: ${inputData}")

    val inputDataCount = utilToCountInputData.getInputDataCount(inputData)

    val programName = new Exception().getStackTrace.head.getFileName
        println(s"Count of records in input file: ${inputDataCount}")

    val status : String = "S"

        countryObj.getJobMetrics(programName: String, inputDataCount : String, writeCount : Long  ,status :String ,
          countryObj.loadDate :Date ,
      countryObj.jobMetricsWritePath: String)
    val status1 : String = "S"
  }
}


//status.onComplete {
//  case Success(value) => println(s"Got the callback, meaning = $value")
//  case Failure(e) => e.printStackTrace
//}