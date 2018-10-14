package com.mycompany.drivercode

import java.io.{FileNotFoundException, IOException}
import java.sql.Timestamp
import java.text.SimpleDateFormat

import scala.util.control.NonFatal
import java.util.{Calendar, Date}

import com.mycompany.country
import com.mycompany.utils.utilToWriteToCSVwdHeader
import com.mycompany.utils.utilToCountInputData
import org.apache.spark.sql.{AnalysisException, DataFrame}
import org.apache.spark.sql.types.DateType
import org.joda.time.DateTimeFieldType

object countryDriverProgram {
  def main(args: Array[String]) = {

    val countryObj = new country

    val fileName = countryObj.countrySourceDataPath

    val programName = new Exception().getStackTrace.head.getFileName

    try {
      val inputData = s"${countryObj.countrySourceDataPath}"
      println(s"Name of the input file: ${inputData}")



      val countrySourceData = countryObj.readSourceData(countryObj.countrySourceDataPath)

      val inputDataCount = countrySourceData.count()
      println(s"Input file count : ${inputDataCount}")


      val writeCountryDim = utilToWriteToCSVwdHeader.writeToCSV(countrySourceData, countryObj.countryDimSavePath)

      val writeCount = countrySourceData.count()


      val status: String = "S"

      countryObj.getJobMetrics(programName: String, inputDataCount:Long , writeCount: Long, status: String,
        countryObj.loadDate: String,
        countryObj.jobMetricsWritePath: String)


    }
    catch {

      case e: IOException => e.printStackTrace
      case e: IllegalArgumentException => println("illegal arg. exception");
      case e: AnalysisException => println(s"IO exception file not found ${fileName} ")
      /*case e: FileNotFoundException => println("Couldn't find that file.")*/
      /*case NonFatal(_) => println("Ooops. Much better, only the non fatal exceptions end up here.")*/

    }
    finally {
      println("Running finally block")

      countryObj.elapsedTime
      countryObj.closeSpark


    }

  }
}
