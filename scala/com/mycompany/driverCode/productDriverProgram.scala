package com.mycompany.drivercode

import java.io.{FileNotFoundException, IOException}

import com.mycompany.product
import com.mycompany.utils.{utilToCountInputData, utilToWriteToCSVwdHeader}
import org.apache.spark.sql.AnalysisException

import scala.util.control.NonFatal

object productDriverProgram {
  def main(args: Array[String]) = {
    val productObj = new product
    val fileName = productObj.productSourceDataPath
    val programName = new Exception().getStackTrace.head.getFileName

    try {
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
    catch {

      case e: IOException => e.printStackTrace
      case e: IllegalArgumentException => println("illegal arg. exception");
      case e: AnalysisException => println(s"IO exception file not found ${fileName} ")

      /*case e: FileNotFoundException => println("Couldn't find that file.")*/
      case NonFatal(_) => println("Ooops. Much better, only the non fatal exceptions end up here.")

    }
    finally {
      println("Running finally block")

    }

  }
}
