package com.mycompany.drivercode


import com.mycompany.country
import com.mycompany.utils.writeToCSVwdHeader
import sys.process._
import scala.sys.process._

object countryDriverProgram {
  def main(args: Array[String]) = {
    val countryObj = new country
    val writeCountryDim = writeToCSVwdHeader.writeToCSV(countryObj.countrySourceData ,countryObj.countryDimSavePath)
    val inputPath = s"${countryObj.countrySourceDataPath}"
    println(s"Name of the insert file: ${inputPath}")
    val programName = new Exception().getStackTrace.head.getFileName
    val inputCount :String  = s"wc -l ${inputPath}" #| s"cut -d \\  -f1"!!
    val inputCount2 : String = s"cut '-d ' -f1 ${inputCount}"!!

    println(s"Name of the inputCount: ${inputCount}")
          /*println(s"Name of the sharedvariable : ${countryObj.insertCount}")*/
   /* println(s"Name of the file : ${programName}")*/
  }
}