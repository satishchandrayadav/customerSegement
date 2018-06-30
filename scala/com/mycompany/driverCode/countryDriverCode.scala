package com.mycompany.drivercode


import com.mycompany.country
import com.mycompany.utils.writeToCSVwdHeader

object countryDriverProgram {
  def main(args: Array[String]) = {
    val countryObj = new country
    val w = writeToCSVwdHeader.writeToCSV(countryObj.sourceData ,countryObj.savePath)
  }
}