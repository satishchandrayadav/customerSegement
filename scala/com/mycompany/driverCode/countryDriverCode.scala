package com.mycompany.drivercode


import com.mycompany.country
import com.mycompany.utils.writeToCsvWdHeader

object countryDriverProgram {
  def main(args: Array[String]) = {
    val countryObj = new country
    val w = writeToCsvWdHeader.writeToCSV(countryObj.sourceData ,countryObj.savePath)
  }
}