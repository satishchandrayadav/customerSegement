package com.mycompany.drivercode


import com.mycompany.country
import com.mycompany.utils.writeToCSVwdHeader

object countryDriverProgram {
  def main(args: Array[String]) = {
    val countryObj = new country
    val writeCountryDim = writeToCSVwdHeader.writeToCSV(countryObj.countrySourceData ,countryObj.countryDimSavePath)
  }
}