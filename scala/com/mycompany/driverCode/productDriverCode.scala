package com.mycompany.drivercode

import com.mycompany.product
import com.mycompany.utils.writeToCSVwdHeader

object productDriverProgram {
  def main(args: Array[String]) = {
    val productObj = new product
    val writeCustomerDim = writeToCSVwdHeader.writeToCSV(productObj.productSourceData ,productObj.productSavePath)


  }
}