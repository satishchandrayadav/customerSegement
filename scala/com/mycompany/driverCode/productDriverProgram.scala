package com.mycompany.drivercode

import com.mycompany.product
import com.mycompany.utils.utilToWriteToCSVwdHeader

object productDriverProgram {
  def main(args: Array[String]) = {
    val productObj = new product
    val writeCustomerDim = utilToWriteToCSVwdHeader.writeToCSV(productObj.productSourceData ,productObj.productSavePath)


  }
}