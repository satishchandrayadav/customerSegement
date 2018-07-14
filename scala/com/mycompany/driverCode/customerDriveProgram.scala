package com.mycompany.drivercode

import com.mycompany.customer.customer
import com.mycompany.utils.utilToWriteToCSVwdHeader

object customerDriveProgram {
  def main(args: Array[String]) = {
    val customerObj = new customer
    val writeCustomerDim = utilToWriteToCSVwdHeader.writeToCSV(customerObj.customerSourceData ,customerObj.customerSavePath)
  }
}