package com.mycompany.drivercode

import com.mycompany.customer.customer
import com.mycompany.utils.writeToCSVwdHeader

object customerDriveProgram {
  def main(args: Array[String]) = {
    val customerObj = new customer
    val writeCustomerDim = writeToCSVwdHeader.writeToCSV(customerObj.customerSourceData ,customerObj.customerSavePath)
  }
}