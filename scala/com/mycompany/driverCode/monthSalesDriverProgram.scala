package com.mycompany.drivercode
import com.mycompany.{sales}


object monthSalesDriverProgram  {

  def main(args: Array[String]) = {
    val salesObj = new sales
    val salesAgg = salesObj.monthlyAggregateSalesByCustomer()
    val customerSegmentbasedonTransac = salesObj.calculateCustomerSegmentBasedOnTransVolume()

  }

}

