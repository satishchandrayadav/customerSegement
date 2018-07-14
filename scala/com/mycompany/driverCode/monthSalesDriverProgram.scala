package com.mycompany.drivercode
import com.mycompany.sales
import com.mycompany.utils.utilToWriteToCSVwdHeader
import org.apache.spark.sql.DataFrame


object monthSalesDriverProgram  {

  def main(args: Array[String]) = {
    val salesObj = new sales
    val salesAgg = salesObj.monthlyAggregateSalesByCustomer()
    val writemonthlyAggregateSalesByCustomer = utilToWriteToCSVwdHeader.writeToCSV(salesAgg :DataFrame ,salesObj.aggMonthlySalesPath)
    val customerSegmentbasedonTransac :DataFrame = salesObj.calculateCustomerSegmentBasedOnTransVolume()
    val writecustomerSegment = utilToWriteToCSVwdHeader.writeToCSV(customerSegmentbasedonTransac :DataFrame ,salesObj.salesSavePath)

  }

}

