package com.mycompany.utils

import org.apache.spark.sql.DataFrame


object writeToCSVwdHeader {
  def writeToCSV (sourceData : DataFrame ,savePath: String) {
    val outputData = sourceData.coalesce(1).write.mode("overwrite").option("header", "true")
      .csv(savePath)

  }
}

