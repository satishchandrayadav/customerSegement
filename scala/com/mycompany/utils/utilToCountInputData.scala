package com.mycompany.utils

import scala.sys.process.Process

object utilToCountInputData {
  def getInputDataCount (inputDataPath : String) : String =  {
    val inputPath : String = Process(s"wc -l ${inputDataPath}").!!
    val inputDataCount : String = inputPath.split(" ")(0)
    return inputDataCount
  }
}
