package com.mycompany.utils

import scala.sys.process.Process

object utilToCountInputData {
  def getInputDataCount (inputDataPath : String) : String =  {

    try {val inputPath : String = Process(s"wc -l ${inputDataPath}").!!
    val inputDataCount : String = inputPath.split(" ")(0)
    return inputDataCount
  }
  catch {case e: java.lang.RuntimeException => ("Couldn't find that file 0")}
}}
