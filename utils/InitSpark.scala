package com.mycompany.utils

import org.apache.log4j.LogManager
import org.apache.spark.scheduler.{SparkListenerStageCompleted, _}
import org.apache.spark.sql.{DataFrame, SparkSession}
import java.sql.Timestamp

import org.apache.hadoop.yarn.util.RackResolver
import org.apache.log4j.{Level, Logger}


trait InitSpark {
  val spark: SparkSession = SparkSession.builder()
    .appName("Customer Segment")
    .master("local[*]")
    .config("log4j.configuration", "file:///root/spark-2.3.0-bin-hadoop2.7/conf/log4j.propertiessome-value")
    /*Since Hive is not installed, this property is not required. */
    /*.enableHiveSupport() /*to enable support for Hive */ */
    .getOrCreate()


  val startDate: Timestamp = new Timestamp(System.currentTimeMillis)

  println(s"satishyadav:$startDate")

  /*
  PropertyConfigurator.configure("/root/spark-2.3.0-bin-hadoop2.7/conf/log4j.properties")
*/

  spark.conf.set("spark.executor.memory", "2g")


  /*But the used compression codec can make a difference. Most codecs somehow represent a trade off between
  the memory they use, the time they need and the size of the compressed data. So far I had good experiences with "lz4".
*/

  spark.conf.set("spark.io.compression.codec", "lz4")
  spark.conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
  spark.conf.set("spark.kryo.registrationRequired", "true")
  spark.conf.set("spark.kryoserializer.buffer.mb", "24")

  /*
  spark.conf.set("log4j.configuration", "/root/spark-2.3.0-bin-hadoop2.7/conf")

*/


  val sc = spark.sparkContext

  /* setting log configuration*/
  sc.setLogLevel("OFF")
  Logger.getLogger(classOf[RackResolver]).getLevel
  Logger.getLogger("org").setLevel(Level.OFF)


  /* Add hadoop configuration*/

  val hadoopConf = sc.hadoopConfiguration
  hadoopConf.set("fs.s3.impl", "org.apache.hadoop.fs.s3native.NativeS3FileSystem")


  val sqlContext = spark.sqlContext



  sc.addSparkListener(new SparkListener() {
    override def onApplicationStart(applicationStart: SparkListenerApplicationStart) {
      println("Spark ApplicationStart: " + applicationStart.time);
    }

    override def onApplicationEnd(applicationEnd: SparkListenerApplicationEnd) {
      println("Spark ApplicationEnd: " + applicationEnd.time)
      //println("Spark ApplicationEnd: " + FinalApplicationStatus.FAILED)
      println("program ends")
    }


    var recordsWrittenCount = sc.longAccumulator("recordsWrittenCount")


    override def onTaskEnd(taskEnd: SparkListenerTaskEnd) {
      var recordsWrittenCount = 0L
      synchronized {

        recordsWrittenCount += taskEnd.taskMetrics.outputMetrics.recordsWritten
      }

      println(s"recordcount : ${recordsWrittenCount}")

    }


    override def onJobEnd(jobEnd: SparkListenerJobEnd): Unit = {
      println(s"[ ${jobEnd.jobId} ] Job completed with Result : ${jobEnd.jobResult}")
    }

  });

  val deployment_environment = "sales_dev"

  val loadDate = "08-26-2016"

  println(s"loading date : ${loadDate}")


  def reader = spark.read
    .option("header", true)
    .option("inferSchema", true)
    .option("mode", "DROPMALFORMED")

  def readerWithoutHeader = spark.read
    .option("header", false)
    .option("inferSchema", false)
    .option("mode", "DROPMALFORMED")

  private def init = {
    sc.setLogLevel("ERROR")
    Logger.getLogger("org").setLevel(Level.ERROR)
    Logger.getLogger("akka").setLevel(Level.ERROR)
    LogManager.getRootLogger.setLevel(Level.ERROR)
  }


  def closeSpark = {
    spark.stop()
  }

  def readSourceData(inputFile: String): DataFrame = {
    val sourceData = reader.csv(inputFile)
    sourceData.cache()

  }


}