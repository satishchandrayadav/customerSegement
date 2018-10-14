package com.mycompany.utils
import org.apache.log4j.{Level, LogManager, Logger}
import org.apache.spark.scheduler.{SparkListenerStageCompleted, _}
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import java.util.{Calendar, Date, Properties}

import org.apache.hadoop.yarn.util.RackResolver
import org.apache.log4j.{Level, Logger}
import org.apache.log4j.PropertyConfigurator

trait InitSpark  {
  val spark: SparkSession = SparkSession.builder()
    .appName("Customer Segment")
    .master("local[*]")
    .config("log4j.configuration", "file:///root/spark-2.3.0-bin-hadoop2.7/conf/log4j.propertiessome-value")
    /*Since Hive is not installed, this property is not required. */
    /*.enableHiveSupport() /*to enable support for Hive */ */
    .getOrCreate()

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
  spark.conf.set("spark.kryoserializer.buffer.mb","24")

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

  var startDate : Date = Calendar.getInstance().getTime

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

 /*   override def  onStageSubmitted(SparkListenerStageSubmitted : SparkListenerStageSubmitted): Unit = {
      println(s"[Stage Name : ${SparkListenerStageSubmitted.stageInfo}] Stage submitted : ${SparkListenerStageSubmitted.stageInfo}")
    }
    override def onStageCompleted(SparkListenerStageCompleted: SparkListenerStageCompleted): Unit = {
      println(s"[ ${SparkListenerStageCompleted.stageInfo} ] Job completed with Result : ${SparkListenerStageCompleted.stageInfo}")
    }
*/
  });

  val deployment_environment = "sales_dev"

  import sqlContext.implicits._

 /* val loadDate = sc.parallelize(Seq("08-26-2016"))*/
  val loadDate = "08-26-2016"

  /* loadDate.createOrReplaceTempView("table1")
   val loadDate1 :DataFrame = spark.sql("""select from_unixtime(unix_timestamp(Id, 'MM-dd-yyyy')) as new_format from table1""")
   loadDate1.show()*/


  println(s"loading date : ${loadDate}")

  /*var loadDate = new Date(2018,12,1)*/


  def reader = spark.read
    .option("header",true)
    .option("inferSchema", true)
    .option("mode", "DROPMALFORMED")

  def readerWithoutHeader = spark.read
    .option("header",true)
    .option("inferSchema", true)
    .option("mode", "DROPMALFORMED")

  private def init = {
    sc.setLogLevel("ERROR")
    Logger.getLogger("org").setLevel(Level.ERROR)
    Logger.getLogger("akka").setLevel(Level.ERROR)
    LogManager.getRootLogger.setLevel(Level.ERROR)
  }
  init
  def closeSpark = {
    spark.stop()
  }

  def getJobMetrics (programName: String, inputCount : Long, insertCount : Long ,status :String ,loadDate : String ,
                     jobMetricsWritePath: String ) {
    val createDataForDF = Seq(
      Row(programName, inputCount,insertCount,status,loadDate)
    )
    val TableSchema = List(
      StructField("programName", StringType, true),
      StructField("inputCount", LongType, true),
      StructField("insertCount", LongType, true),
      StructField("status", StringType, true),
      StructField("loadDate", StringType, true)
          )

    val JobMetricsDF = spark.createDataFrame(
      spark.sparkContext.parallelize(createDataForDF),
      StructType(TableSchema)
    )
    val outputData = JobMetricsDF.coalesce(1).write.mode("append").option("header", "true")
      .csv(jobMetricsWritePath)


      }

  def readSourceData (inputFile :String) : DataFrame = {
    val sourceData = reader.csv(inputFile)
    sourceData.cache()

  }


  def elapsedTime = {
    var endDate = Calendar.getInstance().getTime
    var diff = endDate.getTime() - startDate.getTime()
    var diffSeconds = diff / 1000 % 60
    var diffMinutes = diff / (60 * 1000) % 60
    var diffHours = diff / (60 * 60 * 1000) % 24
    var diffDays = diff / (24 * 60 * 60 * 1000)
    /* var startDateFormat = new SimpleDateFormat("yyyy-MM-dd:hh:mm:ss").format(difference) (used for
    * formating the date*/
    import org.apache.spark.sql.catalyst.expressions.CurrentDate


    println(s"Time elapsed : ${diffDays} : ${diffHours} : ${diffMinutes} : ${diffSeconds}")
  }


}