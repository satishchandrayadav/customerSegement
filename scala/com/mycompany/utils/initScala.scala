package com.mycompany.utils


import org.apache.log4j.{Level, LogManager, Logger}
import org.apache.spark.scheduler.{SparkListenerStageCompleted, _}
import org.apache.spark.sql.SparkSession

/* date */
import java.text.SimpleDateFormat
import java.util.{Calendar, Date}


trait InitSpark  {
  val spark: SparkSession = SparkSession.builder()
    .appName("Spark example")
    .master("local[*]")
    .config("option", "some-value")
    /*Since Hive is not installed, this property is not required. */
    /*.enableHiveSupport() /*to enable support for Hive */ */
    .getOrCreate()


  val sc = spark.sparkContext

  val sqlContext = spark.sqlContext

  var startDate = Calendar.getInstance().getTime

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
      val insertCount = s"${recordsWrittenCount}"
      println(s"recordcount : ${recordsWrittenCount}")
    }


    override def onJobEnd(jobEnd: SparkListenerJobEnd): Unit = {
      println(s"[ ${jobEnd.jobId} ] Job completed with Result : ${jobEnd.jobResult}")
    }

    override def  onStageSubmitted(SparkListenerStageSubmitted : SparkListenerStageSubmitted): Unit = {
      println(s"[Stage Name : ${SparkListenerStageSubmitted.stageInfo}] Stage submitted : ${SparkListenerStageSubmitted.stageInfo}")
    }
    override def onStageCompleted(SparkListenerStageCompleted: SparkListenerStageCompleted): Unit = {
      println(s"[ ${SparkListenerStageCompleted.stageInfo} ] Job completed with Result : ${SparkListenerStageCompleted.stageInfo}")
    }

  });

  val deployment_environment = "sales_dev"


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
  def close = {
    spark.stop()
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
    println(s"Time elapsed : ${diffDays} : ${diffHours} : ${diffMinutes} : ${diffSeconds}")
  }
  //  class ExampleClass extends EnvelopeWrappers {
  //    val config = SmtpConfiguration("localhost", 25)
  //    val mailer = Mailer(config)
  //    val content = Multipart(
  //      parts = Seq(Text("text"), Html("<p>text</p>")),
  //      subType = MultipartTypes.alternative
  //    )
  //
  //    val envelope = Envelope(
  //      from = "from@localhost.com",
  //      to = Seq("chandrasatish2009@gmail.com"),
  //      subject = "test",
  //      content = content
  //    )
  //
  //    mailer.send(envelope)
  //  }

}