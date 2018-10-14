package com.mycompany.utils

object onJobFailedNotification {
  def utilToSendJobFailedNotification(programName :String) : Unit = {

    val failMessage :String =
      s"""
        #Hi
        #The job $programName failed. Please refer to the attached error log.
        """
        .stripMargin('#')
    sendMail.utilTosendMail( s"$failMessage",
      s"Job failed -- $programName")
          }

}
