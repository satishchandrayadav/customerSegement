package com.mycompany.utils

object onJobSuccessNotification {
  def utilToSendJobSuccessNotification(programName :String) : Unit = {

       val successMessage :String =
      s"""
         #Hi All,
         #
         #The job $programName completed successfully. Please refer to the attached error log.
         #
         #
         #Regards,
         #Manaaged Serivces""".trim
    sendMail.utilTosendMail( s"$successMessage",s"Job Success -- $programName")

  }


}
