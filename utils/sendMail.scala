package com.mycompany.utils

import java.util.Properties

import javax.mail.{Message, Session}
import javax.mail.internet.{InternetAddress, MimeMessage}

import scala.io.Source
object sendMail {


  def utilTosendMail (text:String, subject:String) = {

    val host = "smtp.gmail.com"
    val port = "587"

    val address = "apachesparkforcoder@gmail.com"
    val username = "apachesparkforcoder@gmail.com"

     val password = Source.fromFile(System.getProperty("user.home")
        + "/authFile").getLines.mkString


    val properties = new Properties()
    properties.put("mail.smtp.port", port)
    properties.put("mail.smtp.auth", "true")
    properties.put("mail.smtp.starttls.enable", "true")

    val session = Session.getDefaultInstance(properties, null)
    val message = new MimeMessage(session)
    val logFilename = "/usr/local/spark/RELEASE1.txt"
    message.addRecipient(Message.RecipientType.TO, new InternetAddress(address));
    message.setSubject(subject)
    message.setFileName(logFilename)
    message.setContent(text, "text/html")

    val transport = session.getTransport("smtp")
    transport.connect(host, username, password)
    transport.sendMessage(message, message.getAllRecipients)
  }
}
