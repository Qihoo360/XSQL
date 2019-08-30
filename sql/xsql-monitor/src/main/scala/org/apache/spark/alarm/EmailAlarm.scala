/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.spark.alarm

import org.apache.commons.mail.{DefaultAuthenticator, EmailConstants, HtmlEmail}

import org.apache.spark.internal.Logging

class EmailAlarm extends Alarm with Logging {
  import EmailAlarm._
  override val name: String = "email"
  var haveSetOnce: Boolean = false
  val email = new HtmlEmail()
  EmailAlarm.email = Option(email)

  private lazy val hostName = options.getOrElse(HOSTNAME, "localhost")
  private lazy val useSSL = options.getOrElse(SSL_ON_CONNECT, "false").toBoolean
  private lazy val port = options.getOrElse(SMTP_PORT, "587").toInt
  private lazy val user = options.getOrElse(USERNAME, "user")
  private lazy val password = options.getOrElse(PASSWORD, "password")
  private lazy val from = options.getOrElse(FROM, "from@test.com")
  // scalastyle:off
  private lazy val shortname = options.getOrElse(NAME, "xsql_monitor")
  private lazy val toList =
    options.getOrElse(TO, "").split(",") ++ Seq("to@test.com")

  override def finalAlarm(msg: AlertMessage): AlertResp = {
    try {
      email.setHostName(hostName)
      email.setSmtpPort(port)
      email.setCharset(EmailConstants.UTF_8)
      if (options.getOrElse(AUTH, "false").toBoolean) {
        val authn = new DefaultAuthenticator(user, password)
        email.setAuthenticator(authn)
        email.setSSLOnConnect(useSSL)
      }
      email.setFrom(from, shortname)
      email.setSubject(msg.title.toString)
      email.setHtmlMsg(s"<html>${msg.toHtml()}</html>")
      //      email.setMsg(msg.content)
      email.addTo(toList: _*)
      logError(msg.toString)
      val ret = email.send()
      AlertResp.success(ret)
    } catch {
      case e: Exception =>
        logError(s"User $user failed to send email from $from to $toList", e)
        AlertResp.failure(e.getMessage)
    }
  }
  override def alarm(msg: AlertMessage): AlertResp = {
    try {
      val email = new HtmlEmail()
      email.setHostName(hostName)
      email.setSmtpPort(port)
      email.setCharset(EmailConstants.UTF_8)
      if (options.getOrElse(AUTH, "false").toBoolean) {
        val authn = new DefaultAuthenticator(user, password)
        email.setAuthenticator(authn)
        email.setSSLOnConnect(useSSL)
      }
      email.setFrom(from, shortname)
      email.setSubject(msg.title.toString)
      email.setHtmlMsg(s"<html>${msg.toHtml()}</html>")
//      email.setMsg(msg.content)
      email.addTo(toList: _*)
      logError(msg.toHtml())
      val ret = email.send()
      AlertResp.success(ret)
    } catch {
      case e: Exception =>
        logError(s"User $user failed to send email from $from to $toList", e)
        AlertResp.failure(e.getMessage)
    }
  }
}

object EmailAlarm {
  var email: Option[HtmlEmail] = Option.empty
  def get(): Option[HtmlEmail] = email
  val AUTH = "auth"
  val NAME = "name"
  val HOSTNAME = "smtp.host"
  val SMTP_PORT = "smtp.port"
  val USERNAME = "username"
  val PASSWORD = "password"
  val SSL_ON_CONNECT = "ssl.on.connect"
  val FROM = "from"
  val TO = "to"
}
