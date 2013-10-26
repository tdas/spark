package org.apache.spark.streaming.examples.demo

import org.apache.spark.streaming._
import org.apache.spark.rdd.RDD
import twitter4j.conf.ConfigurationBuilder
import twitter4j.auth.{OAuthAuthorization, Authorization}


case class OAuthDetails(consumerKey: String, consumerSecret: String, accessToken: String, accessTokenSecret: String)

object TwitterDemoHelper {

  val twitterOAuthDetails = Seq(
    OAuthDetails(
      "aI9wwmvu4tjYnx5TkiHeA", "3FfhHlhpiO9qCwqeZuJY6V88yuPQuOfdBOpePemcU", 
      "1088099640-nTuq8qB939p5dVbDRK2QLrEQYDLaeat1b1eFusX", "pPHRVqSPwsaGbciSxW8F4OOoWXh5oM1mHGxiYAXGFYAMQ"
    ),
    OAuthDetails(
      "KGQvqVzgF3ZLct8sbip01w", "mLb1wBHUnHyUpu2eNVPuwODh3pAlwrkQDMFzhVWGM",
      "1088101806-TJyxC7xFaXcEbhEanepOjvY3bjW7dJfbF9BaUfE", "PuOm0XzNls4HVH7Ml1dYM7jI0vRiIjI7yfFGaLzQ6Ecg5"
    ),
    OAuthDetails(
      "Eb1oKYbmNg3Zt4WhJiIBA", "1HiOCIOZ1OyPWBHKN6R5ttaVibh4tnpgMM23TIWma4",
      "1088142601-QyvIODVlyU65dxHruOT8ujSg3NLzbCfPsWoLV0I", "89wvox5NQUC7n7aVaP6HwkcxM0s0Ri3q2rwF9r8o67ZPC"
    ),
    OAuthDetails(
      "aoBPytmo5jrSGkMrbAbUw", "sJM4DZ1ar787a1PLyClvc0lvaT0LsWGnWTjKmsKc",
      "1090129970-iuPrjGZeBsoAtrjmVrLQjvHTccVscWEMJqphjsm", "DIKioeWTIAxNgBoqbvGQj5amBrdsAcaA0apI7Ac48SNse"
    ),
    OAuthDetails(
      "WrjOBGboXbsoFYF0CAh3Aw", "EQjiGUHPvXQ8Fs36qahBBVxhnhTRIZQZtIXXCCDKjU",
      "2155676874-T3WPsHNkz3jqdPPUFx6H2yH5u1X4wapNDBRwi8M", "zd03Nk8Qzhaanq5Z7ukRRLS9z0H4qcd734HsBfJ6WyXuG"
    )
  )

  def authorizations(num: Int): Seq[Authorization] = {
    assert(twitterOAuthDetails.length >= num)
    twitterOAuthDetails.map(oauth => {
      val confBuilder = new ConfigurationBuilder()
      confBuilder.setOAuthConsumerKey(oauth.consumerKey)
      confBuilder.setOAuthConsumerSecret(oauth.consumerSecret)
      confBuilder.setOAuthAccessToken(oauth.accessToken)
      confBuilder.setOAuthAccessTokenSecret(oauth.accessTokenSecret)
      val conf = confBuilder.build()
      new OAuthAuthorization(conf)
    }).take(num)
  }

  def showTopTags(k: Int)(rdd: RDD[(Long, String)], time: Time) {
    val topTags = rdd.take(k)
    val topTagsString =  topTags.map(x => "Tag: " + x._2.formatted("%-30s") + "\t Freq: " + x._1).mkString("\n")
    println("\nPopular tags in last 60 seconds (at " + time + ")\n" + topTagsString)
    // val topTagsTable = "<table>\n"+ topTags.map(x => "<td>" + x._2 + "</td><td>" + x._1 + "</td>").mkString("<tr>", "</tr><tr>", "</tr>") + "\n</table>"
    // StreamingDashboard.updateContents("Popular tags in last 60 seconds", Seq(topTagsTable))*/
  }
}


