package org.apache.spark.streaming.examples.demo

import org.apache.spark.streaming._
import org.apache.spark.rdd.RDD
import org.apache.spark.{SecurityManager, SparkConf, Logging}
import org.apache.spark.ui.{UIUtils, JettyUtils}
import org.eclipse.jetty.server.Handler
import org.apache.spark.ui.JettyUtils._
import scala.xml.Node
import javax.servlet.http.HttpServletRequest
import org.apache.spark.util.Utils
import scala.collection.mutable.ArrayBuffer



object TagCountDemoHelper {

  import twitter4j.Status 

  lazy val demoUI = new TagCountDemoUI()

  def getTags(status: Status) = status.getText.split(" ").filter(_.startsWith("#"))
  
  def showTopTags(k: Int)(rdd: RDD[(Long, String)], time: Time) {
    val topTags = rdd.take(k)
    val topTagsString =  topTags.map(x => "Tag: " + x._2.formatted("%-30s") + "\t Freq: " + x._1).mkString("\n")
    println("\nPopular tags in last 60 seconds (at " + time + ")\n" + topTagsString)
    demoUI.updateData(topTags.map(_.swap))
  }
}


class TagCountDemoUI() extends Logging {
  val host = Option(System.getenv("SPARK_PUBLIC_DNS")).getOrElse(Utils.localHostName())
  val port = 6060
  val handler = (request: HttpServletRequest) => this.render(request)
  val tagFreqData = ArrayBuffer[(String, Long)]()
  val tableHeaders = Seq("Tag", "Frequency")
  val conf = new SparkConf()
  val securityManager = new SecurityManager(conf)
  JettyUtils.startJettyServer(
    "0.0.0.0", port, Seq(JettyUtils.createServletHandler("/", handler, securityManager)), conf)

  def updateData(newData: Seq[(String, Long)]) {
    tagFreqData.clear()
    tagFreqData ++= newData
  }

  def render(request: HttpServletRequest): Seq[Node] = {
    import UIUtils._
    <html>
      <head>
        <meta http-equiv="refresh" content="1"/>
      </head>
      <body>
        <table class="table table-bordered table-striped table-condensed sortable table-fixed">
          <thead><th width="80%" align="left">Tag</th><th width="20%">Frequency</th></thead>
          <tbody>
            {tagFreqData.map(r => makeRow(r))}
          </tbody>
        </table>
      </body>
    </html>
  }

  def makeRow(tagFreq: (String, Long)): Seq[Node] = {
    <tr>
      <td>
        {tagFreq._1}
      </td>
      <td>
        {tagFreq._2}
      </td>
    </tr>
  }
}


