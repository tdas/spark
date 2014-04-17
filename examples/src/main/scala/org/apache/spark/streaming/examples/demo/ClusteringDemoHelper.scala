package org.apache.spark.streaming.examples.demo

import org.apache.spark.streaming._
import org.apache.spark.{SparkConf, Logging}
import org.apache.spark.ui.{UIUtils, JettyUtils}
import org.apache.spark.ui.JettyUtils._
import org.apache.spark.util.Utils

import org.eclipse.jetty.server.Handler

import javax.servlet.http.HttpServletRequest
import scala.collection.mutable.ArrayBuffer
import scala.xml.Node


class ClusteringDemoUI() extends Logging {
  val host = Option(System.getenv("SPARK_PUBLIC_DNS")).getOrElse(Utils.localHostName())
  val port = 6060
  val handler = (request: HttpServletRequest) => this.render(request)
  val tagFreqData = ArrayBuffer[(String, Long)]()
  val tableHeaders = Seq("Tag", "Frequency")
  JettyUtils.startJettyServer("0.0.0.0", port, Seq(("/", handler)), new SparkConf())

  def updateData(newData: Seq[(String, Long)]) {
    tagFreqData.clear()
    tagFreqData ++= newData
  }

  def render(request: HttpServletRequest): Seq[Node] = {
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
}


