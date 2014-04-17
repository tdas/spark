package org.apache.spark.streaming.examples.demo

import org.apache.spark.{SparkConf, Logging}
import org.apache.spark.ui.JettyUtils
import org.apache.spark.util.Utils
import org.apache.spark.SecurityManager

import org.eclipse.jetty.server.Handler

import javax.servlet.http.HttpServletRequest
import scala.collection.mutable.ArrayBuffer
import scala.xml.Node


class ClusteringDemoUI() extends Logging {
  val host = Option(System.getenv("SPARK_PUBLIC_DNS")).getOrElse(Utils.localHostName())
  val port = 6060
  val handler = (request: HttpServletRequest) => this.render(request)
  val data = ArrayBuffer[String]()
  val tableHeaders = Seq("Tag", "Frequency")
  val conf = new SparkConf()
  val securityManager = new SecurityManager(conf)
  JettyUtils.startJettyServer(
    "0.0.0.0", port, Seq(JettyUtils.createServletHandler("/", handler, securityManager)), conf)

  def update(newData: Seq[String]) {
    data.clear()
    data ++= newData
  }

  def render(request: HttpServletRequest): Seq[Node] = {

    <html>
      <head>
        <meta http-equiv="refresh" content="1"/>
      </head>
      <body>
        <table class="table table-bordered table-striped table-condensed sortable table-fixed">
          <thead><th align="left">Filtered Tweets</th></thead>
          <tbody>
            {data.map(r => <tr><td>{r}</td></tr>) } 
          </tbody>
        </table>
      </body>
    </html>
  }
}


