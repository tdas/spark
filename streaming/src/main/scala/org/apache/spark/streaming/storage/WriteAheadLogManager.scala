package org.apache.spark.streaming.storage

import java.nio.ByteBuffer

import scala.collection.mutable.ArrayBuffer
import scala.concurrent.{ExecutionContext, Future}

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.spark.{Logging, SparkConf}
import org.apache.spark.util.Utils

private[streaming] class WriteAheadLogManager(logDirectory: String, conf: SparkConf,
    hadoopConf: Configuration, threadPoolName: String = "WriteAheadLogManager") extends Logging {

  private case class LogInfo(startTime: Long, endTime: Long, path: String)

  private val logWriterChangeIntervalSeconds =
    conf.getInt("spark.streaming.wal.interval", 60)
  private val logWriterMaxFailures =
    conf.getInt("spark.streaming.wal.maxRetries", 3)
  private val pastLogs = new ArrayBuffer[LogInfo]
  implicit private val executionContext =
    ExecutionContext.fromExecutorService(Utils.newDaemonFixedThreadPool(1, threadPoolName))

  private var currentLogPath: String = null
  private var currentLogWriter: WriteAheadLogWriter = null
  private var currentLogWriterStartTime: Long = -1L

  def writeToLog(byteBuffer: ByteBuffer): FileSegment = synchronized {
    var failures = 0
    var lastException: Exception = null
    var fileSegment: FileSegment = null
    var succeeded = false
    while (!succeeded && failures < logWriterMaxFailures) {
      try {
        fileSegment = logWriter.write(byteBuffer)
        succeeded = true
      } catch {
        case ex: Exception =>
          lastException = ex
          logWarning("Failed to ...")
          reset()
          failures += 1
      }
    }
    if (fileSegment == null) {
      throw lastException
    }
    fileSegment
  }

  def readFromLog(): Iterator[ByteBuffer] = {
    WriteAheadLogManager.logsToIterator(pastLogs sortBy { _.startTime} map { _.path}, hadoopConf)
  }

  def clear(threshTime: Long): Unit = {
    // Get the log files that are older than the threshold time, while accounting for possible
    // time skew between the node issues the threshTime (say, driver node), and the local time at
    // the node this is being executed (say, worker node)
    val maxTimeSkewMs = 60 * 1000 // 1 minute
    val oldLogFiles = synchronized { pastLogs.filter { _.endTime < threshTime - maxTimeSkewMs } }
    logDebug(s"Attempting to clear ${oldLogFiles.size} old log files in $logDirectory " +
      s"older than $threshTime: ${oldLogFiles.map { _.path }.mkString("\n")}")

    def deleteFiles() {
      oldLogFiles.foreach { logInfo =>
        try {
          val path = new Path(logInfo.path)
          val fs = hadoopConf.synchronized { path.getFileSystem(hadoopConf) }
          fs.delete(path, true)
          synchronized { pastLogs -= logInfo }
          logDebug(s"Cleared log file $logInfo")
        } catch {
          case ex: Exception => logWarning("Could not delete ...")
        }
      }
      logInfo(s"Cleared log files in $logDirectory older than $threshTime")
    }

    Future { deleteFiles() }
  }

  private def logWriter: WriteAheadLogWriter = synchronized {
    val currentTime = System.currentTimeMillis
    if (currentLogWriter == null ||
      currentTime - currentLogWriterStartTime > logWriterChangeIntervalSeconds * 1000) {
      pastLogs += LogInfo(currentLogWriterStartTime, currentTime, currentLogPath)
      val newLogPath = new Path(logDirectory, s"data-$currentTime".toString)
      currentLogPath = newLogPath.toString
      currentLogWriter = new WriteAheadLogWriter(currentLogPath, hadoopConf)
      currentLogWriterStartTime = currentTime
    }
    currentLogWriter
  }

  private def reset(): Unit = synchronized {
    currentLogWriter.close()
    currentLogWriter = null
  }
}

private[storage] object WriteAheadLogManager {
  def logsToIterator(
      chronologicallySortedLogFiles: Seq[String],
      hadoopConf: Configuration
    ): Iterator[ByteBuffer] = {
    chronologicallySortedLogFiles.iterator.map { file =>
      new WriteAheadLogReader(file, hadoopConf)
    } flatMap { x => x }
  }
}
