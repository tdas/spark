package org.apache.spark.streaming.storage

import java.nio.ByteBuffer

import scala.collection.mutable.ArrayBuffer
import scala.concurrent.{ExecutionContext, Future}

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.spark.Logging
import org.apache.spark.streaming.storage.WriteAheadLogManager._
import org.apache.spark.streaming.util.{Clock, ManualClock, SystemClock}
import org.apache.spark.util.Utils

private[streaming] class WriteAheadLogManager(
    logDirectory: String,
    hadoopConf: Configuration,
    rollingIntervalSecs: Int = 60,
    maxFailures: Int = 3,
    threadPoolName: String = "WriteAheadLogManager",
    clock: Clock = new SystemClock
  ) extends Logging {

  private val pastLogs = new ArrayBuffer[LogInfo]
  implicit private val executionContext =
    ExecutionContext.fromExecutorService(Utils.newDaemonFixedThreadPool(1, threadPoolName))

  private var currentLogPath: String = null
  private var currentLogWriter: WriteAheadLogWriter = null
  private var currentLogWriterStartTime: Long = -1L
  private var currentLogWriterStopTime: Long = -1L

  recoverPastLogs()

  def writeToLog(byteBuffer: ByteBuffer): FileSegment = synchronized {
    var fileSegment: FileSegment = null
    var failures = 0
    var lastException: Exception = null
    var succeeded = false
    while (!succeeded && failures < maxFailures) {
      try {
        fileSegment = getLogWriter.write(byteBuffer)
        succeeded = true
      } catch {
        case ex: Exception =>
          lastException = ex
          logWarning("Failed to ...")
          resetWriter()
          failures += 1
      }
    }
    if (fileSegment == null) {
      throw lastException
    }
    fileSegment
  }

  def readFromLog(): Iterator[ByteBuffer] = {
    logsToIterator(pastLogs.map{ _.path}, hadoopConf)
  }

  def clearOldLogs(threshTime: Long): Unit = {
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
          case ex: Exception =>
            logWarning(s"Error clearing log file $logInfo", ex)
        }
      }
      logInfo(s"Cleared log files in $logDirectory older than $threshTime")
    }

    Future { deleteFiles() }
  }

  private def getLogWriter: WriteAheadLogWriter = synchronized {
    val currentTime = clock.currentTime()
    if (currentLogPath == null || currentTime > currentLogWriterStopTime) {
      resetWriter()
      if (currentLogPath != null) {
        pastLogs += LogInfo(currentLogWriterStartTime, currentLogWriterStopTime, currentLogPath)
      }
      currentLogWriterStartTime = currentTime
      currentLogWriterStopTime = currentTime + (rollingIntervalSecs * 1000)
      val newLogPath = new Path(logDirectory,
        timeToLogFile(currentLogWriterStartTime, currentLogWriterStopTime))
      currentLogPath = newLogPath.toString
      currentLogWriter = new WriteAheadLogWriter(currentLogPath, hadoopConf)
    }
    currentLogWriter
  }

  private def recoverPastLogs(): Unit = synchronized {
    val logDirectoryPath = new Path(logDirectory)
    val fs = logDirectoryPath.getFileSystem(hadoopConf)
    if (fs.exists(logDirectoryPath) && fs.getFileStatus(logDirectoryPath).isDir) {
      val logFiles = fs.listStatus(logDirectoryPath).map { _.getPath }
      pastLogs.clear()
      pastLogs ++= logFilesTologInfo(logFiles)
      logInfo(s"Recovered ${logFiles.size} log files from $logDirectory")
      logDebug(s"Recoved files are:\n${logFiles.mkString("\n")}")
    }
  }

  private def resetWriter(): Unit = synchronized {
    if (currentLogWriter != null) {
      currentLogWriter.close()
      currentLogWriter = null
    }
  }

/*
  private def tryMultipleTimes[T](message: String)(body: => T): T = {
    var result: T = null.asInstanceOf[T]
    var failures = 0
    var lastException: Exception = null
    var succeeded = false
    while (!succeeded && failures < maxFailures) {
      try {
        result = body
        succeeded = true
      } catch {
        case ex: Exception =>
          lastException = ex
          resetWriter()
          failures += 1
          logWarning(message, ex)
      }
    }
    if (!succeeded) {
      throw new Exception(s"Failed $message after $failures failures", lastException)
    }
    result
  } */
}

private[storage] object WriteAheadLogManager {

  case class LogInfo(startTime: Long, endTime: Long, path: String)

  val logFileRegex = """log-(\d+)-(\d+)""".r

  def timeToLogFile(startTime: Long, stopTime: Long): String = {
    s"log-$startTime-$stopTime"
  }

  def logFilesTologInfo(files: Seq[Path]): Seq[LogInfo] = {
    println("Creating log info with " + files.mkString("[","],[","]"))
    files.flatMap { file =>
      logFileRegex.findFirstIn(file.getName()) match {
        case logFileRegex(startTimeStr, stopTimeStr) =>
          val startTime = startTimeStr.toLong
          val stopTime = stopTimeStr.toLong
          Some(LogInfo(startTime, stopTime, file.toString))
        case _ => None
      }
    }.sortBy { _.startTime }
  }

  def logsToIterator(
      chronologicallySortedLogFiles: Seq[String],
      hadoopConf: Configuration
    ): Iterator[ByteBuffer] = {
    println("Creating iterator with " + chronologicallySortedLogFiles.mkString("[", "],[", "]"))
    chronologicallySortedLogFiles.iterator.map { file =>
      println(s"Creating log reader with $file")
      new WriteAheadLogReader(file, hadoopConf)
    } flatMap { x => x }
  }
}
