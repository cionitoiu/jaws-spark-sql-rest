package apiactors

import server.Configuration
import akka.actor.Actor
import scala.util.Try
import scala.util.Success
import scala.util.Failure
import com.xpatterns.jaws.data.contracts.DAL
import customs.CommandsProcessor._
import customs.ResultsProcessor._
import java.io.ByteArrayOutputStream
import java.io.OutputStreamWriter
import sys.process._
import scala.concurrent.ExecutionContext
import java.util.concurrent.Executors
import java.util.UUID
import com.xpatterns.jaws.data.utils.QueryState
import scala.concurrent._
import java.io.ByteArrayInputStream
import com.xpatterns.jaws.data.utils.Utils._

/**
 * Created by emaorhian
 */

case class RunQueryMessage(script: String, limit: Int, userId: String)
case class ErrorMessage(message: String)

class HiveRunnerActor(dals: DAL) extends Actor {

  override def receive = {

    case message: RunQueryMessage =>
      Configuration.log4j.info(s"[HiveRunnerActor]: Running script=${message.script}")
      val uuid = System.currentTimeMillis + UUID.randomUUID.toString
      implicit val ec = ExecutionContext.fromExecutor(Executors.newFixedThreadPool(Configuration.nrOfThreads.getOrElse("10").toInt))
      var script = ""

      val startTime = System.currentTimeMillis
      dals.loggingDal.setTimestamp(uuid, startTime, message.userId)

      val tryPreRunScript = Try {
        writeLaunchStatus(uuid, message.script, message.userId)
        script = prepareCommands(message.script, message.limit)
      }

      tryPreRunScript match {
        case Success(v) => sender ! uuid
        case Failure(e) => sender ! ErrorMessage(s"Run hive query failed with the following message: ${getCompleteStackTrace(e)}")
      }

      val runResponse = future {
        Configuration.log4j.info(s"[HiveRunnerActor]: Executing commands $script")
        runHiveScript(script, uuid, message.userId)
      }

      runResponse onComplete {
        case Success(s) =>
          val mes = s"[HiveRunnerActor]: Query $uuid has successfully finished"
          dals.resultsDal.setResults(uuid, s)
          setStatus(uuid, mes, QueryState.DONE, message.userId)
          val executionTime = System.currentTimeMillis - startTime
          dals.loggingDal.setExecutionTime(uuid, executionTime, message.userId)

        case Failure(e) =>
          val mes = s"[HiveRunnerActor]: Query $uuid has failed with the following exception ${getCompleteStackTrace(e)}"
          setStatus(uuid, mes, QueryState.FAILED, message.userId)

      }
  }

  private def runHiveScript(script: String, uuid: String, userId: String) = {
    val stdOutOS = new ByteArrayOutputStream
    val osWriter = new OutputStreamWriter(stdOutOS)

    val command = Seq("hive", "-e", script)

    try {
      command ! ProcessLogger(
        stdOutLine => osWriter.write(s"$stdOutLine\n"),
        stdErrLine => {
          Configuration.log4j.info(stdErrLine)
          dals.loggingDal.addLog(uuid, "hive", System.currentTimeMillis(), stdErrLine, userId)
        })
      osWriter flush ()

      getLastResults(new ByteArrayInputStream(stdOutOS.toByteArray))

    } finally {
      if (osWriter != null) osWriter close ()
    }
  }

  private def writeLaunchStatus(uuid: String, script: String, userId: String) {
    dals.loggingDal.addLog(uuid, "hive", System.currentTimeMillis(), s"Launching task for $uuid", userId)
    dals.loggingDal.setState(uuid, QueryState.IN_PROGRESS, userId)
    dals.loggingDal.setScriptDetails(uuid, script, userId)
  }

  private def setStatus(uuid: String, message: String, status: QueryState.Value, userId: String) {
    Configuration.log4j.info(message)
    dals.loggingDal.addLog(uuid, "hive", System.currentTimeMillis(), message, userId)
    dals.loggingDal.setState(uuid, status, userId)
  }
}
