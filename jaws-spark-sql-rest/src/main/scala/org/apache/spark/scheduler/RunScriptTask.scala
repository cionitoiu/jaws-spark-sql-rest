package org.apache.spark.scheduler

import java.util.concurrent.TimeUnit

import com.xpatterns.jaws.data.contracts.DAL
import org.apache.hadoop.conf.{ Configuration => HadoopConfiguration }
import com.xpatterns.jaws.data.utils.Utils._
import server.Configuration
import org.apache.commons.lang.time.DurationFormatUtils
import com.xpatterns.jaws.data.utils.QueryState
import implementation.HiveContextWrapper
import com.xpatterns.jaws.data.DTO.QueryMetaInfo
import akka.pattern.ask
import messages.RegisterTableMessage
import akka.util.Timeout
import scala.util.Failure
import scala.concurrent.ExecutionContext.Implicits.global
import messages.ErrorMessage
import scala.util.Success
import server.JawsController
import scala.concurrent.Future
import messages.RunScriptMessage
import messages.RunParquetMessage
import com.xpatterns.jaws.data.utils.ResultsConverter
import org.apache.spark.sql.hive.HiveUtils
/**
 * Created by emaorhian
 */
class RunScriptTask(dals: DAL, hiveContext: HiveContextWrapper,
                    uuid: String, hdfsConf: HadoopConfiguration, runMessage: RunScriptMessage,
                    @volatile var isCanceled: Boolean = false) extends Runnable {

  override def run() {
    try {
      // parse the hql into independent commands
      val commands = HiveUtils.parseHql(runMessage.script)
     
      HiveUtils.logInfoMessage(uuid, s"There are ${commands.length} commands that need to be executed", "sparksql",
                               dals.loggingDal,runMessage.userId)

      val startTime = System.currentTimeMillis()

      // Set the start time for the query
      dals.loggingDal.setTimestamp(uuid, startTime, runMessage.userId)

      // job group id used to identify these jobs when trying to cancel them.
      hiveContext.sparkContext.setJobGroup(uuid, "")

      val result: ResultsConverter = executeCommands(commands)

      val executionTime = System.currentTimeMillis() - startTime
      val formattedDuration = DurationFormatUtils.formatDurationHMS(executionTime)

      HiveUtils.logInfoMessage(uuid, s"The total execution time was: $formattedDuration!", "sparksql",
                               dals.loggingDal, runMessage.userId)
      writeResults(result, executionTime, runMessage.userId)
    } catch {
      case e: Exception =>
        val message = getCompleteStackTrace(e)
        Configuration.log4j.error(message)
        HiveUtils.logMessage(uuid, message, "sparksql", dals.loggingDal,runMessage.userId)
        dals.loggingDal.setState(uuid, QueryState.FAILED, runMessage.userId)
        dals.loggingDal.setRunMetaInfo(uuid, new QueryMetaInfo(0, runMessage.maxNumberOfResults, 0, runMessage.limited),
                                       runMessage.userId)
        throw new RuntimeException(e)
    }
  }

  def executeCommands(commands: List[String]): ResultsConverter = {
    var result: ResultsConverter = null
    val nrOfCommands = commands.size

    for (commandIndex <- 1 to nrOfCommands) {
      val cmd = commands(commandIndex - 1)
      val isLastCmd = if (commandIndex == nrOfCommands) true else false
      isCanceled match {
        case false =>
          result = HiveUtils.runCmdRdd(cmd,
                                       hiveContext,
                                       Configuration.numberOfResults.getOrElse("100").toInt,
                                       uuid,
                                       runMessage.limited,
                                       runMessage.maxNumberOfResults,
                                       isLastCmd,
                                       Configuration.rddDestinationIp.get,
                                       dals.loggingDal,
                                       hdfsConf,
                                       runMessage.rddDestination,
                                       runMessage.userId)
          HiveUtils.logInfoMessage(uuid, s"Command progress : There were executed $commandIndex commands out " +
                                         s"of $nrOfCommands", "sparksql", dals.loggingDal, runMessage.userId)

        case _ =>
          val message = s"The command $cmd was canceled!"
          Configuration.log4j.warn(message)
          HiveUtils.logMessage(uuid, message, "sparksql", dals.loggingDal, runMessage.userId)

      }
    }

    result
  }

  private def writeResults(result: ResultsConverter, executionTime:Long, userId: String) {
    isCanceled match {
      case false =>
        Option(result) match {
          case None => Configuration.log4j.debug("[RunScriptTask] result is null")
          case _    => dals.resultsDal.setResults(uuid, result, userId)
        }
        dals.loggingDal.setState(uuid, QueryState.DONE, userId)
        dals.loggingDal.setExecutionTime(uuid, executionTime, userId)
      case _ =>
        val message = s"The query failed because it was canceled!"
        Configuration.log4j.warn(message)
        HiveUtils.logMessage(uuid, message, "sparksql", dals.loggingDal, userId)
        dals.loggingDal.setState(uuid, QueryState.FAILED, userId)
        dals.loggingDal.setExecutionTime(uuid, executionTime, userId)
    }
  }

  def setCanceled(canceled: Boolean) {
    isCanceled = canceled
  }

}

class RunParquetScriptTask(dals: DAL, hiveContext: HiveContextWrapper,
                           uuid: String, hdfsConf: HadoopConfiguration,
                           runMessage: RunParquetMessage, isCanceled: Boolean = false)
  extends RunScriptTask(dals, hiveContext, uuid, hdfsConf,
                        new RunScriptMessage(runMessage.script, runMessage.limited, runMessage.maxNumberOfResults,
                                             runMessage.rddDestination, runMessage.userId), isCanceled) {
  override def run() {
    implicit val timeout = Timeout(Configuration.timeout, TimeUnit.MILLISECONDS)
    val future = ask(JawsController.balancerActor,
                     RegisterTableMessage(runMessage.table, runMessage.tablePath, runMessage.namenode, runMessage.userId))
      .map(innerFuture => innerFuture.asInstanceOf[Future[Any]])
      .flatMap(identity)

    future onComplete {
      case Success(x) => x match {
        case e: ErrorMessage =>
          HiveUtils.logMessage(uuid, e.message, "sparksql", dals.loggingDal, runMessage.userId)
          dals.loggingDal.setState(uuid, QueryState.FAILED, runMessage.userId)
        case result: String =>
          Configuration.log4j.info(result)
          super.run()
      }
      case Failure(ex) =>
        HiveUtils.logMessage(uuid, getCompleteStackTrace(ex), "sparksql", dals.loggingDal, runMessage.userId)
        dals.loggingDal.setState(uuid, QueryState.FAILED, runMessage.userId)
    }
    super.run()
  }

}