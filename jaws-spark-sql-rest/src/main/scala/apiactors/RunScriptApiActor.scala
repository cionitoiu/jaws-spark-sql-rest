package apiactors

import akka.actor.Actor
import com.xpatterns.jaws.data.contracts.DAL
import messages._
import java.util.UUID
import server.Configuration
import com.google.common.cache.CacheBuilder
import java.util.concurrent.TimeUnit
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor
import com.google.common.cache.Cache
import org.apache.spark.scheduler.RunScriptTask
import implementation.HiveContextWrapper
import scala.util.Try
import apiactors.ActorOperations._
import org.apache.spark.scheduler.RunParquetScriptTask
import org.apache.spark.sql.hive.HiveUtils
import com.xpatterns.jaws.data.utils.QueryState
/**
 * Created by emaorhian
 */
class RunScriptApiActor(hiveContext: HiveContextWrapper, dals: DAL) extends Actor {
  var taskCache: Cache[String, RunScriptTask] = _
  var threadPool: ThreadPoolTaskExecutor = _
 

  override def preStart() {
    taskCache = {
      CacheBuilder
        .newBuilder()
        .maximumSize(1000)
        .expireAfterWrite(1, TimeUnit.HOURS)
        .build[String, RunScriptTask]
    }

    threadPool = new ThreadPoolTaskExecutor()
    threadPool.setCorePoolSize(Configuration.nrOfThreads.getOrElse("10").toInt)
    threadPool.initialize()
  }

  override def receive = {
    case message: RunScriptMessage =>

      val uuid = System.currentTimeMillis() + UUID.randomUUID().toString
      val tryRun = Try {
        Configuration.log4j.info("[RunScriptApiActor -run]: running the following script: " + message.script +
                                 " for user " + message.userId)
        Configuration.log4j.info("[RunScriptApiActor -run]: The script will be executed with the limited flag set on "
                                  + message.limited + ". The maximum number of results is " + message.maxNumberOfResults)

        val task = new RunScriptTask(dals, hiveContext, uuid, message.hdfsConf, message)
        taskCache.put(uuid, task)
        writeLaunchStatus(uuid, message.script, message.userId)
        threadPool.execute(task)
      }
      returnResult(tryRun, uuid, "run query failed with the following message: ", sender())

    case message:RunQueryMessage =>
      val uuid = System.currentTimeMillis() + UUID.randomUUID().toString
      val tryRunMessage = Try {
        // Make sure that there is a query with the sent name
        val queryName = message.name.trim()
        val queries = dals.loggingDal.getQueriesByName(queryName, message.userId).queries
        if (queries.length == 0 || queries(0).query == null) {
          throw new Exception(s"There is no query with the name $queryName")
        }

        val query = queries(0)
        // Set the previous query not published
        if (query.metaInfo.published == Some(true)) {
          dals.loggingDal.deleteQueryPublishedStatus(query.metaInfo.name.get, query.metaInfo.published, message.userId)
        }

        // Save the query name and prepare a message to execute the run query
        dals.loggingDal.setQueryProperties(uuid, query.metaInfo.name, query.metaInfo.description,
          query.metaInfo.published, overwrite = true, message.userId)

        val runScript = RunScriptMessage(query.query, query.metaInfo.isLimited, query.metaInfo.maxNrOfResults,
          query.metaInfo.resultsDestination.toString, message.userId, message.hdfsConf)
        Configuration.log4j.info("[RunScriptApiActor -run]: running the following query: " + queryName)
        Configuration.log4j.info("[RunScriptApiActor -run]: running the following script: " + runScript.script)
        Configuration.log4j.info("[RunScriptApiActor -run]: The script will be executed with the limited flag set on "
                                 + runScript.limited + ". The maximum number of results is " + runScript.maxNumberOfResults)

        val task = new RunScriptTask(dals, hiveContext, uuid, message.hdfsConf, runScript)
        taskCache.put(uuid, task)
        writeLaunchStatus(uuid, query.query, message.userId)
        threadPool.execute(task)
      }
      returnResult(tryRunMessage, uuid, "run query failed with the following message: ", sender())

    case message: RunParquetMessage =>
      val uuid = System.currentTimeMillis() + UUID.randomUUID().toString
      val tryRunParquet = Try {

        Configuration.log4j.info(s"[RunScriptApiActor -runParquet]: running the following sql: ${message.script} " +
                                 s"for user ${message.userId}")
        Configuration.log4j.info(s"[RunScriptApiActor -runParquet]: The script will be executed over " +
                                 s"the ${message.tablePath} file with the ${message.table} table name")
     
        val task = new RunParquetScriptTask(dals, hiveContext, uuid, message.hdfsConf, message)
        taskCache.put(uuid, task)
        writeLaunchStatus(uuid, message.script, message.userId)
        threadPool.execute(task)
      }
      returnResult(tryRunParquet, uuid, "run parquet query failed with the following message: ", sender())

    case message: CancelMessage =>
      Configuration.log4j.info("[RunScriptApiActor]: Canceling the jobs for the following uuid: " + message.queryID +
                               " for user " + message.userId)

      val task = taskCache.getIfPresent(message.queryID)

      Option(task) match {
        case None => Configuration.log4j.info("No job to be canceled")
        case _ =>
          task.setCanceled(true)
          taskCache.invalidate(message.queryID)

          if (Option(hiveContext.sparkContext.getConf.get("spark.mesos.coarse")).getOrElse("true").equalsIgnoreCase("true")) {
            Configuration.log4j.info("[RunScriptApiActor]: Jaws is running in coarse grained mode!")
            hiveContext.sparkContext.cancelJobGroup(message.queryID)
          } else {
            Configuration.log4j.info("[RunScriptApiActor]: Jaws is running in fine grained mode!")
          }
      }
  }

  private def writeLaunchStatus(uuid: String, script: String, userId: String) {
    HiveUtils.logMessage(uuid, s"Launching task for $uuid", "sparksql", dals.loggingDal, userId)
    dals.loggingDal.setState(uuid, QueryState.IN_PROGRESS, userId)
    dals.loggingDal.setScriptDetails(uuid, script, userId)
  }

 
}