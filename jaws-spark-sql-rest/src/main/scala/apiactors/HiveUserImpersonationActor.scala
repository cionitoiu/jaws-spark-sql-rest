package apiactors

import java.io.{PrintWriter, StringWriter}
import java.util.UUID
import java.util.concurrent.TimeUnit
import java.security._

import apiactors.ActorOperations._
import com.google.common.cache.{CacheBuilder, Cache}
import com.typesafe.config.{ConfigFactory, Config}
import com.xpatterns.jaws.data.DTO._
import com.xpatterns.jaws.data.utils.Utils._
import com.xpatterns.jaws.data.utils._
import implementation.SchemaSettingsFactory.{Tachyon, Hdfs, Parquet, Hive}
import messages.ResultFormat._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.analysis.NoSuchTableException
import org.apache.spark.sql.hive.HiveUtils._
import messages._
import org.apache.spark.sql.types.StructType
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor
import scala.concurrent._
import ExecutionContext.Implicits.global
import org.apache.hadoop.security.UserGroupInformation
import org.apache.spark.scheduler.{RunParquetScriptTask, RunScriptTask, LoggingListener}
import org.apache.spark.sql.hive.HiveUtils
import org.apache.spark.{SparkContext, SparkConf}
import server.Configuration
import org.apache.spark.sql.execution.datasources.parquet.SparkParquetUtility._

import scala.collection.JavaConverters._
import akka.actor.Actor
import com.xpatterns.jaws.data.contracts.DAL
import implementation.HiveContextWrapper
import scala.concurrent._
import scala.util.{Try, Failure, Success}


/**
  * Created by cristianionitoiu on 15/04/16.
  */
class HiveUserImpersonationActor (realUgi: UserGroupInformation, dals: DAL,
                                  hdfsConf : org.apache.hadoop.conf.Configuration) extends Actor {


    var taskCache: Cache[String, RunScriptTask] = _
    var contextCache : Cache[String, HiveContextWrapper] = _
    var threadPool: ThreadPoolTaskExecutor = _


    override def preStart() {
      taskCache = {
        CacheBuilder
          .newBuilder()
          .maximumSize(1000)
          .expireAfterWrite(1, TimeUnit.HOURS)
          .build[String, RunScriptTask]
      }

      contextCache = {
        CacheBuilder
          .newBuilder()
          .maximumSize(1000)
          .expireAfterWrite(24, TimeUnit.HOURS)
          .build[String, HiveContextWrapper]
      }

      threadPool = new ThreadPoolTaskExecutor()
      threadPool.setCorePoolSize(Configuration.nrOfThreads.getOrElse("10").toInt)
      threadPool.initialize()
    }


    override def receive = {
      case message: GetDatasourceSchemaMessage =>
        val hiveDSMContext = impersonateUser(message.userId, parquetInit=false)
        val hostname: String = Configuration.rddDestinationIp.get
        val path: String = s"${message.path}"
        Configuration.log4j.info(s"Getting the data source schema for path $path, sourceType ${message.sourceType}, " +
                                 s"storageType ${message.storageType}")
        val currentSender = sender

        val getDatasourceSchemaFuture = future {
          var result: StructType = null
          message.sourceType match {
            case Hive() =>

              try {
                val table = hiveDSMContext.table(path)
                result = table.schema
              } catch {
                // When the table doesn't exists, throw a new exception with a better message.
                case _:NoSuchTableException => throw new Exception("Table does not exist")
              }
            case Parquet() =>
              message.storageType match {
                case Hdfs() =>
                  val hdfsURL = HiveUtils.getHdfsPath(hostname)

                  // Make sure that file exists
                  checkFileExistence(message.hdfsConf, hdfsURL, path)

                  result = hiveDSMContext.readXPatternsParquet(hdfsURL, path).schema
                case Tachyon() =>
                  val tachyonURL = HiveUtils.getTachyonPath(hostname)

                  // Make sure that file exists
                  checkFileExistence(message.hdfsConf, tachyonURL, path)

                  result = hiveDSMContext.readXPatternsParquet(tachyonURL, path).schema
              }
          }

          Configuration.log4j.info("Reading the avro schema from result df")

          val avroSchema = AvroConverter.getAvroSchema(result).toString(true)
          Configuration.log4j.debug(avroSchema)
          avroSchema
        }

        getDatasourceSchemaFuture onComplete {
          case Success(result) => currentSender ! result
          case Failure(e) => currentSender ! ErrorMessage(s"GET data source schema failed with the following " +
                                                          s"message: ${getCompleteStackTrace(e)}")
        }

      case message: GetDatabasesMessage =>
        val hiveDMContext = impersonateUser(message.userId, parquetInit=false)
        Configuration.log4j.info("[HiveActor]: showing databases for user " + message.userId)
        val currentSender = sender

        val getDatabasesFuture = future {getDatabases(hiveDMContext)}

        getDatabasesFuture onComplete {
          case Success(result) => currentSender ! result
          case Failure(e) => currentSender ! ErrorMessage(s"GET databases failed with the following " +
                                                          s"message: ${e.getMessage}")
        }

      case message: GetTablesMessage =>
        val hiveTMContext = impersonateUser(message.userId, parquetInit=false)
        Configuration.log4j.info("[HiveActor]: showing tables for user " + message.userId)
        val currentSender = sender

        val getTablesFutures = future {
          // if no database is specified, the tables for all databases will be retrieved
          Option(message.database).getOrElse("") match {
            case "" =>
              getDatabases(hiveTMContext).databases.map(db => getTablesForDatabase(db, new Regular, message.describe,
                                                                                   hiveTMContext))

            case _ =>
              // if there is a list of tables specified, then
              if (Option(message.tables).getOrElse(Array.empty).isEmpty) {
                Array(getTablesForDatabase(message.database, new Regular, message.describe, hiveTMContext))

              } else {
                Array(Tables(message.database, message.tables map (table =>
                                                describeTable(message.database, table, new Regular, hiveTMContext))))
              }
          }
        }

      case message: GetExtendedTablesMessage =>
        val hiveETContext = impersonateUser(message.userId, parquetInit=false)
        Configuration.log4j.info("[HiveActor]: showing extended tables for user " + message.userId)
        val currentSender = sender
        val getExtendedTablesFuture = future {
          Option(message.tables).getOrElse(Array.empty).isEmpty match {
            case true => Array(getTablesForDatabase(message.database, new Extended, describe=true, hiveETContext))
            case _    => Array(Tables(message.database, message.tables map (table =>
                                                describeTable(message.database, table, new Extended, hiveETContext))))
          }
        }

        getExtendedTablesFuture onComplete {
          case Success(result) => currentSender ! result
          case Failure(e)      => currentSender ! ErrorMessage(s"GET extended tables failed with the following " +
                                                               s"message: ${e.getMessage}")
        }


      case message: GetFormattedTablesMessage =>
        val hiveFTContext = impersonateUser(message.userId, parquetInit=false)
        Configuration.log4j.info("[HiveActor]: showing formatted tables for user " + message.userId)
        val currentSender = sender

        val getFormattedTablesFuture = future {
          Option(message.tables).getOrElse(Array.empty).isEmpty match {
            case true => Array(getTablesForDatabase(message.database, new Formatted, describe=true, hiveFTContext))
            case _    => Array(Tables(message.database, message.tables map (table =>
                                          describeTable(message.database, table, new Formatted, hiveFTContext))))
          }
        }

        getFormattedTablesFuture onComplete {
          case Success(result) => currentSender ! result
          case Failure(e)      => currentSender ! ErrorMessage(s"GET formatted tables failed with the following " +
                                                               s"message: ${e.getMessage}")
        }

      case message: GetParquetTablesMessage =>
        val hivePTContext = impersonateUser(message.userId, parquetInit=true)
        val currentSender = sender

        val getTablesFuture = future {
          if (message.tables.isEmpty) {
            val tables = dals.parquetTableDal.listParquetTables(message.userId)
            message.describe match {
              case true  => Array(Tables("None", tables map (pTable => getFields(pTable.name, hivePTContext))))
              case false => Array(Tables("None", tables map (pTable => Table(pTable.name, Array.empty, Array.empty))))
            }

          } else {
            val tablesMap = message.tables.map(table => {
              if (!dals.parquetTableDal.tableExists(table, message.userId))
                throw new Exception(s" Table $table does not exist")
              getFields(table, hivePTContext)
            })
            Array(Tables("None", tablesMap))
          }
        }

        getTablesFuture onComplete {
          case Success(result) => currentSender ! result
          case Failure(e)      => currentSender ! ErrorMessage(s"GET tables failed with the following message: ${e.getMessage}")
        }

      case message: RegisterParquetTableMessage =>
        val hiveRPTContext = impersonateUser(message.userId, parquetInit=true)
        Configuration.log4j.info(s"[HiveActor]: registering table ${message.name} at ${message.path} " +
          s"for user ${message.userId}")
        val currentSender = sender

        val registerTableFuture = future {
          val (namenode, folderPath) = if (message.namenode.isEmpty) HiveUtils.splitPath(message.path)
          else (message.namenode, message.path)
          HiveUtils.registerParquetTable(hiveRPTContext, message.name, namenode, folderPath, dals, message.userId)
        }

        registerTableFuture onComplete {
          case Success(_) => currentSender ! s"Table ${message.name} was registered"
          case Failure(e) => val sw = new StringWriter
            e.printStackTrace(new PrintWriter(sw))
            currentSender ! ErrorMessage(s"RegisterTable failed with the following message: ${sw.toString}")
        }

      case message: UnregisterParquetTableMessage =>
        val hiveUPTContext = impersonateUser(message.userId, parquetInit=true)
        Configuration.log4j.info(s"[HiveActor]: Unregistering table ${message.name}")
        val currentSender = sender

        val unregisterTableFuture = future {
          // unregister table
          hiveUPTContext.unregisterTable(message.name)
          dals.parquetTableDal.deleteParquetTable(message.name, message.userId)
        }

        unregisterTableFuture onComplete {
          case Success(result) => currentSender ! s"Table ${message.name} was unregistered"
          case Failure(e) => currentSender ! ErrorMessage(s"UnregisterTable failed with the following message: ${e.getMessage}")
        }
      case message: GetResultsMessage =>
        val hiveRMContext = impersonateUser(message.userId, parquetInit=false)
        Configuration.log4j.info(s"[GetResultsMessage]: retrieving results for: ${message.queryID} in " +
          s"the ${message.format} and user ${message.userId}")
        val currentSender = sender
        val getResultsFuture = future {

          val (offset, limit) = getOffsetAndLimit(message)
          val metaInfo = dals.loggingDal.getMetaInfo(message.queryID, message.userId) ///change

          metaInfo.resultsDestination match {
            // cassandra
            case 0 =>
              val endIndex = offset + limit
              message.format match {
                case AVRO_BINARY_FORMAT => new AvroBinaryResult(getDBAvroResults(message.queryID, offset, endIndex, message.userId))
                case AVRO_JSON_FORMAT   => getDBAvroResults(message.queryID, offset, endIndex, message.userId).result
                case _                  => getCustomResults(message.queryID, offset, endIndex, message.userId)
              }

            //hdfs
            case 1 =>
              val destinationPath = HiveUtils.getHdfsPath(Configuration.rddDestinationIp.get)
              getFormattedResult(message.format, getResults(offset, limit, destinationPath, message.hdfsConf,
                                 message.queryID, hiveRMContext))

            //tachyon
            case 2 =>
              val destinationPath = HiveUtils.getTachyonPath(Configuration.rddDestinationIp.get)
              getFormattedResult(message.format, getResults(offset, limit, destinationPath, message.hdfsConf,
                                 message.queryID, hiveRMContext))

            case _ =>
              Configuration.log4j.info("[GetResultsMessage]: Unidentified results path : " + metaInfo.resultsDestination)
              null

          }
        }

        getResultsFuture onComplete {
          case Success(results) => currentSender ! results
          case Failure(e)       => currentSender ! ErrorMessage(s"GET results failed with the following message: ${e.getMessage}")
        }

      case message: RunScriptMessage =>
        val hiveRSMContext = impersonateUser(message.userId, parquetInit=false)
        val uuid = System.currentTimeMillis() + UUID.randomUUID().toString
        val tryRun = Try {
          Configuration.log4j.info("[HiveActor -run]: running the following script: " + message.script +
            " for user " + message.userId)
          Configuration.log4j.info("[HiveActor -run]: The script will be executed with the limited flag set on "
            + message.limited + ". The maximum number of results is " + message.maxNumberOfResults)

          val task = new RunScriptTask(dals, hiveRSMContext, uuid, message.hdfsConf, message)
          taskCache.put(uuid, task)
          writeLaunchStatus(uuid, message.script, message.userId)
          threadPool.execute(task)
        }
        returnResult(tryRun, uuid, "run query failed with the following message: ", sender)

      case message:RunQueryMessage =>
        val hiveRQMContext = impersonateUser(message.userId, parquetInit=false)
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
          Configuration.log4j.info("[HiveActor -run]: running the following query: " + queryName)
          Configuration.log4j.info("[HiveActor -run]: running the following script: " + runScript.script)
          Configuration.log4j.info("[HiveActor -run]: The script will be executed with the limited flag set on "
            + runScript.limited + ". The maximum number of results is " + runScript.maxNumberOfResults)

          val task = new RunScriptTask(dals, hiveRQMContext, uuid, message.hdfsConf, runScript)
          taskCache.put(uuid, task)
          writeLaunchStatus(uuid, query.query, message.userId)
          threadPool.execute(task)
        }
        returnResult(tryRunMessage, uuid, "run query failed with the following message: ", sender)

      case message: RunParquetMessage =>
        val hiveRPMContext = impersonateUser(message.userId, parquetInit=false)
        val uuid = System.currentTimeMillis() + UUID.randomUUID().toString
        val tryRunParquet = Try {

          Configuration.log4j.info(s"[HiveActor -runParquet]: running the following sql: ${message.script} " +
            s"for user ${message.userId}")
          Configuration.log4j.info(s"[HiveActor -runParquet]: The script will be executed over " +
            s"the ${message.tablePath} file with the ${message.table} table name")

          val task = new RunParquetScriptTask(dals, hiveRPMContext, uuid, message.hdfsConf, message)
          taskCache.put(uuid, task)
          writeLaunchStatus(uuid, message.script, message.userId)
          threadPool.execute(task)
        }
        returnResult(tryRunParquet, uuid, "run parquet query failed with the following message: ", sender)

      case message: CancelMessage =>
        val hiveCContext = impersonateUser(message.userId, parquetInit=false)
        Configuration.log4j.info("[HiveActor]: Canceling the jobs for the following uuid: " + message.queryID +
          " for user " + message.userId)

        val task = taskCache.getIfPresent(message.queryID)

        Option(task) match {
          case None => Configuration.log4j.info("No job to be canceled")
          case _ =>
            task.setCanceled(true)
            taskCache.invalidate(message.queryID)

            if (Option(hiveCContext.sparkContext.getConf.get("spark.mesos.coarse")).getOrElse("true").equalsIgnoreCase("true")) {
              Configuration.log4j.info("[HiveActor]: Jaws is running in coarse grained mode!")
              hiveCContext.sparkContext.cancelJobGroup(message.queryID)
            } else {
              Configuration.log4j.info("[HiveActor]: Jaws is running in fine grained mode!")
            }
        }
      case message: Any => Configuration.log4j.error(message.toString)
    }

  /**
    * Hive context creation using user impersonation
    * @param userId
    * @return
    */
    private def createHiveContext(userId: String): HiveContextWrapper = {
      val jars = Configuration.jarPath.get.split(",")

      def configToSparkConf(config: Config, contextName: String, jars: Array[String]): SparkConf = {
        val sparkConf = new SparkConf().setAppName(contextName).setJars(jars)
        sparkConf.setMaster("yarn-client")
        sparkConf.set("spark.yarn.principal", Configuration.kerberosPrincipal.get)
        sparkConf.set("spark.yarn.keytab", Configuration.kerberosKeytab.get)
        for (
          property <- config.entrySet().asScala if property.getKey.startsWith("spark") && property.getValue != null
        ) {
          val key = property.getKey.replaceAll("-", ".")
          println(key + " | " + property.getValue.unwrapped())
          sparkConf.set(key, property.getValue.unwrapped().toString)
        }
        sparkConf
      }

      val hContext: HiveContextWrapper = {
        val sparkConf = configToSparkConf(Configuration.sparkConf, Configuration.applicationName.getOrElse("Jaws"), jars)
        val sContext = new SparkContext(sparkConf)
        sContext.addSparkListener(new LoggingListener(dals, userId))

        val hContext = new HiveContextWrapper(sContext)
        HiveUtils.setSharkProperties(hContext, this.getClass.getClassLoader.getResourceAsStream("sharkSettings.txt"))
        //make sure that lazy variable hiveConf gets initialized
        hContext.runMetadataSql("use default")
        hContext
      }
      hContext
    }

    private def impersonateUser(userId: String, parquetInit: Boolean) : HiveContextWrapper = {
      val hiveContext = contextCache.getIfPresent(userId)
      Option(hiveContext) match {
        case Some(value) => value
        case _ =>
          //This user has no context - we have to create one
          val ugi = UserGroupInformation.createProxyUser(userId, realUgi)
          try {
            ugi.doAs(new PrivilegedExceptionAction[HiveContextWrapper]() {
              override def run(): HiveContextWrapper = {
                  val hContext = createHiveContext(userId)
                  contextCache.put(userId, hContext)
                  if (parquetInit) {
                    initializeParquetTables(userId, hContext)
                  }
                  hContext
              }
            })
          } catch {
            case e: Exception =>
              // Hadoop's AuthorizationException suppresses the exception's stack trace, which
              // makes the message printed to the output by the JVM not very helpful. Instead,
              // detect exceptions with empty stack traces here, and treat them differently.
              if (e.getStackTrace.isEmpty) {
                System.err.println(s"ERROR: ${e.getClass.getName}: ${e.getMessage}")
                System.exit(1)
              }
              null
          }
      }
    }

    /**
      * Initialize the parquet tables. Each time a new user is first time impersonated the parquet files,
      * that are used as tables, are registered as temporary table.
      */
    private def initializeParquetTables(userId: String, hiveContext: HiveContextWrapper) {
      Configuration.log4j.info("Initializing parquet tables on the current spark context for user " + userId)
      val parquetTables = dals.parquetTableDal.listParquetTables(userId)

      parquetTables.foreach(pTable => {
        val newConf = new org.apache.hadoop.conf.Configuration(hdfsConf)
        newConf.set("fs.defaultFS", pTable.namenode)
        if (Utils.checkFileExistence(pTable.filePath, newConf)) {
          //register the parquet table
          val registerTableFuture = future {
            val (namenode, folderPath) = if (pTable.namenode.isEmpty) HiveUtils.splitPath(pTable.filePath)
            else (pTable.namenode, pTable.filePath)
            HiveUtils.registerParquetTable(hiveContext, pTable.name, namenode, folderPath, dals, userId)
          }

          // When registering is complete display the proper message or in case of failure delete the table.
          registerTableFuture onComplete {
            case Success(_) =>  Configuration.log4j.info(s"Table ${pTable.name} was registered")
            case Failure(e) => val sw = new StringWriter
              e.printStackTrace(new PrintWriter(sw))
              Configuration.log4j.warn(s"The table ${pTable.name} at path ${pTable.filePath} failed during registration " +
                s"with the following stack trace : \n ${sw.toString}\n The table will be deleted!")
              dals.parquetTableDal.deleteParquetTable(pTable.name, userId)
          }

        } else {
          Configuration.log4j.warn(s"The table ${pTable.name} doesn't exists at path ${pTable.filePath}. The table will be deleted")
          dals.parquetTableDal.deleteParquetTable(pTable.name, userId)
        }
      })
    }

    private def getFields(tableName: String, hiveContext: HiveContextWrapper): Table = {
      val tableSchemaRDD = hiveContext.table(tableName)
      val schema = CustomConverter.getCustomSchema(tableSchemaRDD.schema)

      Table(tableName, schema, Array.empty)
    }

    private def writeLaunchStatus(uuid: String, script: String, userId: String) {
      HiveUtils.logMessage(uuid, s"Launching task for $uuid", "sparksql", dals.loggingDal, userId)
      dals.loggingDal.setState(uuid, QueryState.IN_PROGRESS, userId)
      dals.loggingDal.setScriptDetails(uuid, script, userId)
    }

    private def getOffsetAndLimit(message: GetResultsMessage): (Int, Int) = {
      var offset = message.offset
      var limit = message.limit

      Option(offset) match {
        case None =>
          Configuration.log4j.info("[GetResultsMessage]: offset null... setting it on 0")
          offset = 0
        case _ =>  Configuration.log4j.info("[GetResultsMessage]: offset = " + offset)

      }

      Option(limit) match {
        case None =>
          Configuration.log4j.info("[GetResultsMessage]: limit null... setting it on 100")
          limit = 100
        case _ => Configuration.log4j.info("[GetResultsMessage]: limit = " + limit)

      }
      (offset, limit)
    }

    private def getDBAvroResults(queryID: String, offset: Int, limit: Int, userId: String) = {
      val result = dals.resultsDal.getAvroResults(queryID, userId)
      val lastResultIndex = if (limit > result.result.length) result.result.length else limit
      new AvroResult(result.schema, result.result.slice(offset, lastResultIndex))
    }

    private def getCustomResults(queryID: String, offset: Int, limit: Int, userId: String) = {
      val result = dals.resultsDal.getCustomResults(queryID, userId)
      val lastResultIndex = if (limit > result.result.length) result.result.length else limit
      new CustomResult(result.schema, result.result.slice(offset, lastResultIndex))
    }

    private def getFormattedResult(format: String, resultsConverter: ResultsConverter) = {
      format match {
        case AVRO_BINARY_FORMAT => resultsConverter.toAvroBinaryResults
        case AVRO_JSON_FORMAT   => resultsConverter.toAvroResults().result
        case _                  => resultsConverter.toCustomResults
      }
    }

    private def getResults(offset: Int, limit: Int, destinationPath: String,
                           hdfsConf: org.apache.hadoop.conf.Configuration, queryID: String,
                           hiveContext: HiveContextWrapper): ResultsConverter = {

      val schemaBytes = Utils.readBytes(hdfsConf, Configuration.schemaFolder.getOrElse("jawsSchemaFolder") + "/" + queryID)
      val schema = HiveUtils.deserializaSchema(schemaBytes)
      val resultsRDD: RDD[Tuple2[Object, Array[Object]]] = hiveContext.sparkContext.objectFile(HiveUtils.getRddDestinationPath(queryID, destinationPath))
      val filteredResults = resultsRDD.filter(tuple => tuple._1.asInstanceOf[Long] >= offset && tuple._1.asInstanceOf[Long] < offset + limit).collect()
      val resultRows = filteredResults map { case (index, row) => Row.fromSeq(row) }
      new ResultsConverter(schema, resultRows)
    }

    /**
      * Checks the file existence on the sent file system. If the file is not found an exception is thrown
      * @param hdfsConfiguration the hdfs configuration
      * @param defaultFSUrl the file system default path. It is different for hdfs and for tachyon.
      * @param filePath the path for the file for which the existence is checked
      */
    private def checkFileExistence(hdfsConfiguration: org.apache.hadoop.conf.Configuration, defaultFSUrl:String, filePath:String) = {
      val newConf = new org.apache.hadoop.conf.Configuration(hdfsConfiguration)
      newConf.set("fs.defaultFS", defaultFSUrl)
      if (!Utils.checkFileExistence(defaultFSUrl + filePath, newConf)) {
        throw new Exception("File path does not exist")
      }
    }

    private def getDatabases (hiveContext: HiveContextWrapper) = {
      val metadataQueryResult = HiveUtils.runMetadataCmd(hiveContext, "show databases").flatten
      new Databases(metadataQueryResult)
    }

    private def describeTable(database: String, table: String, isExtended: DescriptionType,
                              hiveContext: HiveContextWrapper): Table = {
      Configuration.log4j.info(s"[HiveActor]: describing table $table from database $database")
      HiveUtils.runMetadataCmd(hiveContext, s"use $database")

      val cmd = isExtended match {
        case _: Extended  => s"describe extended $table"
        case _: Formatted => s"describe formatted $table"
        case _            => s"describe $table"
      }

      val describedTable = HiveUtils.runMetadataCmd(hiveContext, cmd)
      val described = if (isExtended.isInstanceOf[Formatted]) describedTable.drop(2) else describedTable

      val (columnsResult, extraInfoResult) = described.span { arr => !arr.sameElements(Array("", "", "")) }
      val columns = columnsResult map (arr => Column(arr(0), arr(1), arr(2), Array.empty))
      val extraInfo = if (extraInfoResult.isEmpty) extraInfoResult else extraInfoResult.tail
      Table(table, columns, extraInfo)
    }

    private def getTablesForDatabase(database: String, isExtended: DescriptionType, describe: Boolean,
                                     hiveContext: HiveContextWrapper): Tables = {
      Configuration.log4j.info(s"[HiveActor]: showing tables for database $database, describe = $describe")

      HiveUtils.runMetadataCmd(hiveContext, s"use $database")
      val tablesResult = HiveUtils.runMetadataCmd(hiveContext, "show tables")
      val tables = tablesResult map (arr => describe match {
        case true => describeTable(database, arr(0), isExtended, hiveContext)
        case _    => Table(arr(0), Array.empty, Array.empty)
      })

      Tables(database, tables)
    }
}
