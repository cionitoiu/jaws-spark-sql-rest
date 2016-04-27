package server

import java.net.InetAddress
import org.apache.hadoop.security.UserGroupInformation
import server.api._
import scala.collection.JavaConverters._
import com.typesafe.config.{ConfigFactory, Config}
import com.xpatterns.jaws.data.utils.Utils
import akka.actor.ActorSystem
import customs.CORSDirectives
import com.xpatterns.jaws.data.impl.CassandraDal
import com.xpatterns.jaws.data.impl.HdfsDal
import spray.routing.Directive.pimpApply
import spray.routing.SimpleRoutingApp
import org.apache.hadoop.security.UserGroupInformation
import com.xpatterns.jaws.data.contracts.DAL
import org.apache.spark.sql.hive.HiveUtils
import implementation.HiveContextWrapper
import org.apache.spark.SparkContext
import org.apache.spark.scheduler.LoggingListener
import org.apache.spark.SparkConf

import scala.sys.SystemProperties

/**
 * Created by emaorhian
 */
object JawsController extends App with UIApi with IndexApi with ParquetApi with MetadataApi with QueryManagementApi
  with SimpleRoutingApp with CORSDirectives {
  initialize()

  // initialize parquet tables
  //initializeParquetTables("tempUser") ///ChangeMe!?????

  implicit val spraySystem: ActorSystem = ActorSystem("spraySystem")

  startServer(interface = Configuration.serverInterface.getOrElse(InetAddress.getLocalHost.getHostName),
    port = Configuration.webServicesPort.getOrElse("8080").toInt) {
      pathPrefix("jaws") {
        uiRoute ~ indexRoute ~ runLogsResultsQueriesCancelRoute ~ parquetRoute ~ hiveSchemaRoute
      }
    }

  private val reactiveServer = new ReactiveServer(Configuration.webSocketsPort.getOrElse("8081").toInt, MainActors.logsActor)
  reactiveServer.start()

  def initialize() = {
    Configuration.log4j.info("Initializing...")

    //Kerberos settings and inits
    System.setProperty("java.security.krb5.realm", Configuration.kerberosRealm.get)
    System.setProperty("java.security.krb5.kdc", Configuration.kerberosIP.get)

    val kConf = new org.apache.hadoop.conf.Configuration();
    kConf.set("fs.defaultFS", "hdfs://scdh56-master.staging.xpatterns.com:8020");
    kConf.set("fs.hdfs.impl", "org.apache.hadoop.hdfs.DistributedFileSystem");
    kConf.set("hadoop.security.authentication", "Kerberos");
    hdfsConf = getHadoopConf(kConf)
    UserGroupInformation.setConfiguration(kConf);
    UserGroupInformation.loginUserFromKeytab(Configuration.kerberosPrincipal.get, Configuration.kerberosKeytab.get) ;
    realUgi = UserGroupInformation.getLoginUser


    Utils.createFolderIfDoesntExist(hdfsConf,
                                    Configuration.schemaFolder.getOrElse("jawsSchemaFolder"),
                                    forcedMode = false)

    Configuration.dalType.getOrElse("cassandra") match {
      case "cassandra" => dals = new CassandraDal(Configuration.cassandraHost.get,
                                                  Configuration.cassandraClusterName.get,
                                                  Configuration.cassandraKeyspace.get)
      case _           => dals = new HdfsDal(hdfsConf)
    }

  }


  def getHadoopConf (configuration: org.apache.hadoop.conf.Configuration) : org.apache.hadoop.conf.Configuration = {
    configuration.setBoolean(Utils.FORCED_MODE, Configuration.forcedMode.getOrElse("false").toBoolean)

    configuration.set(Utils.LOGGING_FOLDER, Configuration.loggingFolder.getOrElse("jawsLogs"))
    configuration.set(Utils.STATUS_FOLDER, Configuration.stateFolder.getOrElse("jawsStates"))
    configuration.set(Utils.DETAILS_FOLDER, Configuration.detailsFolder.getOrElse("jawsDetails"))
    configuration.set(Utils.METAINFO_FOLDER, Configuration.metaInfoFolder.getOrElse("jawsMetainfoFolder"))
    configuration.set(Utils.QUERY_NAME_FOLDER, Configuration.queryNameFolder.getOrElse("jawsQueryNameFolder"))
    configuration.set(Utils.QUERY_PUBLISHED_FOLDER, Configuration.queryPublishedFolder.getOrElse("jawsQueryPublishedFolder"))
    configuration.set(Utils.QUERY_UNPUBLISHED_FOLDER, Configuration.queryUnpublishedFolder.getOrElse("jawsQueryUnpublishedFolder"))
    configuration.set(Utils.RESULTS_FOLDER, Configuration.resultsFolder.getOrElse("jawsResultsFolder"))
    configuration.set(Utils.PARQUET_TABLES_FOLDER, Configuration.parquetTablesFolder.getOrElse("parquetTablesFolder"))

    configuration
  }
}
