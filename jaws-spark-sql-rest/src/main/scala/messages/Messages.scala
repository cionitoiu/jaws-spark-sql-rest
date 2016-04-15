package messages

import implementation.SchemaSettingsFactory.{ StorageType, SourceType }
import org.apache.hadoop.conf.Configuration

/**
 * Created by emaorhian
 */
case class CancelMessage(queryID: String, userId: String) extends Serializable
case class GetDatabasesMessage(userId: String)
case class GetQueriesMessage(queryIDs: Seq[String], userId: String)
case class GetQueriesByName(name: String, userId: String)
case class GetPublishedQueries(userId: String)
case class GetPaginatedQueriesMessage(startQueryID: String, limit: Int, userId: String)
case class GetLogsMessage(queryID: String, startDate: Long, limit: Int, userId: String)
case class GetResultsMessage(queryID: String, offset: Int, limit: Int, format : String, userId: String,
                             hdfsConf:Configuration)
case class GetTablesMessage(database: String, describe: Boolean, tables: Array[String], userId: String)
case class GetExtendedTablesMessage(database: String, tables: Array[String], userId: String)
case class GetFormattedTablesMessage(database: String, tables: Array[String], userId: String)
case class RunQueryMessage(name: String, userId: String, hdfsConf: Configuration)
case class RunScriptMessage(script: String, limited: Boolean, maxNumberOfResults: Long, rddDestination: String,
                            userId: String, hdfsConf:Configuration)
case class RunParquetMessage(script: String, tablePath: String, namenode:String, table: String, limited: Boolean,
                             maxNumberOfResults: Long, rddDestination: String, userId: String, hdfsConf: Configuration)
case class GetDatasourceSchemaMessage(path: String, sourceType: SourceType, storageType: StorageType,
                                      hdfsConf: Configuration, userId: String)
case class ErrorMessage(message: String)
case class DeleteQueryMessage(queryID: String, userId: String)
case class RegisterParquetTableMessage(name: String, path: String, namenode: String, userId: String)
case class UnregisterParquetTableMessage(name: String, userId: String)
case class GetParquetTablesMessage(tables: Array[String], describe: Boolean, userId: String)
case class UpdateQueryPropertiesMessage(queryID:String, name:Option[String], description:Option[String],
                                        published:Option[Boolean], overwrite:Boolean, userId: String)


object ResultFormat {
  val AVRO_BINARY_FORMAT = "avrobinary"
  val AVRO_JSON_FORMAT = "avrojson"
  val DEFAULT_FORMAT = "default"
}
