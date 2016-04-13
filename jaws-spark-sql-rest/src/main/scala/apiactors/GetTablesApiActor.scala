package apiactors

import java.util.concurrent.TimeUnit

import messages._
import scala.concurrent.Await
import com.xpatterns.jaws.data.contracts.DAL
import akka.util.Timeout
import server.Configuration
import akka.pattern.ask
import org.apache.spark.sql.hive.HiveUtils
import implementation.HiveContextWrapper
import akka.actor.Actor
import scala.util.{ Success, Failure }
import scala.concurrent._
import ExecutionContext.Implicits.global
import messages.ErrorMessage
import com.xpatterns.jaws.data.DTO.Table
import com.xpatterns.jaws.data.DTO.Databases
import com.xpatterns.jaws.data.DTO.Tables
import com.xpatterns.jaws.data.DTO.Column
/**
 * Created by emaorhian
 */

trait DescriptionType
case class Extended() extends DescriptionType
case class Formatted() extends DescriptionType
case class Regular() extends DescriptionType

class GetTablesApiActor(hiveContext: HiveContextWrapper, dals: DAL) extends Actor {

  val databasesActor = context.actorSelection(ActorsPaths.GET_DATABASES_ACTOR_PATH)
  implicit val timeout = Timeout(Configuration.timeout, TimeUnit.MILLISECONDS)

  def getTablesForDatabase(database: String, isExtended: DescriptionType, describe: Boolean): Tables = {
    Configuration.log4j.info(s"[GetTablesApiActor]: showing tables for database $database, describe = $describe")

    HiveUtils.runMetadataCmd(hiveContext, s"use $database")
    val tablesResult = HiveUtils.runMetadataCmd(hiveContext, "show tables")
    val tables = tablesResult map (arr => describe match {
      case true => describeTable(database, arr(0), isExtended)
      case _    => Table(arr(0), Array.empty, Array.empty)
    })

    Tables(database, tables)
  }

  def describeTable(database: String, table: String, isExtended: DescriptionType): Table = {
    Configuration.log4j.info(s"[GetTablesApiActor]: describing table $table from database $database")
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

  override def receive = {

    case message: GetTablesMessage => {
      val currentSender = sender

      val getTablesFutures = future {
        // if no database is specified, the tables for all databases will be retrieved
        Option(message.database).getOrElse("") match {
          case "" => {
            val future = ask(databasesActor, GetDatabasesMessage("TestUser"))
            val allDatabases = Await.result(future, timeout.duration)

            allDatabases match {
              case e: ErrorMessage   => throw new Exception(e.message)
              case result: Databases => result.databases.map(db => getTablesForDatabase(db, new Regular, message.describe))
            }

          }
          case _ => {
            // if there is a list of tables specified, then
            if (Option(message.tables).getOrElse(Array.empty).isEmpty) {
              Array(getTablesForDatabase(message.database, new Regular, message.describe))

            } else {
              Array(Tables(message.database, message.tables map (table => describeTable(message.database, table, new Regular))))
            }
          }
        }
      }

      getTablesFutures onComplete {
        case Success(result) => currentSender ! result
        case Failure(e)      => currentSender ! ErrorMessage(s"GET tables failed with the following message: ${e.getMessage}")
      }
    }

    case message: GetExtendedTablesMessage => {
      val currentSender = sender
      val getExtendedTablesFuture = future {
        Option(message.tables).getOrElse(Array.empty).isEmpty match {
          case true => Array(getTablesForDatabase(message.database, new Extended, true))
          case _    => Array(Tables(message.database, message.tables map (table => describeTable(message.database, table, new Extended))))
        }
      }

      getExtendedTablesFuture onComplete {
        case Success(result) => currentSender ! result
        case Failure(e)      => currentSender ! ErrorMessage(s"GET extended tables failed with the following message: ${e.getMessage}")
      }
    }

    case message: GetFormattedTablesMessage => {
      val currentSender = sender

      val getFormattedTablesFuture = future {
        Option(message.tables).getOrElse(Array.empty).isEmpty match {
          case true => Array(getTablesForDatabase(message.database, new Formatted, true))
          case _    => Array(Tables(message.database, message.tables map (table => describeTable(message.database, table, new Formatted))))
        }
      }

      getFormattedTablesFuture onComplete {
        case Success(result) => currentSender ! result
        case Failure(e)      => currentSender ! ErrorMessage(s"GET formatted tables failed with the following message: ${e.getMessage}")
      }
    }

  }

}