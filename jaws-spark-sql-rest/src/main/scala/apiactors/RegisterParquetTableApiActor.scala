package apiactors

import implementation.HiveContextWrapper
import com.xpatterns.jaws.data.contracts.DAL
import akka.actor.Actor
import messages.RegisterParquetTableMessage
import server.Configuration
import org.apache.spark.sql.hive.HiveUtils
import org.apache.spark.sql.hive.HiveUtils._
import messages.UnregisterParquetTableMessage
import scala.concurrent._
import ExecutionContext.Implicits.global
import scala.util.{ Success, Failure }
import messages.ErrorMessage

import java.io.{StringWriter, PrintWriter}

class RegisterParquetTableApiActor(hiveContext: HiveContextWrapper, dals: DAL) extends Actor {
  override def receive = {

    case message: RegisterParquetTableMessage =>
      Configuration.log4j.info(s"[RegisterParquetTableApiActor]: registering table ${message.name} at ${message.path} " +
                               s"for user ${message.userId}")
      val currentSender = sender

      val registerTableFuture = future {
        val (namenode, folderPath) = if (message.namenode.isEmpty) HiveUtils.splitPath(message.path)
                                     else (message.namenode, message.path)
        HiveUtils.registerParquetTable(hiveContext, message.name, namenode, folderPath, dals, message.userId)
      }

      registerTableFuture onComplete {
        case Success(_) => currentSender ! s"Table ${message.name} was registered"
        case Failure(e) => val sw = new StringWriter
                           e.printStackTrace(new PrintWriter(sw))
                           currentSender ! ErrorMessage(s"RegisterTable failed with the following message: ${sw.toString}")
      }


    case message: UnregisterParquetTableMessage =>
      Configuration.log4j.info(s"[RegisterParquetTableApiActor]: Unregistering table ${message.name}")
      val currentSender = sender

      val unregisterTableFuture = future {
        // unregister table
    	  hiveContext.unregisterTable(message.name)
        dals.parquetTableDal.deleteParquetTable(message.name, message.userId)
      }

      unregisterTableFuture onComplete {
        case Success(result) => currentSender ! s"Table ${message.name} was unregistered"
        case Failure(e) => currentSender ! ErrorMessage(s"UnregisterTable failed with the following message: ${e.getMessage}")
      }
  }
}