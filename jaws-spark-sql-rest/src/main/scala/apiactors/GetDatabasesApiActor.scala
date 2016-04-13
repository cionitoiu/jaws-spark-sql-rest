package apiactors

import akka.actor.Actor
import akka.actor.actorRef2Scala
import com.xpatterns.jaws.data.contracts.DAL
import messages.GetDatabasesMessage
import java.util.UUID
import server.Configuration
import org.apache.spark.sql.hive.HiveUtils
import implementation.HiveContextWrapper
import scala.concurrent._
import ExecutionContext.Implicits.global
import scala.util.{ Success, Failure }
import messages.ErrorMessage
import com.xpatterns.jaws.data.DTO.Databases

/**
 * Created by emaorhian
 */
class GetDatabasesApiActor(hiveContext: HiveContextWrapper, dals: DAL) extends Actor {

  override def receive = {

    case message: GetDatabasesMessage =>
      Configuration.log4j.info("[GetDatabasesApiActor]: showing databases for user " + message.userId)
      val currentSender = sender

      val getDatabasesFuture = future {
        val uuid = System.currentTimeMillis() + UUID.randomUUID().toString
        val metadataQueryResult = HiveUtils.runMetadataCmd(hiveContext, "show databases").flatten
        new Databases(metadataQueryResult)
        
      }

      getDatabasesFuture onComplete {
        case Success(result) => currentSender ! result
        case Failure(e) => currentSender ! ErrorMessage(s"GET databases failed with the following message: ${e.getMessage}")
      }
  }
}
