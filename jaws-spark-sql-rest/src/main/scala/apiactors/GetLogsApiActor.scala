package apiactors

import akka.actor.Actor
import akka.actor.actorRef2Scala
import com.xpatterns.jaws.data.contracts.DAL
import messages.GetLogsMessage
import org.joda.time.DateTime
import server.Configuration
import scala.concurrent._
import ExecutionContext.Implicits.global
import scala.util.{ Success, Failure }
import messages.ErrorMessage

/**
 * Created by emaorhian
 */
class GetLogsApiActor(dals: DAL) extends Actor {

  override def receive = {

    case message: GetLogsMessage =>
      Configuration.log4j.info("[GetLogsApiActor]: retrieving logs for: " + message.queryID + " for user" + message.userId)
      val currentSender = sender

      val getLogsFuture = future {
        val limit = Option(message.limit) getOrElse 100
        val startDate = Option(message.startDate) getOrElse new DateTime(1977, 1, 1, 1, 1, 1, 1).getMillis

        dals.loggingDal.getLogs(message.queryID, startDate, limit, message.userId)
      }
      getLogsFuture onComplete {
        case Success(result) => currentSender ! result
        case Failure(e) => currentSender ! ErrorMessage(s"GET logs failed with the following message: ${e.getMessage}")
      }
  }
}