package apiactors

import akka.actor.Actor
import com.xpatterns.jaws.data.contracts.DAL
import com.xpatterns.jaws.data.utils.QueryState
import messages.{UpdateQueryPropertiesMessage, ErrorMessage}
import server.Configuration
import scala.concurrent._
import ExecutionContext.Implicits.global

import scala.concurrent._
import scala.util.{Failure, Success}

/**
  * Handles the properties operation on a query
  */
class QueryPropertiesApiActor (dals: DAL) extends Actor {
  override def receive = {
    case message: UpdateQueryPropertiesMessage =>

      Configuration.log4j.info(s"[QueryPropertiesApiActor]: updating query id ${message.queryID} with " +
                               s"name ${message.name} for user ${message.userId}")

      val currentSender = sender()

      val updateQueryFuture = future {
        dals.loggingDal.getState(message.queryID, message.userId) match {
          case QueryState.NOT_FOUND => throw new Exception(s"The query ${message.queryID} for user ${message.userId} " +
                                                           s"was not found. Please provide a valid query id")
          case _ =>
            dals.loggingDal.setQueryProperties(message.queryID,
              message.name,
              message.description,
              message.published,
              message.overwrite,
              message.userId)
            s"Query information for ${message.queryID} has been updated"
        }
      }

      updateQueryFuture onComplete {
        case Success(successfulMessage) => currentSender ! successfulMessage
        case Failure(e) => currentSender ! ErrorMessage(s"Updating query failed with the following message: ${e.getMessage}")
      }
  }
}
