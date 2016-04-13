package apiactors

import akka.actor.Actor
import com.xpatterns.jaws.data.contracts.DAL
import server.Configuration
import scala.concurrent._
import ExecutionContext.Implicits.global
import scala.util.{ Success, Failure }
import messages._
/**
  * Created by emaorhian
  */
class GetQueriesApiActor(dals: DAL) extends Actor {

  override def receive = {

    case message: GetPaginatedQueriesMessage =>

      Configuration.log4j.info("[GetQueriesApiActor]: retrieving " + message.limit +
        " number of queries starting with " + message.startQueryID + " for user" + message.userId)
      val currentSender = sender()
      val getQueriesFuture = future {
        dals.loggingDal.getQueries(message.startQueryID, message.limit, message.userId)
      }

      getQueriesFuture onComplete {
        case Success(result) => currentSender ! result
        case Failure(e) => currentSender ! ErrorMessage(s"GET queries failed with the following message: ${e.getMessage}")
      }

    case message: GetQueriesMessage =>
      Configuration.log4j.info("[GetQueryInfoApiActor]: retrieving the query information for " + message.queryIDs)

      val currentSender = sender()

      val getQueryInfoFuture = future {
        dals.loggingDal.getQueries(message.queryIDs, message.userId)
      }

      getQueryInfoFuture onComplete {
        case Success(result) => currentSender ! result
        case Failure(e) => currentSender ! ErrorMessage(s"GET query info failed with the following message: ${e.getMessage}")
      }

    case message: GetQueriesByName =>
      Configuration.log4j.info("[GetQueryInfoApiActor]: retrieving the queries for " + message.name)

      val currentSender = sender()

      val getQueryInfoFuture = future {
        dals.loggingDal.getQueriesByName(message.name, message.userId)
      }

      getQueryInfoFuture onComplete {
        case Success(result) => currentSender ! result
        case Failure(e) => currentSender ! ErrorMessage(s"GET query info failed with the following message: ${e.getMessage}")
      }

    case message: GetPublishedQueries =>
      Configuration.log4j.info("[GetQueryInfoApiActor]: retrieving the published queries ")

      val currentSender = sender()

      val getQueryInfoFuture = future {
        dals.loggingDal.getPublishedQueries(message.userId)
      }

      getQueryInfoFuture onComplete {
        case Success(result) => currentSender ! result
        case Failure(e) => currentSender ! ErrorMessage(s"GET published queries failed with the following message: ${e.getMessage}")
      }
  }

}