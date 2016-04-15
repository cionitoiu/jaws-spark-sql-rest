package server.api

import apiactors.ActorsPaths._
import apiactors._
import com.xpatterns.jaws.data.utils.GsonHelper._
import server.Configuration
import server.MainActors._
import spray.httpx.marshalling.Marshaller
import scala.concurrent.ExecutionContext.Implicits.global
import akka.actor.Props
import akka.pattern.ask
import customs.CORSDirectives
import messages._
import com.xpatterns.jaws.data.DTO._
import spray.http._
import spray.httpx.SprayJsonSupport._
import spray.httpx.marshalling.ToResponseMarshallable.isMarshallable
import spray.json.DefaultJsonProtocol._
import spray.routing.Directive.pimpApply
import scala.util.Try
import scala.collection.mutable.ListBuffer
import customs.CustomDirectives._

/**
 * Handles the query management api
 */
trait QueryManagementApi extends BaseApi with CORSDirectives {
  // Actor used for getting results for a query
  //lazy val getResultsActor = createActor(Props(new GetResultsApiActor(hiveContext, dals)), GET_RESULTS_ACTOR_NAME, localSupervisor)

  // Actor used for getting logs for a query
  lazy val getLogsActor = createActor(Props(new GetLogsApiActor(dals)), GET_LOGS_ACTOR_NAME, localSupervisor)

  // Actor used for deleting queries
  lazy val deleteQueryActor = createActor(Props(new DeleteQueryApiActor(dals)), DELETE_QUERY_ACTOR_NAME, localSupervisor)

  // Actor used for getting information about queries
  lazy val getQueriesActor = createActor(Props(new GetQueriesApiActor(dals)), GET_QUERIES_ACTOR_NAME, localSupervisor)

  // Actor used for updating information about queries
  lazy val queryPropertiesApiActor = createActor(Props(new QueryPropertiesApiActor(dals)), QUERY_NAME_ACTOR_NAME, localSupervisor)

  /**
   * Manages the calls about queries management. It handles the following type of calls:
   * <ul>
   *   <li><b>/jaws/run</b> - used for running queries</li>
   *   <li><b>/jaws/logs</b> - used for getting logs generated by execution of queries</li>
   *   <li><b>/jaws/results</b> - used for getting results of a query</li>
   *   <li><b>/jaws/queries</b> - used for getting the list of queries</li>
   *   <li><b>/jaws/cancel</b> - used for canceling a query </li>
   * </ul>
   */
  def runLogsResultsQueriesCancelRoute = runRoute ~ logsRoute ~ resultsRoute ~ queriesRoute ~ cancelRoute

  /**
   * Handles the call to <b>POST /jaws/run</b>. This call is executing the sent query as http body.
   * The following parameters are used:
   * <ul>
   *   <li>
   *     <b>limited</b>: if set on <b>true</b> the result of the query is limited to a fixed number specified by
   *    parameter <b>numberOfResults</b>. If the number of results is exceeding the limit, the remaining results are
   *    stored in the set destination.
   *   </li>
   *   <li>
   *     <b>numberOfResults</b>: [not required]: it is taken in consideration only when the <b>limited</b> is <b>true</b>
   *   </li>
   *   <li>
   *     <b>destination</b> [not required, default <b>hdfs</b>]: the persistence layer where the results are stored
   *   </li>
   *   <li>
   *     <b>name</b> [not required]: when this parameter is set, then the query with the sent name is re-executed.
   *   </li>
   * </ul>
   * It returns the query id in string format.
   */
  private def runRoute = path("run") {
    post {
      securityFilter { userId =>
        // Execute the sent query
        parameters('numberOfResults.as[Int] ? 100, 'limited.as[Boolean], 'destination.as[String] ? Configuration.rddDestinationLocation.getOrElse("hdfs")) {
          (numberOfResults, limited, destination) =>
            corsFilter(List(Configuration.corsFilterAllowedHosts.getOrElse("*"))) {
              entity(as[String]) { query: String =>
                validateCondition(query != null && !query.trim.isEmpty, Configuration.SCRIPT_EXCEPTION_MESSAGE, StatusCodes.BadRequest) {
                  respondWithMediaType(MediaTypes.`text/plain`) { ctx =>
                    Configuration.log4j.info(s"The query is limited=$limited and the destination is $destination")
                    val future = ask(/*runScript*/hiveActor, RunScriptMessage(query, limited, numberOfResults,
                                                                      destination.toLowerCase, userId, hdfsConf))
                    future.map {
                      case e: ErrorMessage => ctx.complete(StatusCodes.InternalServerError, e.message)
                      case result: String => ctx.complete(StatusCodes.OK, result)
                    }
                  }
                }
              }
            }
        } ~
          // Execute the query with the sent name
          parameters('name.as[String]) { (queryName) =>
            corsFilter(List(Configuration.corsFilterAllowedHosts.getOrElse("*"))) {
              validateCondition(queryName != null && !queryName.trim.isEmpty, Configuration.QUERY_NAME_MESSAGE, StatusCodes.BadRequest) {
                respondWithMediaType(MediaTypes.`text/plain`) { ctx =>
                  Configuration.log4j.info(s"Running the query with name $queryName")
                  val future = ask(/*runScript*/hiveActor, RunQueryMessage(queryName.trim, userId, hdfsConf))
                  future.map {
                    case e: ErrorMessage => ctx.complete(StatusCodes.InternalServerError, e.message)
                    case result: String => ctx.complete(StatusCodes.OK, result)
                  }
                }
              }
            }
          }
      }
    } ~
      options {
        corsFilter(List(Configuration.corsFilterAllowedHosts.getOrElse("*")), HttpHeaders.`Access-Control-Allow-Methods`(Seq(HttpMethods.OPTIONS, HttpMethods.POST))) {
          complete {
            "OK"
          }
        }
      }
  }

  /**
   * Handles the call to <b>POST /jaws/logs</b>. This call is getting the logs for a query that has been executed.
   * The following parameters are used:
   * <ul>
   *   <li>
   *     <b>queryID</b>: the id of the query for which the logs are returned
   *   </li>
   *   <li>
   *     <b>startTimestamp</b>: start timestamp from where the logs are returned
   *   </li>
   *   <li>
   *     <b>limit</b>: the number of log entries to be returned
   *   </li>
   * </ul>
   * It returns the logs for the query and its status (this information is stored on [[com.xpatterns.jaws.data.DTO.Logs]])
   */
  private def logsRoute = path("logs") {
    get {
      securityFilter { userId =>
        parameters('queryID, 'startTimestamp.as[Long].?, 'limit.as[Int]) { (queryID, startTimestamp, limit) =>
          corsFilter(List(Configuration.corsFilterAllowedHosts.getOrElse("*"))) {
            validateCondition(queryID != null && !queryID.trim.isEmpty, Configuration.UUID_EXCEPTION_MESSAGE, StatusCodes.BadRequest) {
              respondWithMediaType(MediaTypes.`application/json`) { ctx =>
                var timestamp: java.lang.Long = 0L
                if (startTimestamp.isDefined) {
                  timestamp = startTimestamp.get
                }
                val future = ask(getLogsActor, GetLogsMessage(queryID, timestamp, limit, userId))
                future.map {
                  case e: ErrorMessage => ctx.complete(StatusCodes.InternalServerError, e.message)
                  case result: Logs => ctx.complete(StatusCodes.OK, result)
                }
              }
            }
          }
        }
      }
    } ~
      options {
        corsFilter(List(Configuration.corsFilterAllowedHosts.getOrElse("*")), HttpHeaders.`Access-Control-Allow-Methods`(Seq(HttpMethods.OPTIONS, HttpMethods.GET))) {
          complete {
            "OK"
          }
        }
      }
  }

  /**
   * Handles the call to <b>POST /jaws/results</b>. This call is getting the results for a query that has been executed.
   * The following parameters are used:
   * <ul>
   *   <li>
   *     <b>queryID</b>: the id of the query for which the results are returned
   *   </li>
   *   <li>
   *     <b>offset</b>: the starting result entry that is returned
   *   </li>
   *   <li>
   *     <b>limit</b>: the number of log entries to be returned
   *   </li>
   *   <li>
   *     <b>format</b> [not required]: the format used to return the results. Possible values: <b>avrobinary</b>, <b>avrojson</b>,
   *     <b>default</<b>
   *   </li>
   * </ul>
   * It returns the results for the query. The results can be returned in the following formats:
   * [[com.xpatterns.jaws.data.DTO.AvroBinaryResult]], [[com.xpatterns.jaws.data.DTO.AvroResult]],
   * [[com.xpatterns.jaws.data.DTO.CustomResult]]
   */
  private def resultsRoute = path("results") {
    get {
      securityFilter { userId =>
        parameters('queryID, 'offset.as[Int], 'limit.as[Int], 'format ? "default") { (queryID, offset, limit, format) =>
          corsFilter(List(Configuration.corsFilterAllowedHosts.getOrElse("*"))) {
            validateCondition(queryID != null && !queryID.trim.isEmpty,
                              Configuration.UUID_EXCEPTION_MESSAGE, StatusCodes.BadRequest) {
              respondWithMediaType(MediaTypes.`application/json`) { ctx =>

                implicit def customResultMarshaller[T] = Marshaller.delegate[T, String](ContentTypes.`application/json`) {
                  value ⇒ customGson.toJson(value)
                }

                val future = ask(/*getResults*/hiveActor, GetResultsMessage(queryID, offset, limit, format.toLowerCase, userId, hdfsConf))
                future.map {
                  case e: ErrorMessage => ctx.complete(StatusCodes.InternalServerError, e.message)
                  case result: Any => ctx.complete(StatusCodes.OK, result)
                }
              }
            }
          }
        }
      }
    } ~
      options {
        corsFilter(List(Configuration.corsFilterAllowedHosts.getOrElse("*")),
                   HttpHeaders.`Access-Control-Allow-Methods`(Seq(HttpMethods.OPTIONS, HttpMethods.GET))) {
          complete {
            "OK"
          }
        }
      }
  }

  /**
   * Used to get information about the queries, update the query information or delete queries. The following calls are handled:
   * <ul>
   *   <li>
   *     GET /jaws/queries/published
   *   </li>
   *   <li>
   *     GET /jaws/queries
   *   </li>
   *   <li>
   *     PUT /jaws/queries/{queryID}
   *   </li>
   *   <li>
   *     DELETE /jaws/queries/{queryID}
   *   </li>
   * </ul>
   */
  private def queriesRoute = pathPrefix("queries") {
    queriesPublishedRoute ~ queriesGetRoute ~ queriesPutRoute ~ queriesDeleteRoute ~
      options {
        corsFilter(List(Configuration.corsFilterAllowedHosts.getOrElse("*")),
                        HttpHeaders.`Access-Control-Allow-Methods`(Seq(HttpMethods.OPTIONS, HttpMethods.GET,
                                                                       HttpMethods.DELETE))) {
          complete {
            "OK"
          }
        }
      }
  }

  /**
   * Handles the call <b>GET /jaws/queries/published</b>. It is used for getting the published queries.
   * It returns an array with the name of published queries.
   */
  private def queriesPublishedRoute = path("published") {
    get {
      securityFilter { userId =>
        corsFilter(List(Configuration.corsFilterAllowedHosts.getOrElse("*"))) {
          respondWithMediaType(MediaTypes.`application/json`) { ctx =>
            val future = ask(getQueriesActor, GetPublishedQueries(userId))

            future.map {
              case e: ErrorMessage => ctx.complete(StatusCodes.InternalServerError, e.message)
              case result: Array[String] => ctx.complete(StatusCodes.OK, result)
            }
          }
        }
      }
    }
  }

  /**
   * Handles the call <b>GET /jaws/queries</b>. It is used for getting information about queries.
   * The following parameters are used:
   * <ul>
   *   <li>
   *     <b>startQueryID</b> [not required]: the id of the first query from the list that is returned
   *   </li>
   *   <li>
   *     <b>limit</b> [not required]: the number of the returned queries
   *   </li>
   *   <li>
   *     <b>queryID</b> [not required]: the id of the returned query
   *   </li>
   *   <li>
   *     <b>name</b> [not required]: the name of the returned query
   *   </li>
   * </ul>
   * It returns an array with the information about queries. The information is stored in
   * [[com.xpatterns.jaws.data.DTO.Queries]].
   */
  private def queriesGetRoute = pathEnd {
    get {
      securityFilter { userId =>
        parameterSeq { params =>
          corsFilter(List(Configuration.corsFilterAllowedHosts.getOrElse("*"))) {
            respondWithMediaType(MediaTypes.`application/json`) { ctx =>
              var limit: Int = 100
              var startQueryID: String = null
              val queries = ListBuffer[String]()
              var queryName: String = null

              params.foreach {
                case ("limit", value) => limit = Try(value.toInt).getOrElse(100)
                case ("startQueryID", value) => startQueryID = Option(value).orNull
                case ("queryID", value) if value.nonEmpty => queries += value
                case ("name", value) if value.trim.nonEmpty => queryName = value.trim()
                case (key, value) => Configuration.log4j.warn(s"Unknown parameter $key!")
              }

              val future = if (queryName != null && queryName.nonEmpty) {
                ask(getQueriesActor, GetQueriesByName(queryName, userId))
              } else if (queries.isEmpty) {
                ask(getQueriesActor, GetPaginatedQueriesMessage(startQueryID, limit, userId))
              } else {
                ask(getQueriesActor, GetQueriesMessage(queries, userId))
              }

              future.map {
                case e: ErrorMessage => ctx.complete(StatusCodes.InternalServerError, e.message)
                case result: Queries => ctx.complete(StatusCodes.OK, result)
              }
            }
          }
        }
      }
    }
  }

  /**
   * Handles the call <b>PUT /jaws/queries/{queryID}</b>. It is used to update meta information about query.
   * The following parameters are used:
   * <ul>
   *  <li>
   *    <b>queryID</b>: the id of the returned query
   *  </li>
   *  <li>
   *    <b>overwrite</b> [not required, default <b>false</b>: when it set on <b>true</b> and another query has
   *    the same name, then the name is updated otherwise an error is returned
   *  </li>
   * </ul>
   *
   * The JSON data that is sent may contain the following fields:
   * <ul>
   *  <li>
   *    <b>name</b> [not required]: the name of the query. To delete the name of a query this field should be set
   *    on <b>null</b>
   *  </li>
   *  <li>
   *    <b>description</b> [not required]: the description of the query
   *  </li>
   *  <li>
   *    <b>published</b> [not required, default <b>false</b>]: the published state of the query. When the query is
   *    published it can be retrieve using <b>queries/published</b>.
   *  </li>
   * </ul>
   */
  private def queriesPutRoute = put {
    securityFilter { userId =>
      (path(Segment) & entity(as[QueryMetaInfo]) & parameter("overwrite".as[Boolean] ? false)) {
        (queryID, metaInfo, overwrite) =>
          corsFilter(List(Configuration.corsFilterAllowedHosts.getOrElse("*"))) {
            validateCondition(queryID != null && !queryID.trim.isEmpty, Configuration.UUID_EXCEPTION_MESSAGE, StatusCodes.BadRequest) {
              validateCondition(metaInfo != null, Configuration.META_INFO_EXCEPTION_MESSAGE, StatusCodes.BadRequest) {
                respondWithMediaType(MediaTypes.`text/plain`) { ctx =>
                  val future = ask(queryPropertiesApiActor, new UpdateQueryPropertiesMessage(queryID, metaInfo.name,
                    metaInfo.description, metaInfo.published, overwrite, userId))
                  future.map {
                    case e: ErrorMessage => ctx.complete(StatusCodes.InternalServerError, e.message)
                    case message: String => ctx.complete(StatusCodes.OK, message)
                  }
                }
              }
            }
          }
      }
    }
  }

  /**
   * Handles the call <b>DELETE /jaws/queries/{queryID}</b>. It is used to update meta information about query.
   * The following parameter is used:
   * <ul>
   *  <li>
   *    <b>queryID</b>: the id of the returned query
   *  </li>
   * </ul>
   */
  private def queriesDeleteRoute = delete {
    securityFilter { userId =>
      path(Segment) { queryID =>
        corsFilter(List(Configuration.corsFilterAllowedHosts.getOrElse("*"))) {
          validateCondition(queryID != null && !queryID.trim.isEmpty, Configuration.UUID_EXCEPTION_MESSAGE, StatusCodes.BadRequest) {
            respondWithMediaType(MediaTypes.`text/plain`) { ctx =>
              val future = ask(deleteQueryActor, new DeleteQueryMessage(queryID, userId))
              future.map {
                case e: ErrorMessage => ctx.complete(StatusCodes.InternalServerError, e.message)
                case message: String => ctx.complete(StatusCodes.OK, message)
              }
            }
          }
        }
      }
    }
  }

  /**
   * Handles the call to <b>POST /jaws/cancel</b>. This call is cancelling the execution of a query
   * The following parameter is used:
   * <ul>
   *   <li>
   *     <b>queryID</b>: the id of the query that is cancelled
   *   </li>
   * </ul>
   */
  private def cancelRoute = path("cancel") {
    post {
      securityFilter { userId =>
        parameters('queryID.as[String]) { queryID =>
          corsFilter(List(Configuration.corsFilterAllowedHosts.getOrElse("*"))) {
            complete {
              balancerActor ! CancelMessage(queryID, userId)

              Configuration.log4j.info("Cancel message was sent")
              "Cancel message was sent"
            }
          }
        }
      }
    } ~
      options {
        corsFilter(List(Configuration.corsFilterAllowedHosts.getOrElse("*")), HttpHeaders.`Access-Control-Allow-Methods`(Seq(HttpMethods.OPTIONS, HttpMethods.POST))) {
          complete {
            "OK"
          }
        }
      }
  }
}
