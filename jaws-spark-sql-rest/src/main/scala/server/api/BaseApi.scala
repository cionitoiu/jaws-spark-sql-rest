package server.api

import akka.actor.{Props, ActorRef}
import akka.util.Timeout
import java.util.concurrent.TimeUnit
import apiactors.BalancerActor
import server.Configuration
import server.MainActors._
import apiactors.ActorsPaths._

/**
 * The base trait api. It contains the common data used by the other api classes.
 */
trait BaseApi extends SecurityApi {
  // The default timeout for the futures
  implicit val timeout = Timeout(Configuration.timeout.toInt, TimeUnit.MILLISECONDS)

  // The actor that is handling the parquet tables
  lazy val balancerActor = createActor(Props(classOf[BalancerActor]), BALANCER_ACTOR_NAME, remoteSupervisor)

  /**
   * @param pathType the path type of the requested name node
   * @return the proper namenode path
   */
  protected def getNamenodeFromPathType(pathType:String):String = {
    if ("hdfs".equals(pathType)) {
      Configuration.hdfsNamenodePath
    } else if ("tachyon".equals(pathType)) {
      Configuration.tachyonNamenodePath
    } else {
      ""
    }
  }
}
