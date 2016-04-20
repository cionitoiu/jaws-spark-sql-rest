/*
package api

import java.util.concurrent.TimeUnit

import org.scalatest.FunSuite
import org.scalatest.BeforeAndAfter
import org.scalatest.concurrent._
import server.JawsController
import com.xpatterns.jaws.data.contracts.DAL
import server.Configuration
import com.xpatterns.jaws.data.impl.CassandraDal
import com.xpatterns.jaws.data.impl.HdfsDal
import akka.actor.ActorSystem
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import akka.util.Timeout
import akka.pattern.ask
import com.xpatterns.jaws.data.DTO.Query
import akka.testkit.TestActorRef
import com.xpatterns.jaws.data.utils.QueryState
import java.util.UUID
import com.xpatterns.jaws.data.DTO.QueryMetaInfo
import apiactors.GetQueriesApiActor
import messages.GetQueriesMessage
import com.xpatterns.jaws.data.DTO.Queries

@RunWith(classOf[JUnitRunner])
class GetQueryInfoTest extends FunSuite with BeforeAndAfter with ScalaFutures {

  val hdfsConf = JawsController.getHadoopConf
  var dals: DAL = _

  implicit val timeout = Timeout(10000, TimeUnit.MILLISECONDS)
  implicit val system = ActorSystem("localSystem")

  before {
    Configuration.dalType.getOrElse("cassandra") match {
      case "cassandra" => dals = new CassandraDal(Configuration.cassandraHost.get,
                                                  Configuration.cassandraClusterName.get,
                                                  Configuration.cassandraKeyspace.get)
      case _ => dals = new HdfsDal(hdfsConf)
    }
  }

  // **************** TESTS *********************

  test(" not found ") {

    val tAct = TestActorRef(new GetQueriesApiActor(dals))
    val queryId = System.currentTimeMillis() + UUID.randomUUID().toString
    val f = tAct ? GetQueriesMessage(Seq(queryId), "testUser")
    whenReady(f)(s => s match {
      case queries: Queries => {
    	assert(queries.queries.length === 1)
        assert(queries.queries(0) === new Query("NOT_FOUND", queryId, "", new QueryMetaInfo))
      }
      case _ => fail
    })
  }

  test(" found ") {

    val tAct = TestActorRef(new GetQueriesApiActor(dals))
    val queryId = System.currentTimeMillis() + UUID.randomUUID().toString
    val executionTime = 100L
    val currentTimestamp = System.currentTimeMillis()
    val metaInfo = new QueryMetaInfo(100, 150, 1, true)
    dals.loggingDal.setState(queryId, QueryState.IN_PROGRESS, "testUser")
    dals.loggingDal.setScriptDetails(queryId, "test script", "testUser")
    dals.loggingDal.setExecutionTime(queryId, executionTime, "testUser")
    dals.loggingDal.setTimestamp(queryId, currentTimestamp, "testUser")
    dals.loggingDal.setRunMetaInfo(queryId, metaInfo, "testUser")
    metaInfo.timestamp = currentTimestamp
    metaInfo.executionTime = executionTime

    val f = tAct ? GetQueriesMessage(Seq(queryId), "testUser")
    whenReady(f)(s => s match {
      case queries: Queries =>
    	  assert(queries.queries.length === 1)
        assert(queries.queries(0) === new Query("IN_PROGRESS", queryId, "test script", metaInfo))
      case _ => fail()
    })
    
    dals.loggingDal.deleteQuery(queryId, "testUser")

  }
}*/
