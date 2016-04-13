package com.xpatterns.jaws.data.impl

import com.xpatterns.jaws.data.contracts.TJawsLogging
import org.scalatest.BeforeAndAfter
import org.scalatest.FunSuite
import com.xpatterns.jaws.data.utils.Randomizer
import com.typesafe.config.ConfigFactory
import me.prettyprint.cassandra.service.CassandraHostConfigurator
import me.prettyprint.cassandra.service.ThriftCluster
import me.prettyprint.hector.api.factory.HFactory
import me.prettyprint.cassandra.model.AllOneConsistencyLevelPolicy
import org.joda.time.DateTime
import com.xpatterns.jaws.data.utils.QueryState
import com.xpatterns.jaws.data.DTO.Log
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import com.xpatterns.jaws.data.DTO.QueryMetaInfo

@RunWith(classOf[JUnitRunner])
class JawsLoggingTest extends FunSuite with BeforeAndAfter {

  var logingDal: TJawsLogging = _

  before {
    if (logingDal == null) {

      val conf = ConfigFactory.load

      val cassandraConf = conf.getConfig("cassandraConf").withFallback(conf)

      // cassandra configuration
      val cassandraHost = cassandraConf.getString("cassandra.host")
      val cassandraKeyspace = cassandraConf.getString("cassandra.keyspace")
      val cassandraClusterName = cassandraConf.getString("cassandra.cluster.name")

      val cassandraHostConfigurator = new CassandraHostConfigurator(cassandraHost)
      val cluster = new ThriftCluster(cassandraClusterName, cassandraHostConfigurator)
      val keyspace = HFactory.createKeyspace(cassandraKeyspace, cluster, new AllOneConsistencyLevelPolicy)

      logingDal = new JawsCassandraLogging(keyspace)
    }

    logingDal
  }

  test("testWriteReadStatus") {
    val uuid = DateTime.now().getMillis.toString
    logingDal.setState(uuid, QueryState.IN_PROGRESS, "testUser")
    val state1 = logingDal.getState(uuid,"testUser")

    logingDal.setState(uuid, QueryState.DONE, "testUser1")
    val state2 = logingDal.getState(uuid, "testUser1")

    assert(QueryState.IN_PROGRESS === state1)
    assert(QueryState.DONE === state2)

  }

  test("testWriteReadMetaInfo") {
    val uuid = DateTime.now().getMillis.toString
    val metaInfo = Randomizer.createQueryMetainfo
    logingDal.setRunMetaInfo(uuid, metaInfo, "testUser")
    val result = logingDal.getMetaInfo(uuid, "testUser")

    assert(metaInfo === result)
  }

  test("testWriteReadDetails") {
    val uuid = DateTime.now().getMillis.toString
    val details = Randomizer.getRandomString(10)

    logingDal.setScriptDetails(uuid, details, "testUser")
    val resultDetails = logingDal.getScriptDetails(uuid, "testUser")
    assert(details === resultDetails)
  }

  test("testWriteReadLogs") {
    val uuid = DateTime.now().getMillis.toString
    val queryId = Randomizer.getRandomString(5)
    val log1 = Randomizer.getRandomString(300)
    val log2 = Randomizer.getRandomString(300)
    val log3 = Randomizer.getRandomString(300)
    val log4 = Randomizer.getRandomString(300)

    val now = System.currentTimeMillis()
    val logDto = new Log(log1, queryId, now)

    logingDal.addLog(uuid, queryId, now, log1, "testUser")
    var result = logingDal.getLogs(uuid, now, 100, "testUser")
    assert(1 === result.logs.length)
    assert(logDto === result.logs(0))

    logingDal.addLog(uuid, queryId, System.currentTimeMillis() + 100, log2, "testUser")
    logingDal.addLog(uuid, queryId, System.currentTimeMillis() + 200, log3, "testUser")
    logingDal.addLog(uuid, queryId, System.currentTimeMillis() + 300, log4, "testUser")

    result = logingDal.getLogs(uuid, now, 100, "testUser")
    assert(4 === result.logs.length)
    assert(log1 === result.logs(0).log)
    assert(log2 === result.logs(1).log)
    assert(log3 === result.logs(2).log)
    assert(log4 === result.logs(3).log)

    result = logingDal.getLogs(uuid, now, 2, "testUser")
    assert(2 === result.logs.length)
    assert(log1 === result.logs(0).log)
    assert(log2 === result.logs(1).log)

    val result2 = logingDal.getLogs(uuid, result.logs(1).timestamp, 2, "testUser")
    assert(2 === result2.logs.length)
    assert(log2 === result2.logs(0).log)
    assert(log3 === result2.logs(1).log)
  }

  test("testWriteReadStates") {
    val uuid = DateTime.now().getMillis.toString + " - 1"
    Thread.sleep(300)
    val uuid2 = DateTime.now().getMillis.toString + " - 2"
    Thread.sleep(300)
    val uuid3 = DateTime.now().getMillis.toString + " - 3"
    Thread.sleep(300)
    val uuid4 = DateTime.now().getMillis.toString + " - 4"
    Thread.sleep(300)
    val uuid5 = DateTime.now().getMillis.toString + " - 5"
    Thread.sleep(300)
    val uuid6 = DateTime.now().getMillis.toString + " - 6"
    val queryId = Randomizer.getRandomString(5)
    val log = Randomizer.getRandomString(300)
    val now = System.currentTimeMillis()

    logingDal.addLog(uuid, queryId, now, log, "testUser")
    logingDal.addLog(uuid2, queryId, now, log, "testUser")
    logingDal.addLog(uuid3, queryId, now, log, "testUser")

    logingDal.setState(uuid, QueryState.DONE, "testUser")
    logingDal.setState(uuid2, QueryState.IN_PROGRESS, "testUser")
    logingDal.setState(uuid3, QueryState.FAILED, "testUser")
    logingDal.setState(uuid4, QueryState.FAILED, "testUser")
    logingDal.setState(uuid5, QueryState.FAILED, "testUser")
    logingDal.setState(uuid6, QueryState.FAILED, "testUser")

    var stateOfQuery = logingDal.getQueries(null, 3, "testUser")

    assert(3 === stateOfQuery.queries.length)

    assert(uuid6 === stateOfQuery.queries(0).queryID)
    assert(uuid5 === stateOfQuery.queries(1).queryID)
    assert(uuid4 === stateOfQuery.queries(2).queryID)

    stateOfQuery = logingDal.getQueries(uuid4, 3, "testUser")
    System.out.println(stateOfQuery)
    assert(3 === stateOfQuery.queries.length)

    assert(uuid3 === stateOfQuery.queries(0).queryID)
    assert(uuid2 === stateOfQuery.queries(1).queryID)
    assert(uuid === stateOfQuery.queries(2).queryID)

    stateOfQuery = logingDal.getQueries(uuid3, 2, "testUser")

    assert(2 === stateOfQuery.queries.length)

    assert(uuid2 === stateOfQuery.queries(0).queryID)
    assert(uuid === stateOfQuery.queries(1).queryID)

  }

  test("testDeleteQuery") {
    val uuid = DateTime.now().getMillis.toString

    //state
    logingDal.setState(uuid, QueryState.IN_PROGRESS,"testUser")

    //details
    val details = Randomizer.getRandomString(10)
    logingDal.setScriptDetails(uuid, details, "testUser")

    //logs
    val queryId = Randomizer.getRandomString(5)
    val log1 = Randomizer.getRandomString(300)
    val log2 = Randomizer.getRandomString(300)
    val log3 = Randomizer.getRandomString(300)
    val log4 = Randomizer.getRandomString(300)

    val now = System.currentTimeMillis()

    logingDal.addLog(uuid, queryId, now, log1, "testUser")
    logingDal.addLog(uuid, queryId, System.currentTimeMillis() + 100, log2, "testUser")
    logingDal.addLog(uuid, queryId, System.currentTimeMillis() + 200, log3, "testUser")
    logingDal.addLog(uuid, queryId, System.currentTimeMillis() + 300, log4, "testUser")

    //meta info
    val metaInfo = Randomizer.createQueryMetainfo
    logingDal.setRunMetaInfo(uuid, metaInfo, "testUser")

    // read information about query
    val state1 = logingDal.getState(uuid, "testUser")
    val resultDetails = logingDal.getScriptDetails(uuid, "testUser")
    val logs = logingDal.getLogs(uuid, now, 100, "testUser")
    val resultMeta = logingDal.getMetaInfo(uuid, "testUser")

    // delete query
    logingDal.deleteQuery(uuid, "testUser")

    // read information about query after delete
    val stateDeleted = logingDal.getState(uuid, "testUser")
    val resultDetailsDeleted = logingDal.getScriptDetails(uuid, "testUser")
    val logsDeleted = logingDal.getLogs(uuid, now, 100, "testUser")
    val resultMetaDeleted = logingDal.getMetaInfo(uuid, "testUser")


    assert(QueryState.IN_PROGRESS === state1)
    assert(details === resultDetails)
    assert(4 === logs.logs.length)
    assert(metaInfo === resultMeta)

    assert(QueryState.NOT_FOUND === stateDeleted)
    assert("" === resultDetailsDeleted)
    assert(0 === logsDeleted.logs.length)
    assert(new QueryMetaInfo === resultMetaDeleted)

  }

}