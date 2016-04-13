package com.xpatterns.jaws.data.impl

import com.xpatterns.jaws.data.contracts.TJawsParquetTables
import org.scalatest.FunSuite
import org.scalatest.BeforeAndAfter
import com.typesafe.config.ConfigFactory
import me.prettyprint.cassandra.service.CassandraHostConfigurator
import me.prettyprint.cassandra.service.ThriftCluster
import me.prettyprint.hector.api.factory.HFactory
import me.prettyprint.cassandra.model.AllOneConsistencyLevelPolicy
import com.xpatterns.jaws.data.DTO.ParquetTable
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import com.xpatterns.jaws.data.utils.Randomizer

@RunWith(classOf[JUnitRunner])
class JawsCassandraParquetTablesTest extends FunSuite with BeforeAndAfter {

  var pTablesDal: TJawsParquetTables = _

  before {
    if (pTablesDal == null) {

      val conf = ConfigFactory.load

      val cassandraConf = conf.getConfig("cassandraConf").withFallback(conf)

      // cassandra configuration
      val cassandraHost = cassandraConf.getString("cassandra.host")
      val cassandraKeyspace = cassandraConf.getString("cassandra.keyspace")
      val cassandraClusterName = cassandraConf.getString("cassandra.cluster.name")

      val cassandraHostConfigurator = new CassandraHostConfigurator(cassandraHost)
      val cluster = new ThriftCluster(cassandraClusterName, cassandraHostConfigurator)
      val keyspace = HFactory.createKeyspace(cassandraKeyspace, cluster, new AllOneConsistencyLevelPolicy)

      //!!!!!!! ATTENTION !!!! truncating CF
      cluster.truncate(keyspace.getKeyspaceName, "parquet_tables")

      pTablesDal = new JawsCassandraParquetTables(keyspace)
    }

    pTablesDal
  }

  test("testAddReadTable") {
    val table = Randomizer.getParquetTable

    pTablesDal.addParquetTable(table, "testUser")
    val resultTable = pTablesDal.readParquetTable(table.name, "testUser")
    assert(table === resultTable)
    pTablesDal.deleteParquetTable(table.name, "testUser")

  }

  test("testDeleteTable") {
    val table = Randomizer.getParquetTable

    pTablesDal.addParquetTable(table, "testUser")
    val tableBeforeDeletion = pTablesDal.readParquetTable(table.name, "testUser")
    pTablesDal.deleteParquetTable(table.name, "testUser")
    val tableAfterDeletion = pTablesDal.readParquetTable(table.name, "testUser")

    assert(table === tableBeforeDeletion)
    assert(new ParquetTable === tableAfterDeletion)

  }

  test("testDeleteUnexistingTable") {
    val tName = Randomizer.getRandomString(5)
    pTablesDal.deleteParquetTable(tName, "testUser")
    val tableAfterDeletion = pTablesDal.readParquetTable(tName, "testUser")

    assert(new ParquetTable === tableAfterDeletion)

  }

  test("testTableDoesntExist") {
    val tName = Randomizer.getRandomString(5)
    assert(false === pTablesDal.tableExists(tName, "testUser"))
  }

  test("testTableExists") {
    val table = Randomizer.getParquetTable
    pTablesDal.addParquetTable(table, "testUser")
    assert(true === pTablesDal.tableExists(table.name, "testUser"))
    pTablesDal.deleteParquetTable(table.name, "testUser")
  }

  test("testGetTables Empty") {
    val result = pTablesDal.listParquetTables("testUser")
    assert(false === (result == null))
    assert(0 === result.length)
  }

  test("testGetTables") {
    val tables = Randomizer.getParquetTables(5)
    tables.foreach(table => pTablesDal.addParquetTable(table, "testUser"))
    val result = pTablesDal.listParquetTables("testUser")
    tables.foreach(table => pTablesDal.deleteParquetTable(table.name, "testUser"))

    assert(false === (result == null))
    assert(5 === result.length)
    tables.foreach(table => assert(true === result.contains(table)))
  }
}