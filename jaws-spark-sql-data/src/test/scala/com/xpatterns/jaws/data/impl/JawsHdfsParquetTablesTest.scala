package com.xpatterns.jaws.data.impl

import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import org.scalatest.FunSuite
import org.scalatest.BeforeAndAfter
import com.xpatterns.jaws.data.contracts.TJawsParquetTables
import com.typesafe.config.ConfigFactory
import com.xpatterns.jaws.data.utils.Utils
import com.xpatterns.jaws.data.utils.Randomizer
import com.xpatterns.jaws.data.DTO.ParquetTable

@RunWith(classOf[JUnitRunner])
class JawsHdfsParquetTablesTest extends FunSuite with BeforeAndAfter {


  var pTablesDal: TJawsParquetTables = _

  before {
    if (pTablesDal == null) { val conf = ConfigFactory.load

      val hadoopConf = conf.getConfig("hadoopConf").withFallback(conf)

      //hadoop conf
      val replicationFactor = Option(hadoopConf.getString("replicationFactor"))
      val forcedMode = Option(hadoopConf.getString("forcedMode"))
      val loggingFolder = Option(hadoopConf.getString("loggingFolder"))
      val stateFolder = Option(hadoopConf.getString("stateFolder"))
      val detailsFolder = Option(hadoopConf.getString("detailsFolder"))
      val resultsFolder = Option(hadoopConf.getString("resultsFolder"))
      val metaInfoFolder = Option(hadoopConf.getString("metaInfoFolder"))
      val queryNameFolder = Option(hadoopConf.getString("queryNameFolder"))
      val parquetTablesFolder = Option(hadoopConf.getString("parquetTablesFolder"))
      val namenode = Option(hadoopConf.getString("namenode"))

      val configuration = new org.apache.hadoop.conf.Configuration()
      configuration.setBoolean(Utils.FORCED_MODE, forcedMode.getOrElse("false").toBoolean)

      // set hadoop name node and job tracker
      namenode match {
        case None =>
          throw new RuntimeException("You need to set the namenode! ")
        case _ => configuration.set("fs.defaultFS", namenode.get)

      }

      configuration.set("dfs.replication", replicationFactor.getOrElse("1"))
      configuration.set(Utils.PARQUET_TABLES_FOLDER, parquetTablesFolder.getOrElse("parquetTablesFolder"))

      pTablesDal = new JawsHdfsParquetTables(configuration)
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