package com.xpatterns.jaws.data.impl

import org.scalatest.{ BeforeAndAfter, FunSuite }
import com.typesafe.config.ConfigFactory
import com.xpatterns.jaws.data.utils.{ Randomizer, Utils }
import com.xpatterns.jaws.data.contracts.TJawsResults
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import com.xpatterns.jaws.data.DTO.AvroResult
import com.xpatterns.jaws.data.DTO.CustomResult

/**
  * Created by emaorhian on 7/28/14.
  */
@RunWith(classOf[JUnitRunner])
class JawsResultsOnHdfsTest extends FunSuite with BeforeAndAfter {

  var resultsDal: TJawsResults = _

  before {
    if (resultsDal == null) {

      val conf = ConfigFactory.load

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
      val queryPublishedFolder = Option(hadoopConf.getString("queryPublishedFolder"))
      val queryUnpublishedFolder = Option(hadoopConf.getString("queryUnpublishedFolder"))
      val namenode = Option(hadoopConf.getString("namenode"))

      val configuration = new org.apache.hadoop.conf.Configuration()
      configuration.setBoolean(Utils.FORCED_MODE, forcedMode.getOrElse("false").toBoolean)

      // set hadoop name node and job tracker
      namenode match {
        case None => throw new RuntimeException("You need to set the namenode! ")
        case _ => configuration.set("fs.defaultFS", namenode.get)

      }

      configuration.set("dfs.replication", replicationFactor.getOrElse("1"))

      configuration.set(Utils.LOGGING_FOLDER, loggingFolder.getOrElse("jawsLogs"))
      configuration.set(Utils.STATUS_FOLDER, stateFolder.getOrElse("jawsStates"))
      configuration.set(Utils.DETAILS_FOLDER, detailsFolder.getOrElse("jawsDetails"))
      configuration.set(Utils.METAINFO_FOLDER, metaInfoFolder.getOrElse("jawsMetainfoFolder"))
      configuration.set(Utils.QUERY_NAME_FOLDER, queryNameFolder.getOrElse("jawsQueryNameFolder"))
      configuration.set(Utils.QUERY_PUBLISHED_FOLDER, queryPublishedFolder.getOrElse("jawsQueryPublishedFolder"))
      configuration.set(Utils.QUERY_UNPUBLISHED_FOLDER, queryUnpublishedFolder.getOrElse("jawsQueryUnpublishedFolder"))
      configuration.set(Utils.RESULTS_FOLDER, resultsFolder.getOrElse("jawsResultsFolder"))
      resultsDal = new JawsHdfsResults(configuration)
    }

    resultsDal
  }

  test("testWriteReadResults") {
    val uuid = Randomizer.getRandomString(10)
    val resultsConverter = Randomizer.getResultsConverter
    resultsDal.setResults(uuid, resultsConverter, "userTest")

    val avroResults = resultsDal.getAvroResults(uuid, "userTest")
    val customResults = resultsDal.getCustomResults(uuid, "userTest")

    assert(resultsConverter.toAvroResults() === avroResults)
    assert(resultsConverter.toCustomResults === customResults)

  }

  test("testDeleteResults") {
    val uuid = Randomizer.getRandomString(10)
    val resultsConverter = Randomizer.getResultsConverter
    resultsDal.setResults(uuid, resultsConverter, "userTest")

    val avroResults = resultsDal.getAvroResults(uuid, "userTest")
    val customResults = resultsDal.getCustomResults(uuid, "userTest")

    resultsDal.deleteResults(uuid, "userTest")

    val avroResultsDeleted = resultsDal.getAvroResults(uuid, "userTest")
    val customResultsDeleted = resultsDal.getCustomResults(uuid, "userTest")

    assert(resultsConverter.toAvroResults() === avroResults)
    assert(resultsConverter.toCustomResults === customResults)
    assert(new AvroResult() === avroResultsDeleted)
    assert(new CustomResult() === customResultsDeleted)

  }

}