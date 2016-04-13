package com.xpatterns.jaws.data.impl

import com.xpatterns.jaws.data.contracts.TJawsResults
import org.apache.log4j.Logger
import org.apache.hadoop.conf.Configuration
import net.liftweb.json._
import com.xpatterns.jaws.data.utils.Utils
import com.xpatterns.jaws.data.DTO.AvroResult
import com.xpatterns.jaws.data.DTO.CustomResult
import org.apache.avro.Schema
import com.google.gson.GsonBuilder

class JawsHdfsResults(configuration: Configuration) extends TJawsResults {

  val logger = Logger.getLogger("JawsHdfsResults")
  val forcedMode = configuration.getBoolean(Utils.FORCED_MODE, false)
  Utils.createFolderIfDoesntExist(configuration, configuration.get(Utils.RESULTS_FOLDER), forcedMode)
  Utils.createFolderIfDoesntExist(configuration, s"${configuration.get(Utils.RESULTS_FOLDER)}/avro", forcedMode)
  Utils.createFolderIfDoesntExist(configuration, s"${configuration.get(Utils.RESULTS_FOLDER)}/custom", forcedMode)

  implicit val formats = DefaultFormats
  def setAvroResults(uuid: String, avroResults: AvroResult, userId: String) {
    logger.debug("Writing avro results to query " + uuid + " and user " + userId)

    val (schemaFile, resultsFile) = getAvroResultsFilePaths(uuid)
    Utils.rewriteFile(avroResults.schema.toString(), configuration, schemaFile)
    val bytesR = avroResults.serializeResult()
    Utils.rewriteFile(bytesR, configuration, resultsFile)
  }

  def getAvroResults(uuid: String, userId: String): AvroResult = {
    logger.debug("Reading results for query: " + uuid + " and user " + userId)
    val (schemaFile, resultsFile) = getAvroResultsFilePaths(uuid)
    if (Utils.checkFileExistence(schemaFile, configuration) && Utils.checkFileExistence(resultsFile, configuration)) {
      val schemaParser = new Schema.Parser()
      val schema = schemaParser.parse(Utils.readFile(configuration, schemaFile))
      val results = Utils.readBytes(configuration, resultsFile)
      new AvroResult(schema, AvroResult.deserializeResult(results, schema))
    } else new AvroResult()
  }

  def setCustomResults(uuid: String, results: CustomResult, userId: String) {
    logger.debug("Writing custom results to query " + uuid + " and user " + userId)
    val customFile = getCustomResultsFilePaths(uuid)
    val gson = new GsonBuilder().create()
    Utils.rewriteFile(gson.toJson(results), configuration, customFile)
  }
  def getCustomResults(uuid: String, userId: String): CustomResult = {
    logger.debug("Reading custom results for query: " + uuid + " and user " + userId)
    val customFile = getCustomResultsFilePaths(uuid)
    if (Utils.checkFileExistence(customFile, configuration)) {
      val gson = new GsonBuilder().create()
      gson.fromJson(Utils.readFile(configuration, customFile), classOf[CustomResult])
    } else new CustomResult()
  }

  def deleteResults(uuid: String, userId: String) {
    logger.debug(s"Deleting results for query $uuid and user $userId")
    val (schemaFile, resultsFile) = getAvroResultsFilePaths(uuid)
    val customFile = getCustomResultsFilePaths(uuid)
    Utils.deleteFile(configuration, schemaFile)
    Utils.deleteFile(configuration, resultsFile)
    Utils.deleteFile(configuration, customFile)

  }

  def getResultsFilePath(queryId: String): String = {
    s"${configuration.get(Utils.RESULTS_FOLDER)}/$queryId"
  }

  def getAvroResultsFilePaths(queryId: String): Tuple2[String, String] = {
    val route = s"${configuration.get(Utils.RESULTS_FOLDER)}/avro/${queryId}_"
    (s"${route}schema", s"${route}results")
  }

  def getCustomResultsFilePaths(queryId: String) = {
    s"${configuration.get(Utils.RESULTS_FOLDER)}/custom/$queryId"
  }
}