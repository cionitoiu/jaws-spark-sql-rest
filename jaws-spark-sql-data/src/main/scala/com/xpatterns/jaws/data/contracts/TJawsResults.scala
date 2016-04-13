package com.xpatterns.jaws.data.contracts

import com.xpatterns.jaws.data.utils.Utils
import com.xpatterns.jaws.data.utils.ResultsConverter
import com.xpatterns.jaws.data.DTO.AvroResult
import com.xpatterns.jaws.data.DTO.CustomResult

/**
 * Created by emaorhian
 */
trait TJawsResults {
  def setAvroResults (uuid: String, avroResults : AvroResult, userId: String)
  def getAvroResults(uuid: String, userId: String) : AvroResult
  def setCustomResults(uuid: String, results: CustomResult, userId: String)
  def getCustomResults(uuid: String, userId: String): CustomResult

  def setResults(uuid: String, results: ResultsConverter, userId: String) {
    Utils.TryWithRetry {

      setAvroResults(uuid, results.toAvroResults(), userId)
      setCustomResults(uuid, results.toCustomResults, userId)
    }
  }
  def deleteResults(uuid: String, userId: String)
}