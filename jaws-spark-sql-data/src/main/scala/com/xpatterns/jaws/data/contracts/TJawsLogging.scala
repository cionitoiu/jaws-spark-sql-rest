package com.xpatterns.jaws.data.contracts

import com.xpatterns.jaws.data.DTO.QueryMetaInfo
import com.xpatterns.jaws.data.DTO.Logs
import com.xpatterns.jaws.data.DTO.Queries
import com.xpatterns.jaws.data.utils.QueryState
import com.xpatterns.jaws.data.utils.Utils
import com.xpatterns.jaws.data.DTO.Query

/**
  * Created by emaorhian
  */
trait TJawsLogging {
  def setState(queryId: String, queryState: QueryState.QueryState, userId: String)
  def setScriptDetails(queryId: String, scriptDetails: String, userId: String)
  def addLog(queryId: String, jobId: String, time: Long, log: String, userId: String)

  def setExecutionTime(queryId: String, executionTime: Long, userId: String): Unit = {
    Utils.TryWithRetry {
      val metaInfo = getMetaInfo(queryId, userId)
      metaInfo.executionTime = executionTime
      setMetaInfo(queryId, metaInfo, userId)
    }
  }

  def setTimestamp(queryId: String, time: Long, userId: String): Unit = {
    Utils.TryWithRetry {
      val metaInfo = getMetaInfo(queryId, userId)
      metaInfo.timestamp = time
      setMetaInfo(queryId, metaInfo, userId)
    }
  }

  def setRunMetaInfo(queryId: String, metainfo: QueryMetaInfo, userId: String) = {
    Utils.TryWithRetry {
      val newMetaInfo = getMetaInfo(queryId, userId)
      newMetaInfo.nrOfResults = metainfo.nrOfResults
      newMetaInfo.maxNrOfResults = metainfo.maxNrOfResults
      newMetaInfo.resultsDestination = metainfo.resultsDestination
      newMetaInfo.isLimited = metainfo.isLimited
      setMetaInfo(queryId, newMetaInfo, userId)
    }
  }

  def setQueryProperties(queryId: String, name: Option[String], description: Option[String], published:Option[Boolean],
                         overwrite: Boolean, userId: String) = {
    Utils.TryWithRetry {
      val metaInfo = getMetaInfo(queryId, userId)

      if (name.isDefined) {
        updateQueryName(queryId, metaInfo, name.get, overwrite, userId)
      }

      if (description.isDefined) {
        metaInfo.description = description
      }

      // When the name of a query is not present, the description and published flags should be removed,
      // because they appear only when a query has a name
      if (metaInfo.name.isEmpty || metaInfo.name.get == null) {
        metaInfo.description = None
        metaInfo.published = None
      } else if (published.isDefined) {
        setQueryPublishedStatus(metaInfo.name.get, metaInfo, published.get, userId)
        metaInfo.published = published
      }

      setMetaInfo(queryId, metaInfo, userId)
    }
  }

  private def updateQueryName(queryId: String, metaInfo: QueryMetaInfo,
                              name: String, overwrite:Boolean, userId: String):Unit = {
    val newQueryName = if (name != null) name.trim() else null

    if (newQueryName != null && newQueryName.isEmpty) {
      return
    }

    if (!overwrite) {
      if (newQueryName != null && getQueriesByName(newQueryName, userId).queries.nonEmpty) {
        // When the query name already exist and the overwrite flag is not set,
        // then the client should be warned about it
        throw new Exception(s"There is already a query with the name $name for user $userId. To overwrite " +
          s"the query name, please send the parameter overwrite set on true")
      }
    } else if (newQueryName != null) {
      // When overwriting the old values, the old queries should have the name and description reset
      val notFoundState = QueryState.NOT_FOUND.toString
      for (query <- getQueriesByName(newQueryName, userId).queries) {
        if (query.state != notFoundState) {
          query.metaInfo.name = None
          query.metaInfo.description = None
          setMetaInfo(query.queryID, query.metaInfo, userId)
        }
      }
    }

    if (metaInfo.name.isDefined && metaInfo.name.get != null) {
      // Delete the old query name
      deleteQueryName(metaInfo.name.get, userId)
      // Remove the old published status of the query from storage
      deleteQueryPublishedStatus(metaInfo.name.get, metaInfo.published, userId)
    }
    metaInfo.name = Some(newQueryName)

    if (newQueryName != null) {
      // Save the query name to be able to search it
      saveQueryName(newQueryName, queryId, userId)

      // Set the default published value
      val published = metaInfo.published.getOrElse(false)
      setQueryPublishedStatus(newQueryName, metaInfo, published, userId)
      metaInfo.published = Some(published)
    }
  }

  def setQueryPublishedStatus(name: String, metaInfo: QueryMetaInfo, published: Boolean, userId: String)
  def deleteQueryPublishedStatus(name: String, published: Option[Boolean], userId: String)

  def setMetaInfo(queryId: String, metainfo: QueryMetaInfo, userId: String)

  def getState(queryId: String, userId: String): QueryState.QueryState
  def getScriptDetails(queryId: String, userId: String): String
  def getLogs(queryId: String, time: Long, limit: Int, userId: String): Logs
  def getMetaInfo(queryId: String, userId: String): QueryMetaInfo

  def getQueries(queryId: String, limit: Int, userId: String): Queries
  def getQueries(queryIds: Seq[String], userId: String): Queries = {
    Utils.TryWithRetry {
      val queryArray = queryIds map (queryId => new Query(getState(queryId, userId).toString,
        queryId, getScriptDetails(queryId, userId), getMetaInfo(queryId, userId))) toArray
      val queries = new Queries(queryArray)
      queries
    }
  }

  def getPublishedQueries(userId: String):Array[String]
  def getQueriesByName(name:String, userId: String):Queries
  def deleteQueryName(name: String, userId: String)
  def saveQueryName(name: String, queryId: String, userId: String)

  def deleteQuery(queryId: String, userId: String)
}