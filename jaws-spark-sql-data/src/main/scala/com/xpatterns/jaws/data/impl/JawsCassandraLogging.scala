package com.xpatterns.jaws.data.impl

import scala.collection.JavaConverters._
import org.apache.log4j.Logger
import com.xpatterns.jaws.data.DTO.Log
import com.xpatterns.jaws.data.DTO.Logs
import com.xpatterns.jaws.data.DTO.Queries
import com.xpatterns.jaws.data.DTO.Query
import com.xpatterns.jaws.data.contracts.TJawsLogging
import com.xpatterns.jaws.data.utils.{ Utils, QueryState }
import me.prettyprint.cassandra.serializers.CompositeSerializer
import me.prettyprint.cassandra.serializers.IntegerSerializer
import me.prettyprint.cassandra.serializers.LongSerializer
import me.prettyprint.cassandra.serializers.StringSerializer
import me.prettyprint.hector.api.Keyspace
import me.prettyprint.hector.api.Serializer
import me.prettyprint.hector.api.beans.AbstractComposite.ComponentEquality
import me.prettyprint.hector.api.beans.ColumnSlice
import me.prettyprint.hector.api.beans.Composite
import me.prettyprint.hector.api.beans.HColumn
import me.prettyprint.hector.api.factory.HFactory
import me.prettyprint.hector.api.query.QueryResult
import me.prettyprint.hector.api.query.SliceQuery
import net.liftweb.json._
import spray.json._
import me.prettyprint.hector.api.query.MultigetSliceQuery
import me.prettyprint.hector.api.beans.Rows
import com.xpatterns.jaws.data.DTO.QueryMetaInfo

import scala.collection.mutable.ArrayBuffer

class JawsCassandraLogging(keyspace: Keyspace) extends TJawsLogging {

  val CF_SPARK_LOGS = "logs"
  val CF_SPARK_LOGS_NUMBER_OF_ROWS = 100

  // The query names published and unpublished must be kept on a special row to be faster to access them
  val QUERY_NAME_UNPUBLISHED_ROW = CF_SPARK_LOGS_NUMBER_OF_ROWS + 1
  val QUERY_NAME_PUBLISHED_ROW = CF_SPARK_LOGS_NUMBER_OF_ROWS + 2

  val LEVEL_TYPE = 0
  val LEVEL_USERID = 1
  val LEVEL_UUID = 2
  val LEVEL_TIME_STAMP = 3


  val TYPE_QUERY_STATE = -1
  val TYPE_SCRIPT_DETAILS = 0
  val TYPE_LOG = 1
  val TYPE_META = 2
  val TYPE_QUERY_NAME = 3
  val TYPE_QUERY_PUBLISHED_STATE = 4

  val logger = Logger.getLogger("JawsCassandraLogging")

  val is = IntegerSerializer.get.asInstanceOf[Serializer[Int]]
  val ss = StringSerializer.get.asInstanceOf[Serializer[String]]
  val cs = CompositeSerializer.get.asInstanceOf[Serializer[Composite]]
  val ls = LongSerializer.get.asInstanceOf[Serializer[Long]]
  val ints = IntegerSerializer.get.asInstanceOf[Serializer[Integer]]

  override def setState(queryId: String, queryState: QueryState.QueryState, userId: String) {
    Utils.TryWithRetry {

      logger.debug("Writing query state " + queryState.toString + " to query " + queryId + " for user " + userId)

      val key = computeRowKey(queryId + userId)

      val column = new Composite()
      column.setComponent(LEVEL_TYPE, TYPE_QUERY_STATE, is)
      column.setComponent(LEVEL_USERID, userId, ss)
      column.setComponent(LEVEL_UUID, queryId, ss)

      val mutator = HFactory.createMutator(keyspace, is)
      mutator.addInsertion(key, CF_SPARK_LOGS, HFactory.createColumn(column, queryState.toString, cs, ss))
      mutator.execute()
    }
  }

  override def setScriptDetails(queryId: String, scriptDetails: String, userId: String) {
    Utils.TryWithRetry {

      logger.debug("Writing script details " + scriptDetails + " to query " + queryId + " for user " + userId)

      val key = computeRowKey(queryId + userId)

      val column = new Composite()
      column.setComponent(LEVEL_TYPE, TYPE_SCRIPT_DETAILS, is)
      column.setComponent(LEVEL_USERID, userId, ss)
      column.setComponent(LEVEL_UUID, queryId, ss)

      val mutator = HFactory.createMutator(keyspace, is)
      mutator.addInsertion(key, CF_SPARK_LOGS, HFactory.createColumn(column, scriptDetails, cs, ss))
      mutator.execute()
    }
  }

  private def computeRowKey(uuid: String): Int = {
    Math.abs(uuid.hashCode() % CF_SPARK_LOGS_NUMBER_OF_ROWS)
  }

  override def addLog(queryId: String, jobId: String, time: Long, log: String, userId: String) {
    Utils.TryWithRetry {

      logger.debug("Writing log " + log + " to query " + queryId + " at time " + time + " for user " + userId)
      val dto = new Log(log, jobId, time)

      val key = computeRowKey(queryId + userId)

      val column = new Composite()
      column.setComponent(LEVEL_TYPE, TYPE_LOG, is)
      column.setComponent(LEVEL_USERID, userId, ss)
      column.setComponent(LEVEL_UUID, queryId, ss)
      column.setComponent(LEVEL_TIME_STAMP, time, ls)


      val mutator = HFactory.createMutator(keyspace, is)
      mutator.addInsertion(key, CF_SPARK_LOGS, HFactory.createColumn(column, dto.toJson.toString(), cs, StringSerializer.get.asInstanceOf[Serializer[Object]]))
      mutator.execute()
    }
  }

  override def getState(queryId: String, userId: String): QueryState.QueryState = {
    Utils.TryWithRetry {

      logger.debug("Reading query state for query: " + queryId + " for user " + userId)

      val key = computeRowKey(queryId + userId)

      val column = new Composite()
      column.addComponent(LEVEL_TYPE, TYPE_QUERY_STATE, ComponentEquality.EQUAL)
      column.setComponent(LEVEL_USERID, userId, ss)
      column.addComponent(LEVEL_UUID, queryId, ComponentEquality.EQUAL)

      val sliceQuery: SliceQuery[Int, Composite, String] = HFactory.createSliceQuery(keyspace, is, cs, ss)
      sliceQuery.setColumnFamily(CF_SPARK_LOGS).setKey(key).setRange(column, column, false, 1)

      val result: QueryResult[ColumnSlice[Composite, String]] = sliceQuery.execute()

      Option(result) match {
        case None => logger.info("No results found")
        case _ =>
          val columnSlice: ColumnSlice[Composite, String] = result.get()

          Option(columnSlice) match {
            case None => return QueryState.NOT_FOUND
            case _ =>
              Option(columnSlice.getColumns) match {
                case None => return QueryState.NOT_FOUND
                case _ =>
                  if (columnSlice.getColumns.size() == 0) {
                    return QueryState.NOT_FOUND
                  }
                  val col = columnSlice.getColumns.get(0)
                  val state = col.getValue

                  return QueryState.withName(state)
              }
          }
      }

      return QueryState.NOT_FOUND
    }
  }

  override def getScriptDetails(queryId: String, userId: String): String = {
    Utils.TryWithRetry {

      logger.debug("Reading script details for query " + queryId + " and user " + userId)

      val key = computeRowKey(queryId + userId)

      val column = new Composite()
      column.addComponent(LEVEL_TYPE, TYPE_SCRIPT_DETAILS, ComponentEquality.EQUAL)
      column.addComponent(LEVEL_USERID, userId, ComponentEquality.EQUAL)
      column.addComponent(LEVEL_UUID, queryId, ComponentEquality.EQUAL)

      val sliceQuery: SliceQuery[Int, Composite, String] = HFactory.createSliceQuery(keyspace, is, cs, ss)
      sliceQuery.setColumnFamily(CF_SPARK_LOGS).setKey(key).setRange(column, column, false, 1)

      val result: QueryResult[ColumnSlice[Composite, String]] = sliceQuery.execute()
      Option(result) match {
        case None => return ""
        case _ =>
          val columnSlice: ColumnSlice[Composite, String] = result.get()
          Option(columnSlice) match {
            case None => return ""
            case _ =>
              Option(columnSlice.getColumns) match {
                case None => return ""
                case _ =>

                  if (columnSlice.getColumns.size() == 0) {
                    return ""
                  }
                  val col: HColumn[Composite, String] = columnSlice.getColumns.get(0)
                  val description = col.getValue

                  return description
              }
          }
      }
    }
  }

  override def getLogs(queryId: String, time: Long, limit: Int, userId: String): Logs = {
    Utils.TryWithRetry {

      logger.debug("Reading logs for query: " + queryId + " from date: " + time + " for user " + userId)
      var logs = Array[Log]()
      val state = getState(queryId, userId).toString
      val key = computeRowKey(queryId + userId)

      val startColumn = new Composite()
      startColumn.addComponent(LEVEL_TYPE, TYPE_LOG, ComponentEquality.EQUAL)
      startColumn.addComponent(LEVEL_USERID, userId, ComponentEquality.EQUAL)
      startColumn.addComponent(LEVEL_UUID, queryId, ComponentEquality.EQUAL)
      startColumn.addComponent(LEVEL_TIME_STAMP, time, ComponentEquality.EQUAL)

      val endColumn = new Composite()
      endColumn.addComponent(LEVEL_TYPE, TYPE_LOG, ComponentEquality.EQUAL)
      endColumn.addComponent(LEVEL_USERID, userId, ComponentEquality.EQUAL)
      endColumn.addComponent(LEVEL_UUID, queryId, ComponentEquality.GREATER_THAN_EQUAL)
      val sliceQuery: SliceQuery[Int, Composite, String] = HFactory.createSliceQuery(keyspace, is, cs, ss)
      sliceQuery.setColumnFamily(CF_SPARK_LOGS).setKey(key).setRange(startColumn, endColumn, false, limit)

      val result: QueryResult[ColumnSlice[Composite, String]] = sliceQuery.execute()

      Option(result) match {
        case None => return new Logs(logs, state)
        case _ =>
          val columnSlice: ColumnSlice[Composite, String] = result.get()
          Option(columnSlice) match {
            case None => return new Logs(logs, state)
            case _ =>
              Option(columnSlice.getColumns) match {

                case None => return new Logs(logs, state)
                case _ =>
                  if (columnSlice.getColumns.size == 0) {
                    return new Logs(logs, state)
                  }

                  val columns = columnSlice.getColumns.asScala
                  implicit val formats = DefaultFormats
                  columns.foreach(col => {
                    val value = col.getValue
                    val json = parse(value)
                    val log = json.extract[Log]
                    logs = logs ++ Array(log)
                  })
                  new Logs(logs, state)
              }
          }
      }
    }
  }
  override def getQueries(queryId: String, limit: Int, userId: String): Queries = {
    Utils.TryWithRetry {

      var skipFirst = false
      logger.debug("Reading queries states starting with the query " + queryId + " for user " + userId)

      val map = new java.util.TreeMap[String, Query]()

      val keysList: java.util.List[Integer] = new java.util.ArrayList[Integer]()

      for (key <- 0 until CF_SPARK_LOGS_NUMBER_OF_ROWS) {
        keysList.add(key)
      }

      val startColumn = new Composite()
      startColumn.addComponent(LEVEL_TYPE, TYPE_QUERY_STATE, ComponentEquality.EQUAL)
      startColumn.addComponent(LEVEL_USERID, userId, ComponentEquality.EQUAL)
      if (queryId != null && !queryId.isEmpty) {
        startColumn.addComponent(LEVEL_UUID, queryId, ComponentEquality.EQUAL)
      }


      val endColumn = new Composite()
      if (queryId != null && !queryId.isEmpty) {
        endColumn.addComponent(LEVEL_TYPE, TYPE_QUERY_STATE, ComponentEquality.LESS_THAN_EQUAL)
      } else {
        endColumn.addComponent(LEVEL_TYPE, TYPE_QUERY_STATE, ComponentEquality.GREATER_THAN_EQUAL)
      }
      startColumn.addComponent(LEVEL_USERID, userId, ComponentEquality.EQUAL)

      val multiSliceQuery: MultigetSliceQuery[Integer, Composite, String] = HFactory.createMultigetSliceQuery(keyspace, ints, cs, ss)
      if (queryId != null && !queryId.isEmpty) {
        multiSliceQuery.setColumnFamily(CF_SPARK_LOGS).setKeys(keysList).setRange(startColumn, endColumn, true, limit + 1)
        skipFirst = true
      } else {
        multiSliceQuery.setColumnFamily(CF_SPARK_LOGS).setKeys(keysList).setRange(endColumn, startColumn, true, limit)
      }

      val result: QueryResult[Rows[Integer, Composite, String]] = multiSliceQuery.execute()
      val rows = result.get()
      if (rows == null || rows.getCount == 0) {
        return new Queries(Array[Query]())
      }

      val rrows = rows.asScala

      rrows.foreach(row => {

        val columnSlice = row.getColumnSlice
        if (columnSlice == null || columnSlice.getColumns == null || columnSlice.getColumns.size == 0) {

        } else {
          val columns = columnSlice.getColumns.asScala
          columns.foreach(column => {
            val name = column.getName
            if (name.get(LEVEL_TYPE, is) == TYPE_QUERY_STATE) {
              val queryId = name.get(LEVEL_UUID, ss)
              val query = new Query(column.getValue, queryId, getScriptDetails(queryId, userId),getMetaInfo(queryId, userId))
              map.put(name.get(LEVEL_UUID, ss), query)
            }
          })
        }
      })

      return Queries(getCollectionFromSortedMapWithLimit(map, limit, skipFirst))
    }
  }

  def getCollectionFromSortedMapWithLimit(map: java.util.TreeMap[String, Query],
                                          limit: Int,
                                          skipFirst: Boolean): Array[Query] = {

    var collection = Array[Query]()
    val iterator = map.descendingKeySet().iterator()
    var skipFirstMutable = skipFirst
    var limitMutable = limit

    while (iterator.hasNext && limitMutable > 0) {
      if (skipFirstMutable) {
        skipFirstMutable = false
        iterator.next()
      } else {
        collection = collection ++ Array(map.get(iterator.next()))
        limitMutable = limitMutable - 1
      }
    }

    collection
  }

  override def getQueriesByName(name:String, userId: String):Queries = {
    Utils.TryWithRetry {
      logger.debug(s"Reading queries states for queries with name $name for user $userId")

      val key = computeRowKey(name + userId)

      val column = new Composite()
      column.addComponent(LEVEL_TYPE, TYPE_QUERY_NAME, ComponentEquality.EQUAL)
      column.addComponent(LEVEL_USERID, userId, ComponentEquality.EQUAL)
      column.addComponent(LEVEL_UUID, name, ComponentEquality.EQUAL)

      val columnQuery = HFactory.createColumnQuery(keyspace, is, cs, ss)
      columnQuery.setColumnFamily(CF_SPARK_LOGS).setKey(key).setName(column)

      val result = columnQuery.execute()
      if (result != null) {
        val col = result.get()
        if (col != null) {
          val queryID = col.getValue
          if (queryID != null) {
            return getQueries(List(queryID), userId)
          }
        }
      }
      new Queries(Array[Query]())
    }
  }

  override def setMetaInfo(queryId: String, metainfo: QueryMetaInfo, userId: String) {
    Utils.TryWithRetry {
      logger.debug("Writing script meta info " + metainfo.toJson + " to query " + queryId + " for user " + userId)

      val key = computeRowKey(queryId + userId)

      val column = new Composite()
      column.setComponent(LEVEL_TYPE, TYPE_META, is)
      column.setComponent(LEVEL_USERID, userId, ss)
      column.setComponent(LEVEL_UUID, queryId, ss)

      val value = metainfo.toJson.toString()

      val mutator = HFactory.createMutator(keyspace, is)

      mutator.addInsertion(key, CF_SPARK_LOGS, HFactory.createColumn(column, value, cs, ss))
      mutator.execute()
    }
  }

  override def getMetaInfo(queryId: String, userId: String): QueryMetaInfo = {
    Utils.TryWithRetry {

      logger.debug("Reading meta info for for query: " + queryId + " for user " + userId)

      val key = computeRowKey(queryId + userId)

      val column = new Composite()
      column.addComponent(LEVEL_TYPE, TYPE_META, ComponentEquality.EQUAL)
      column.addComponent(LEVEL_USERID, userId, ComponentEquality.EQUAL)
      column.addComponent(LEVEL_UUID, queryId, ComponentEquality.EQUAL)

      val columnQuery = HFactory.createColumnQuery(keyspace, is, cs, ss)
      columnQuery.setColumnFamily(CF_SPARK_LOGS).setKey(key).setName(column)

      val result = columnQuery.execute()
      if (result != null) {
        val col = result.get()

        if (col == null) {
          return new QueryMetaInfo()
        }

        val json = col.getValue.parseJson
        return json.convertTo[QueryMetaInfo]
      }

      return new QueryMetaInfo()
    }
  }

  override def saveQueryName(name: String, queryId: String, userId: String): Unit = {
    Utils.TryWithRetry {
      logger.debug("Saving query name " + name + " to query " + queryId + " for user " + userId)

      val key = computeRowKey(name)

      val column = new Composite()
      column.setComponent(LEVEL_TYPE, TYPE_QUERY_NAME, is)
      column.setComponent(LEVEL_USERID, userId, ss)
      column.setComponent(LEVEL_UUID, name, ss)

      val value = queryId

      val mutator = HFactory.createMutator(keyspace, is)

      mutator.addInsertion(key, CF_SPARK_LOGS, HFactory.createColumn(column, value, cs, ss))
      mutator.execute()
    }
  }

  override def deleteQueryName(name: String, userId: String): Unit = {
    if (name == null || userId.isEmpty) {
      return
    }

    Utils.TryWithRetry {
      logger.debug("Deleting query name " + name + " for user " + userId)

      val key = computeRowKey(name)

      val column = new Composite()
      column.setComponent(LEVEL_TYPE, TYPE_QUERY_NAME, is)
      column.setComponent(LEVEL_USERID, userId, ss)
      column.setComponent(LEVEL_UUID, name, ss)

      val mutator = HFactory.createMutator(keyspace, is)
      mutator.addDeletion(key, CF_SPARK_LOGS, column, cs)

      mutator.execute()
    }
  }

  override def getPublishedQueries(userId: String):Array[String] = {
    Utils.TryWithRetry {
      logger.info(s"Getting the published queries for user $userId")

      val column = new Composite()
      column.addComponent(LEVEL_TYPE, TYPE_QUERY_PUBLISHED_STATE, ComponentEquality.EQUAL)
      column.addComponent(LEVEL_USERID, userId, ComponentEquality.EQUAL)

      val sliceQuery = HFactory.createSliceQuery(keyspace, is, cs, ss)
      sliceQuery.setColumnFamily(CF_SPARK_LOGS).setKey(QUERY_NAME_PUBLISHED_ROW).setColumnNames(column)
        .setRange(null, null, false, Integer.MAX_VALUE)


      val result = sliceQuery.execute()

      if (result != null) {
        val queryResult = result.get()

        if (queryResult == null || queryResult.getColumns == null) {
          return Array[String]()
        }

        val columnsIterator = queryResult.getColumns.iterator()

        val arrayBuffer = ArrayBuffer[String]()

        while (columnsIterator.hasNext) {
          val compositeColumn = columnsIterator.next()
          arrayBuffer += compositeColumn.getName.get(LEVEL_UUID, ss)
        }
        return arrayBuffer.toArray
      }
    }
    Array[String]()
  }

  def setQueryPublishedStatus(name: String, metaInfo: QueryMetaInfo, published: Boolean, userId: String): Unit = {
    if (name.isEmpty) {
      return
    }

    Utils.TryWithRetry {
      logger.info(s"Updating published status of $name to $published for user $userId")

      val mutator = HFactory.createMutator(keyspace, is)

      // Delete the old entry for query
      deleteQueryPublishedStatus(name, metaInfo.published, userId)

      val column = new Composite()
      column.setComponent(LEVEL_TYPE, TYPE_QUERY_PUBLISHED_STATE, is)
      column.setComponent(LEVEL_USERID, userId, ss)
      column.setComponent(LEVEL_UUID, name, ss)
      var status = QUERY_NAME_PUBLISHED_ROW
      if (!published) {
        status = QUERY_NAME_UNPUBLISHED_ROW
      }
      mutator.addInsertion(status, CF_SPARK_LOGS, HFactory.createColumn(column, "", cs, ss))

      mutator.execute()
    }
  }

  def deleteQueryPublishedStatus(name: String, published:Option[Boolean], userId: String): Unit = {
    Utils.TryWithRetry {
      logger.info(s"Deleting query published status of $name for user $userId")

      val mutator = HFactory.createMutator(keyspace, is)

      val column = new Composite()
      column.setComponent(LEVEL_TYPE, TYPE_QUERY_PUBLISHED_STATE, is)
      column.setComponent(LEVEL_USERID, userId, ss)
      column.setComponent(LEVEL_UUID, name, ss)
      var status = QUERY_NAME_PUBLISHED_ROW
      if (published.isEmpty || !published.get) {
        status = QUERY_NAME_UNPUBLISHED_ROW
      }
      mutator.addDeletion(status, CF_SPARK_LOGS, column, cs)
      mutator.execute()
    }
  }

  def deleteQuery(queryId: String, userId: String) {
    Utils.TryWithRetry {
      logger.debug(s"Deleting query $queryId for user $userId")
      val key = computeRowKey(queryId + userId)
      val mutator = HFactory.createMutator(keyspace, is)

      logger.debug(s"Deleting query state for $queryId and user $userId")
      val column = new Composite()
      column.setComponent(LEVEL_TYPE, TYPE_QUERY_STATE, is)
      column.setComponent(LEVEL_USERID, userId, ss)
      column.setComponent(LEVEL_UUID, queryId, ss)
      mutator.addDeletion(key, CF_SPARK_LOGS, column, cs)

      logger.debug(s"Deleting query details for $queryId and user $userId")
      column.setComponent(LEVEL_TYPE, TYPE_SCRIPT_DETAILS, is)
      mutator.addDeletion(key, CF_SPARK_LOGS, column, cs)

      val metaInfo = getMetaInfo(queryId, userId)
      if (metaInfo.name.isDefined && metaInfo.name.get != null) {
        // The query has a name. It must be deleted to not appear in search.
        deleteQueryName(metaInfo.name.get, userId)

        if (metaInfo.published.isDefined) {
          deleteQueryPublishedStatus(metaInfo.name.get, metaInfo.published, userId)
        }
      }

      logger.debug(s"Deleting meta info for $queryId and user $userId")
      column.setComponent(LEVEL_TYPE, TYPE_META, is)
      mutator.addDeletion(key, CF_SPARK_LOGS, column, cs)

      logger.debug(s"Deleting query logs for $queryId and user $userId")
      var logs = getLogs(queryId, 0, 100, userId)
      while (logs.logs.nonEmpty) {
        logs.logs.foreach(log => {
          column.setComponent(LEVEL_TYPE, TYPE_LOG, is)
          column.setComponent(LEVEL_TIME_STAMP, log.timestamp, ls)
          mutator.addDeletion(key, CF_SPARK_LOGS, column, cs)
        })
        mutator.execute()
        logs = getLogs(queryId, logs.logs(logs.logs.length - 1).timestamp + 1, 100, userId)
      }

      mutator.execute()
    }
  }
}