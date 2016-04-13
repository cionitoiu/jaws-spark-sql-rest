package com.xpatterns.jaws.data.impl

import scala.collection.JavaConverters._
import com.xpatterns.jaws.data.contracts.TJawsParquetTables
import com.xpatterns.jaws.data.DTO.ParquetTable
import com.xpatterns.jaws.data.utils.Utils
import me.prettyprint.hector.api.beans.AbstractComposite.ComponentEquality
import me.prettyprint.hector.api.query.{QueryResult, MultigetSliceQuery}
import org.apache.log4j.Logger
import me.prettyprint.hector.api.beans.{Rows, Composite}
import me.prettyprint.hector.api.factory.HFactory
import me.prettyprint.hector.api.Keyspace
import me.prettyprint.cassandra.serializers.IntegerSerializer
import me.prettyprint.hector.api.Serializer
import me.prettyprint.cassandra.serializers.StringSerializer
import me.prettyprint.cassandra.serializers.CompositeSerializer
import spray.json._
import spray.json.DefaultJsonProtocol._

class JawsCassandraParquetTables(keyspace: Keyspace) extends TJawsParquetTables {

  val CF_PARQUET_TABLES = "parquet_tables"
  val CF_SPARK_LOGS_NUMBER_OF_ROWS = 100
  val PARQUET_TABLES_ROW = CF_SPARK_LOGS_NUMBER_OF_ROWS + 1
  val ROW_ID = "tables"

  val LEVEL_NAME = 0
  val LEVEL_USERID = 1

  val is = IntegerSerializer.get.asInstanceOf[Serializer[Int]]
  val ints = IntegerSerializer.get.asInstanceOf[Serializer[Integer]]
  val ss = StringSerializer.get.asInstanceOf[Serializer[String]]
  val cs = CompositeSerializer.get.asInstanceOf[Serializer[Composite]]

  val logger = Logger.getLogger("JawsCassandraParquetTables")

  override def addParquetTable(pTable: ParquetTable, userId: String) {
    Utils.TryWithRetry {
      logger.debug(s"Adding the parquet table ${pTable.name} for the filepath ${pTable.filePath} for user $userId")
      val key = computeRowKey(pTable.name + userId)

      val column = new Composite()
      column.setComponent(LEVEL_NAME, pTable.name, ss)
      column.setComponent(LEVEL_USERID, userId, ss)

      val mutator = HFactory.createMutator(keyspace, is)
      val valueTouple = (pTable.namenode, pTable.filePath).toJson.prettyPrint
      mutator.addInsertion(key, CF_PARQUET_TABLES, HFactory.createColumn(column, valueTouple, cs, ss))
      mutator.execute()
    }
  }

  override def deleteParquetTable(name: String, userId: String) {
    Utils.TryWithRetry {
      logger.debug(s"Deleting parquet table $name for user $userId")
      val key = computeRowKey(name + userId)
      val column = new Composite()
      column.setComponent(LEVEL_NAME, name, ss)
      column.setComponent(LEVEL_USERID, userId, ss)
      val mutator = HFactory.createMutator(keyspace, is)

      mutator.addDeletion(key, CF_PARQUET_TABLES, column, cs)
      mutator.execute
    }
  }
  override def listParquetTables(userId: String): Array[ParquetTable] = {
    Utils.TryWithRetry {
      var result = Array[ParquetTable]()
      logger.debug("listing all parquet tables for user " + userId)

      //consider all possible keys and just a column
      val keysList: java.util.List[Integer] = new java.util.ArrayList[Integer]()

      for (key <- 0 until CF_SPARK_LOGS_NUMBER_OF_ROWS) {
        keysList.add(key)
      }

      val column = new Composite()
      column.addComponent(LEVEL_USERID, userId, ComponentEquality.EQUAL)

      val multiSliceQuery: MultigetSliceQuery[Integer, Composite, String] = HFactory.createMultigetSliceQuery(keyspace, ints, cs, ss)
      multiSliceQuery.setColumnFamily(CF_PARQUET_TABLES).setKeys(keysList).setColumnNames(column)
      val queryResult: QueryResult[Rows[Integer, Composite, String]] = multiSliceQuery.execute()

      val rows = queryResult.get()
      Option(rows) match {
        case None => result
        case _ =>
          if (rows.getCount == 0) return result
          else {
            val rrows = rows.asScala
            rrows.foreach(row => {
              val columnSlice = row.getColumnSlice
              if (columnSlice == null || columnSlice.getColumns == null || columnSlice.getColumns.size == 0) {

              } else {
                val columns = columnSlice.getColumns.asScala
                columns.foreach ({ column =>
                  val name = column.getName.getComponent(LEVEL_NAME).getValue
                  val (namenode, filepath) = column.getValue.parseJson.convertTo[(String, String)]
                  result = result :+ new ParquetTable(name.toString, filepath, namenode)
                })
              }
              result
            })
            result
          }
      }
    }
  }

  override def tableExists(name: String, userId: String): Boolean = {
    Utils.TryWithRetry {
      logger.debug(s"Reading the parquet table $name for user $userId")
      val key = computeRowKey(name + userId)
      val column = new Composite()
      column.addComponent(LEVEL_NAME, name, ComponentEquality.EQUAL)
      column.addComponent(LEVEL_USERID, userId, ComponentEquality.EQUAL)
      val columnQuery = HFactory.createColumnQuery(keyspace, is, cs, ss)
      columnQuery.setColumnFamily(CF_PARQUET_TABLES).setKey(key).setName(column)

      val queryResult = columnQuery.execute
      Option(queryResult) match {
        case None => false
        case _ =>
          val column = queryResult.get
          Option(column) match {
            case None => false
            case _ => true
          }
      }
    }
  }

  override def readParquetTable(name: String, userId: String): ParquetTable = {
    Utils.TryWithRetry {
      logger.debug(s"Reading the parquet table $name for user $userId")
      val key = computeRowKey(name + userId)
      val column = new Composite()
      column.addComponent(LEVEL_NAME, name, ComponentEquality.EQUAL)
      column.addComponent(LEVEL_USERID, userId, ComponentEquality.EQUAL)
      val columnQuery = HFactory.createColumnQuery(keyspace, is, cs, ss)
      columnQuery.setColumnFamily(CF_PARQUET_TABLES).setKey(key).setName(column)

      val queryResult = columnQuery.execute
      if (queryResult != null) {
        Option(queryResult) match {
          case None => new ParquetTable
          case _ =>
            val column = queryResult.get
            Option(column) match {
              case None => new ParquetTable
              case _ =>
                val (namenode, filepath) = column.getValue.parseJson.convertTo[(String, String)]
                new ParquetTable(name, filepath, namenode)
            }
        }
      } else new ParquetTable
    }
  }

  private def computeRowKey(uuid: String): Int = {
    Math.abs(uuid.hashCode() % CF_SPARK_LOGS_NUMBER_OF_ROWS)
  }
}