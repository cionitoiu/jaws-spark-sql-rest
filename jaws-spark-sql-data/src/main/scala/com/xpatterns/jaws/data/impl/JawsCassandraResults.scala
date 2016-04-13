package com.xpatterns.jaws.data.impl

import com.xpatterns.jaws.data.utils.Utils
import me.prettyprint.hector.api.Keyspace
import com.xpatterns.jaws.data.contracts.TJawsResults
import org.apache.log4j.Logger
import me.prettyprint.cassandra.serializers.StringSerializer
import me.prettyprint.cassandra.serializers.IntegerSerializer
import me.prettyprint.hector.api.Serializer
import me.prettyprint.hector.api.factory.HFactory
import me.prettyprint.hector.api.beans.Composite
import me.prettyprint.cassandra.serializers.CompositeSerializer
import me.prettyprint.cassandra.serializers.BytesArraySerializer
import me.prettyprint.hector.api.beans.AbstractComposite.ComponentEquality
import scala.collection.JavaConversions._
import com.xpatterns.jaws.data.DTO.AvroResult
import com.xpatterns.jaws.data.DTO.CustomResult
import org.apache.avro.Schema
import com.google.gson.GsonBuilder

class JawsCassandraResults(keyspace: Keyspace) extends TJawsResults {

  val CF_SPARK_RESULTS = "results"
  val CF_SPARK_RESULTS_NUMBER_OF_ROWS = 100

  val LEVEL_UUID = 0
  val LEVEL_FORMAT = 1
  val LEVEL_TYPE = 2
  val LEVEL_USERID = 3

  val FORMAT_AVRO = "avro"
  val FORMAT_CUSTOM = "custom"

  val TYPE_SCHEMA = 1
  val TYPE_RESULT = 2

  val logger = Logger.getLogger("JawsCassandraResults")

  val is = IntegerSerializer.get.asInstanceOf[Serializer[Int]]
  val ss = StringSerializer.get.asInstanceOf[Serializer[String]]
  val cs = CompositeSerializer.get.asInstanceOf[Serializer[Composite]]
  val bs = BytesArraySerializer.get.asInstanceOf[Serializer[Array[Byte]]]

  def setAvroResults(uuid: String, avroResults: AvroResult, userId: String) {
    Utils.TryWithRetry {

      logger.debug("Writing avro results to query " + uuid + " and user " + userId)

      val key = computeRowKey(uuid + userId)

      val columnResult = new Composite()
      columnResult.setComponent(LEVEL_UUID, uuid, ss)
      columnResult.setComponent(LEVEL_FORMAT, FORMAT_AVRO, ss)
      columnResult.setComponent(LEVEL_TYPE, TYPE_RESULT, is)
      columnResult.setComponent(LEVEL_USERID, userId, ss)

      val columnSchema = new Composite()
      columnSchema.setComponent(LEVEL_UUID, uuid, ss)
      columnSchema.setComponent(LEVEL_FORMAT, FORMAT_AVRO, ss)
      columnSchema.setComponent(LEVEL_TYPE, TYPE_SCHEMA, is)
      columnSchema.setComponent(LEVEL_USERID, userId, ss)

      val mutator = HFactory.createMutator(keyspace, is)
      mutator.addInsertion(key, CF_SPARK_RESULTS, HFactory.createColumn(columnResult, avroResults.serializeResult(), cs, bs))
      mutator.addInsertion(key, CF_SPARK_RESULTS, HFactory.createColumn(columnSchema, avroResults.schema.toString(), cs, ss))

      mutator.execute()
    }
  }

  def getAvroResults(uuid: String, userId: String): AvroResult = {
    Utils.TryWithRetry {

      logger.debug("Reading results for query " + uuid + " and user " + userId)

      val key = computeRowKey(uuid + userId)
      val schema = new Composite()

      schema.addComponent(LEVEL_UUID, uuid, ComponentEquality.EQUAL)
      schema.addComponent(LEVEL_FORMAT, FORMAT_AVRO, ComponentEquality.EQUAL)
      schema.addComponent(LEVEL_TYPE, TYPE_SCHEMA, ComponentEquality.EQUAL)
      schema.addComponent(LEVEL_USERID, userId, ComponentEquality.EQUAL)

      val results = new Composite()
      results.addComponent(LEVEL_UUID, uuid, ComponentEquality.EQUAL)
      results.addComponent(LEVEL_FORMAT, FORMAT_AVRO, ComponentEquality.EQUAL)
      results.addComponent(LEVEL_TYPE, TYPE_RESULT, ComponentEquality.EQUAL)
      results.addComponent(LEVEL_USERID, userId, ComponentEquality.EQUAL)

      val columnQuery = HFactory.createSliceQuery(keyspace, is, cs, bs)
      columnQuery.setColumnFamily(CF_SPARK_RESULTS).setKey(key).setColumnNames(schema, results)

      val queryResult = columnQuery.execute()
      Option(queryResult) match {
        case None => new AvroResult()
        case _ =>
          val columnSlice = queryResult.get()
          Option(columnSlice) match {
            case None => new AvroResult()
            case _ =>
              Option(columnSlice.getColumns) match {
                case None => new AvroResult()
                case _ =>

                  columnSlice.getColumns.size match {
                    case 2 =>
                      var sch: Schema = null
                      var resultByteArray: Array[Byte] = null
                      columnSlice.getColumns.foreach(col => {
                        col.getName.getComponent(LEVEL_TYPE).getValue(is) match {
                          case TYPE_SCHEMA =>
                            val schemaParser = new Schema.Parser()
                            sch = schemaParser.parse(new String(col.getValue))
                          case _ => resultByteArray = col.getValue
                        }
                      })
                      val res = AvroResult.deserializeResult(resultByteArray, sch)
                      new AvroResult(sch, res)
                    case _ => new AvroResult()
                  }
              }
          }
      }
    }
  }

  def getCustomResults(uuid: String, userId: String): CustomResult = {
    Utils.TryWithRetry {

      logger.debug("Reading custom results for query " + uuid + " and user " + userId)

      val key = computeRowKey(uuid + userId)
      val column = new Composite()

      column.addComponent(LEVEL_UUID, uuid, ComponentEquality.EQUAL)
      column.addComponent(LEVEL_FORMAT, FORMAT_CUSTOM, ComponentEquality.EQUAL)
      column.addComponent(LEVEL_USERID, userId, ComponentEquality.EQUAL)

      val columnQuery = HFactory.createColumnQuery(keyspace, is, cs, ss)
      columnQuery.setColumnFamily(CF_SPARK_RESULTS).setKey(key).setName(column)

      val queryResult = columnQuery.execute()
      Option(queryResult) match {
        case None => new CustomResult()
        case _ =>
          val hColumn = queryResult.get()
          Option(hColumn) match {
            case None => new CustomResult()
            case _ =>
              val gson = new GsonBuilder().create()
              gson.fromJson(hColumn.getValue, classOf[CustomResult])
          }
      }
    }
  }

  def setCustomResults(uuid: String, results: CustomResult, userId: String) {
    Utils.TryWithRetry {
      logger.debug("Writing custom results to query " + uuid + " for user " + userId)
      val gson = new GsonBuilder().create()
      val key = computeRowKey(uuid + userId)

      val column = new Composite()
      column.setComponent(LEVEL_UUID, uuid, ss)
      column.setComponent(LEVEL_FORMAT, FORMAT_CUSTOM, ss)
      column.setComponent(LEVEL_USERID, userId, ss)

      val mutator = HFactory.createMutator(keyspace, is)
      mutator.addInsertion(key, CF_SPARK_RESULTS, HFactory.createColumn(column, gson.toJson(results), cs, ss))

      mutator.execute()
    }
  }

  private def computeRowKey(uuid: String): Integer = {
    Math.abs(uuid.hashCode() % CF_SPARK_RESULTS_NUMBER_OF_ROWS)
  }

  def deleteResults(uuid: String, userId: String) {
    Utils.TryWithRetry {

      logger.debug(s"Deleting results for query $uuid and user $userId")

      val key = computeRowKey(uuid + userId)
      val mutator = HFactory.createMutator(keyspace, is)

      val columnResult = new Composite()
      columnResult.setComponent(LEVEL_UUID, uuid, ss)
      columnResult.setComponent(LEVEL_FORMAT, FORMAT_AVRO, ss)
      columnResult.setComponent(LEVEL_TYPE, TYPE_RESULT, is)
      columnResult.setComponent(LEVEL_USERID, userId, ss)

      val columnSchema = new Composite()
      columnSchema.setComponent(LEVEL_UUID, uuid, ss)
      columnSchema.setComponent(LEVEL_FORMAT, FORMAT_AVRO, ss)
      columnSchema.setComponent(LEVEL_TYPE, TYPE_SCHEMA, is)
      columnSchema.setComponent(LEVEL_USERID, userId, ss)

      val columnCustom = new Composite()
      columnCustom.setComponent(LEVEL_UUID, uuid, ss)
      columnCustom.setComponent(LEVEL_FORMAT, FORMAT_CUSTOM, ss)
      columnCustom.setComponent(LEVEL_USERID, userId, ss)

      mutator.addDeletion(key, CF_SPARK_RESULTS, columnSchema, cs)
      mutator.addDeletion(key, CF_SPARK_RESULTS, columnResult, cs)
      mutator.addDeletion(key, CF_SPARK_RESULTS, columnCustom, cs)
      mutator.execute()
    }
  }
}
