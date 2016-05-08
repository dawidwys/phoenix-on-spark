package com.blogspot.yetanothercoders.hfile

import java.sql.{Connection, Date, DriverManager, PreparedStatement}
import java.util

import org.apache.commons.lang3.ArrayUtils
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.KeyValue
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.util.Bytes
import org.apache.phoenix.schema.types.{PArrayDataType, PDataType, PDate, PhoenixArray}
import org.apache.phoenix.spark.{ConfigurationUtil, ProductRDDFunctions}
import org.apache.phoenix.util.{PhoenixRuntime, QueryUtil}
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.joda.time.DateTime

import scala.collection.JavaConverters._
import com.blogspot.yetanothercoders.utils.JdbcUtils._

/**
  * @author dawid
  * @since  01.06.15.
  */
class ExtendedProductRDDFunctions[A <: scala.Product](data: org.apache.spark.rdd.RDD[A]) extends
ProductRDDFunctions[A](data) with Serializable {

  type SqlType = Int

  /**
    * Converts the given RDD to RDD of HFile ready files.
    *
    * @param tableName name of target table
    * @param columns columns in order of which the input rdd consists
    * @param zkUrl zk url passed as a string
    * @param conf configuration of environment
    * @return rdd ready to be stored in HFile's
    */
  def convertToKeyValue(tableName: String,
                        columns: Seq[String],
                        conf: Option[Configuration] = None,
                        zkUrl: Option[String] = None): RDD[(ImmutableBytesWritable, KeyValue)] = {

    val config = ConfigurationUtil.getOutputConfiguration(tableName, columns, zkUrl, conf)
    val jdbcUrl = zkUrl.map(getJdbcUrl).getOrElse(getJdbcUrl(config))
    val conn = DriverManager.getConnection(jdbcUrl)

    val tableBytes = Bytes.toBytes(tableName)
    val columnTypes = PhoenixRuntime.generateColumnInfo(conn, tableName, columns.asJava).asScala.map(_.getSqlType)
    conn.close()

    processRDD(tableName, columns, jdbcUrl, tableBytes, columnTypes)
  }

  private def processRDD(tableName: String,
                         columns: Seq[String],
                         jdbcUrl: String,
                         tableBytes: Array[Byte],
                         columnTypes: Seq[SqlType]): RDD[(ImmutableBytesWritable, KeyValue)] = {

    val sc = data.sparkContext
    val query = sc.broadcast(QueryUtil.constructUpsertStatement(tableName, columns.toList.asJava, null))
    val colBr = sc.broadcast(columnTypes)
    val tableBr = sc.broadcast(tableBytes)
    data.mapPartitions(x => {
      val conn = DriverManager.getConnection(jdbcUrl)

      x.foreach(product => insertRow(conn, product, colBr, query))

      val uncommittedDataIterator = PhoenixRuntime.getUncommittedDataIterator(conn, true)
      val hRows = uncommittedDataIterator.asScala.filter(kvPair =>
        Bytes.compareTo(tableBytes, kvPair.getFirst) == 0
      ).flatMap(kvPair => kvPair.getSecond.asScala.map(
        kv => (new ImmutableBytesWritable(kv.getRowArray, kv.getRowOffset, kv.getRowLength), kv)))

      conn.rollback()
      conn.close()
      hRows
    }
    )
  }

  private def insertRow(conn: Connection,
                        product: Product,
                        columnsTypes: Broadcast[Seq[SqlType]],
                        query: Broadcast[String]) = {
    val preparedStatement = conn.prepareStatement(query.value)

    columnsTypes.value.zip(product.productIterator.toStream).zipWithIndex.foreach(setInStatement(preparedStatement))
    preparedStatement.execute()
  }


  private def mapRow(product: Product,
                     jdbcUrl: Broadcast[String],
                     columnsTypes: Broadcast[Seq[SqlType]],
                     tableBytes: Broadcast[Array[Byte]],
                     query: Broadcast[String]): Seq[(ImmutableBytesWritable, KeyValue)] = {

    val conn = DriverManager.getConnection(jdbcUrl.value)
    val preparedStatement = conn.prepareStatement(query.value)

    columnsTypes.value.zip(product.productIterator.toStream).zipWithIndex.foreach(setInStatement(preparedStatement))
    preparedStatement.execute()

    val uncommittedDataIterator = PhoenixRuntime.getUncommittedDataIterator(conn, true)
    val hRows = uncommittedDataIterator.asScala.filter(kvPair =>
      Bytes.compareTo(tableBytes.value, kvPair.getFirst) == 0
    ).flatMap(kvPair => kvPair.getSecond.asScala.map(
      kv => (new ImmutableBytesWritable(kv.getRowArray, kv.getRowOffset, kv.getRowLength), kv)))

    conn.rollback()
    conn.close()
    hRows.toList
  }

  private def setInStatement(statement: PreparedStatement): (((SqlType, Any), Int)) => Unit = {
    case ((c, v), i) =>
      try {
        if (v != null) {
          // Both Java and Joda dates used to work in 4.2.3, but now they must be java.sql.Date
          val (finalObj, finalType) = (v, c) match {
            case (dt: DateTime, _) => (new Date(dt.getMillis), PDate.INSTANCE.getSqlType)
            case (d: util.Date, _) => (new Date(d.getTime), PDate.INSTANCE.getSqlType)
            case (b: Array[Byte], sqlType) =>
              val arrayObject = ArrayUtils.toObject(b).asInstanceOf[Array[AnyRef]]
              transformToPhoenixArray(c, arrayObject)
            case (b: Array[Float], sqlType) =>
              val arrayObject = ArrayUtils.toObject(b).asInstanceOf[Array[AnyRef]]
              transformToPhoenixArray(c, arrayObject)
            case (b: Array[Boolean], sqlType) =>
              val arrayObject = ArrayUtils.toObject(b).asInstanceOf[Array[AnyRef]]
              transformToPhoenixArray(c, arrayObject)
            case (b: Array[Int], sqlType) =>
              val arrayObject = ArrayUtils.toObject(b).asInstanceOf[Array[AnyRef]]
              transformToPhoenixArray(c, arrayObject)
            case (b: Array[Byte], sqlType) =>
              val arrayObject = ArrayUtils.toObject(b).asInstanceOf[Array[AnyRef]]
              transformToPhoenixArray(c, arrayObject)
            case (b: Array[_], sqlType) =>
              val arrayObject = b.asInstanceOf[Array[AnyRef]]
              transformToPhoenixArray(c, arrayObject)
            case _ => (v, c)
          }
          statement.setObject(i + 1, finalObj, finalType)
        } else {
          statement.setNull(i + 1, c)
        }
      } catch {
        case e: Throwable =>
          throw new IllegalArgumentException("Index: " + (i + 1) + " object: " + v.toString + " type: " +
            c, e)
      }
  }

  def transformToPhoenixArray(c: SqlType, b: Array[AnyRef]): (PhoenixArray, SqlType) = {
    val arrayValueType = PDataType.fromTypeId(c - PDataType.ARRAY_TYPE_BASE)
    val pArray = PArrayDataType.instantiatePhoenixArray(arrayValueType, b)
    (pArray, c)
  }
}