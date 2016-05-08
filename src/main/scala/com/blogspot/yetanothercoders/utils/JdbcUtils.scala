package com.blogspot.yetanothercoders.utils

import java.sql.{Connection, DriverManager}

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.{HBaseConfiguration, HConstants}
import org.apache.phoenix.util.{ColumnInfo, PhoenixRuntime}

import scala.collection.JavaConverters._

/**
 * Utility class for working with JDBC.
 *
 * @author dawid 
 * @since  06.06.15.
 */
object JdbcUtils {

  /**
   * Creates a connection to Phoenix from given zkUrl or if not set from the configuration.
   * @param conf configuration with [[org.apache.hadoop.hbase.HConstants]].ZOOKEEPER_QUORUM set
   * @param zkUrl Zookeper quorum given as a String
   * @return [[java.sql.Connection]] to Phoenix
   */
  def getConnection(conf: Option[Configuration] = None, zkUrl: Option[String] = None): Connection = {
    val jdbcUrl = zkUrl.map(getJdbcUrl).getOrElse(getJdbcUrl(conf.getOrElse(HBaseConfiguration.create())))

    val conn = DriverManager.getConnection(jdbcUrl)
    conn
  }

  /**
   * Creates a sequence of [[org.apache.phoenix.util.ColumnInfo]] for given table.
   * The sequence is filtered with with the given list.
   *
   * @param tableName name of the table in HBase
   * @param upsertColumnList list of columns to retrieve info, if [[scala.None]] all columns returned
   * @param conf environment configuration
   * @return sequence of [[org.apache.phoenix.util.ColumnInfo]]
   */
  def generateColumnInfo(tableName: String,
                         upsertColumnList: Option[List[String]] = None,
                         conf: Option[Configuration] = None): Seq[ColumnInfo] = {
    val connection = JdbcUtils.getConnection(conf)
    val list = PhoenixRuntime.generateColumnInfo(connection, tableName, upsertColumnList.map(_.asJava).orNull)
      .asScala.filter(!_.getColumnName.startsWith("\"_"))
    list
  }

  /**
   * Constructs fully qualified jdbc url from zookeeper quorum.
   * @param conf configuration with [[org.apache.hadoop.hbase.HConstants]].ZOOKEEPER_QUORUM set
   * @return fully qualified url
   */
  def getJdbcUrl(conf: Configuration): String = {
    val zkQuorum: String = conf.get(HConstants.ZOOKEEPER_QUORUM)
    if (zkQuorum == null) {
      throw new IllegalStateException(HConstants.ZOOKEEPER_QUORUM + " is not configured")
    }
    PhoenixRuntime.JDBC_PROTOCOL + PhoenixRuntime.JDBC_PROTOCOL_SEPARATOR + zkQuorum
  }

  /**
   * Constructs fully qualified jdbc url from zookeeper quorum.
   * @param zkUrl Zookeper quorum given as a String
   * @return fully qualified url
   */
  def getJdbcUrl(zkUrl: String): String = {
    PhoenixRuntime.JDBC_PROTOCOL + PhoenixRuntime.JDBC_PROTOCOL_SEPARATOR + zkUrl
  }

}
