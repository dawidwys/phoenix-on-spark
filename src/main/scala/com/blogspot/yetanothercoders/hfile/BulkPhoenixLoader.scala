package com.blogspot.yetanothercoders.hfile

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.client.{HTable, Put}
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.{HFileOutputFormat2, TableMapReduceUtil}
import org.apache.hadoop.hbase.{HBaseConfiguration, KeyValue}
import org.apache.hadoop.mapreduce.Job
import org.apache.spark.rdd.RDD

import scala.reflect.ClassTag


class BulkPhoenixLoader(rdd: RDD[(ImmutableBytesWritable, KeyValue)]) {

  private def createConf(tableName: String, inConf: Option[Configuration] = None): Configuration = {
    val conf = inConf.map(HBaseConfiguration.create).getOrElse(HBaseConfiguration.create())
    val job: Job = Job.getInstance(conf, "Phoenix bulk load")

    job.setMapOutputKeyClass(classOf[ImmutableBytesWritable])
    job.setMapOutputValueClass(classOf[KeyValue])

    TableMapReduceUtil.initCredentials(job)

    val htable: HTable = new HTable(conf, tableName)

    HFileOutputFormat2.configureIncrementalLoad(job, htable)
    conf
  }


  /**
   * Saves the PairRDD into HFile's.
   *
   * @param tableName name of HBase's table to store to
   * @param outputPath path where to store the generated Hfiles
   * @param conf configuration of HBase
   */
  def bulkSave(tableName: String, outputPath: String, conf: Option[Configuration] = None) = {
    val configuration: Configuration = createConf(tableName, conf)
    rdd.mapPartitions(PartitionSorter.sortPartition)
      .saveAsNewAPIHadoopFile(
        outputPath,
        classOf[ImmutableBytesWritable],
        classOf[Put],
        classOf[HFileOutputFormat2],
        configuration)
  }

}
