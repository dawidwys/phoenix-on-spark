package com.blogspot.yetanothercoders.hfile

import org.apache.hadoop.hbase.KeyValue
import org.apache.hadoop.hbase.io.ImmutableBytesWritable

/**
 * @author dawid 
 * @since  31.05.15.
 */
object PartitionSorter {
  def sortPartition[A <: ImmutableBytesWritable, T <: KeyValue](p: Iterator[(A,T)]): Iterator[(A,T)] = {
    implicit def keyValueOrdering: Ordering[(A,T)] = new Ordering[(A,T)] {
      def compare(x: (A,T), y: (A,T)): Int = {
        new KeyValue.KVComparator().compare(x._2, y._2)
      }
    }
    p.toStream.sorted.toIterator
  }
}
