package com.vesoft.tools

import java.io.{DataInput, DataOutput}

import org.apache.hadoop.io.{BytesWritable, WritableComparable}

/**
  * composite value for SstRecordWriter
  *
  * @param partitionId which partition
  * @param values      encoded values by nebula native client
  */
class PartitionIdAndValueBinaryWritable(var partitionId: Int, var values: BytesWritable) extends WritableComparable[PartitionIdAndValueBinaryWritable] {
  override def compareTo(o: PartitionIdAndValueBinaryWritable): Int = {
    if (partitionId != o.partitionId) {
      values.compareTo(o.values)
    }
    else {
      partitionId - o.partitionId
    }
  }

  override def write(out: DataOutput): Unit = {
    out.write(partitionId)
    values.write(out)
  }

  override def readFields(in: DataInput): Unit = {
    partitionId = in.readInt()
    values.readFields(in)
  }
}
