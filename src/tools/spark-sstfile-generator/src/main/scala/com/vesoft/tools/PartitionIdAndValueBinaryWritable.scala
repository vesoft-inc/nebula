package com.vesoft.tools

import java.io.{DataInput, DataOutput}

import org.apache.hadoop.io.{BytesWritable, WritableComparable}

/**
  * composite value for SstRecordWriter
  *
  * @param partitionId  which partition
  * @param values       encoded values by nebula native client
  * @param vertexOrEdge flag,which indicate what we got, a vertex or an edge, default=vertex
  */
class PartitionIdAndValueBinaryWritable(var partitionId: Int, var values: BytesWritable, val vertexOrEdge: Boolean = true) extends WritableComparable[PartitionIdAndValueBinaryWritable] {
  // can't decide order when an edge compared to a vertex or vise versa, so just give it a random order
  private[this] val Unknow_Order = -1

  override def compareTo(o: PartitionIdAndValueBinaryWritable): Int = {
    if (vertexOrEdge != o.vertexOrEdge) {
      Unknow_Order
    }
    else {
      if (partitionId != o.partitionId) {
        values.compareTo(o.values)

      }
      else {
        partitionId - o.partitionId
      }
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
