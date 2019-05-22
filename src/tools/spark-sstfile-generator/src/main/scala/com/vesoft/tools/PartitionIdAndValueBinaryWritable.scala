package com.vesoft.tools

import java.io.{DataInput, DataOutput}

import com.vesoft.tools.VertexOrEdgeEnum.VertexOrEdgeEnum
import org.apache.hadoop.io.{BytesWritable, WritableComparable}

object VertexOrEdgeEnum extends Enumeration {
  type VertexOrEdgeEnum = Value

  val Vertex = Value("vertex")
  val Edge = Value("edge")
}

/**
  * composite value for SstRecordWriter
  *
  * @param partitionId      which partition
  * @param values           encoded values by nebula native client
  * @param vertexOrEdgeEnum which indicate what the encoded <code>values</code> represents, a vertex or an edge, default=vertex
  */
class PartitionIdAndValueBinaryWritable(var partitionId: Int, var values: BytesWritable, val vertexOrEdgeEnum: VertexOrEdgeEnum = VertexOrEdgeEnum.Vertex) extends WritableComparable[PartitionIdAndValueBinaryWritable] {
  // can't decide order when an edge compared to a vertex or vise versa, so just give it a fixed order
  private[this] val Unknow_Order = -1

  override def compareTo(o: PartitionIdAndValueBinaryWritable): Int = {
    if (vertexOrEdgeEnum != o.vertexOrEdgeEnum) {
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
