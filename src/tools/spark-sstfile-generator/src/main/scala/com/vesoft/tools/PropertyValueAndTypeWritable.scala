/* Copyright (c) 2018 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

package com.vesoft.tools

import java.io.{DataInput, DataOutput}

import com.vesoft.tools.VertexOrEdgeEnum.VertexOrEdgeEnum
import org.apache.hadoop.io.{BytesWritable, Writable}

object VertexOrEdgeEnum extends Enumeration {
  type VertexOrEdgeEnum = Value

  val Vertex = Value(0, "vertex")
  val Edge   = Value(1, "edge")
}

/**
  * composite value for SstRecordWriter
  *
  * @param values           encoded values by nebula native client
  * @param vertexOrEdgeEnum which indicate what the encoded <code>values</code> represents, be it a vertex or an edge, default=vertex
  */
class PropertyValueAndTypeWritable(
    var values: BytesWritable,
    var vertexOrEdgeEnum: VertexOrEdgeEnum = VertexOrEdgeEnum.Vertex
) extends Writable {
  override def write(out: DataOutput): Unit = {
    out.write(vertexOrEdgeEnum.id)
    values.write(out)
  }

  override def readFields(in: DataInput): Unit = {
    vertexOrEdgeEnum = in.readInt() match {
      case 0 => VertexOrEdgeEnum.Vertex
      case 1 => VertexOrEdgeEnum.Edge
      case a @ _ =>
        throw new IllegalStateException(s"Non supported value type:${a}")
    }

    values.readFields(in)
  }
}
