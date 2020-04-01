/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

package com.vesoft.nebula.tools.generator.v2.writer

import com.vesoft.nebula.NebulaCodec
import com.vesoft.nebula.tools.generator.v2.entry.Entry.PartitionID
import com.vesoft.nebula.tools.generator.v2.entry.{Edge, SchemaID, SchemaVersion, Vertex}
import org.rocksdb.{EnvOptions, Options, RocksDB, Slice, SstFileWriter}

abstract class FileBaseWriter(val path: String) extends Writer {
  require(path.trim.size != 0)
}

class NebulaSSTWriter(override val path: String,
                      partition: PartitionID,
                      schema: SchemaID,
                      version: SchemaVersion)
    extends FileBaseWriter(path) {

  RocksDB.loadLibrary

  val options = new Options().setCreateIfMissing(true)
  val env     = new EnvOptions()
  val writer  = new SstFileWriter(env, options)
  writer.open(path)

  override def writeVertex(vertex: Vertex): Unit = {
    val key = NebulaCodec.createVertexKey(partition,
                                          vertex.vertexID,
                                          schema.tagID.get,
                                          version.tagVersion.get)
    writer.put(new Slice(key), new Slice("1"))
  }

  override def writeVertices(vertices: Vertex*): Unit = {
    vertices.foreach(writeVertex(_))
  }

  override def writeEdge(edge: Edge): Unit = {
    val key = NebulaCodec.createEdgeKey(partition,
                                        edge.source,
                                        schema.edgeType.get,
                                        edge.ranking,
                                        edge.destination,
                                        version.edgeVersion.get)
    writer.put(new Slice(key), new Slice("1"))
  }

  override def writeEdges(edges: Edge*): Unit = {
    edges.foreach(writeEdge(_))
  }

  override def close(): Unit = {
    writer.finish()
    env.close()
    options.close()
    writer.close()
  }
}
