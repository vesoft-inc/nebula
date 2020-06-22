/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

package com.vesoft.nebula.tools.generator.v2.writer

import com.google.common.net.HostAndPort
import com.vesoft.nebula.client.graph.GraphClientImpl
import com.vesoft.nebula.client.meta.MetaClientImpl
import com.vesoft.nebula.client.storage.StorageClientImpl
import com.vesoft.nebula.tools.generator.v2.entry.{Edge, Vertex}

import scala.collection.JavaConverters._

abstract class ServerBaseWriter {}

/**
  *
  */
class NebulaGraphClientWriter(addresses: List[(String, Int)],
                              user: String,
                              password: String,
                              timeout: Int,
                              connectionRetry: Int,
                              executionRetry: Int,
                              space: String,
                              tag: Option[String],
                              edge: Option[String])
    extends ServerBaseWriter
    with GraphWriter {

  require(addresses.size != 0 && space.trim.size != 0)
  require(user.trim.size != 0 && password.trim.size != 0)
  require(timeout > 0 && connectionRetry > 0 && executionRetry > 0)

  private[this] val BATCH_INSERT_TEMPLATE                           = "INSERT %s %s(%s) VALUES %s"
  private[this] val INSERT_VALUE_TEMPLATE                           = "%s: (%s)"
  private[this] val INSERT_VALUE_TEMPLATE_WITH_POLICY               = "%s(\"%s\"): (%s)"
  private[this] val ENDPOINT_TEMPLATE                               = "%s(\"%s\")"
  private[this] val EDGE_VALUE_WITHOUT_RANKING_TEMPLATE             = "%s->%s: (%s)"
  private[this] val EDGE_VALUE_WITHOUT_RANKING_TEMPLATE_WITH_POLICY = "%s->%s: (%s)"
  private[this] val EDGE_VALUE_TEMPLATE                             = "%s->%s@%d: (%s)"
  private[this] val EDGE_VALUE_TEMPLATE_WITH_POLICY                 = "%s->%s@%d: (%s)"
  private[this] val USE_TEMPLATE                                    = "USE %s"

  val client = new GraphClientImpl(
    addresses.map(address => HostAndPort.fromParts(address._1, address._2)).asJava,
    timeout,
    connectionRetry,
    executionRetry)
  client.setUser(user)
  client.setPassword(password)
  client.switchSpace(space)

  override def writeVertex(vertex: Vertex): Unit = {
    val sentence = ""
    client.execute(sentence)
  }

  override def writeVertices(vertices: Vertex*): Unit = {
    val sentence = ""
    client.execute(sentence)
  }

  override def writeEdge(edge: Edge): Unit = {
    val sentence = ""
    client.execute(sentence)
  }

  override def writeEdges(edges: Edge*): Unit = {
    val sentence = ""
    client.execute(sentence)
  }

  override def close(): Unit = {
    client.close()
  }
}

/**
  *
  * @param addresses
  * @param space
  */
class NebulaStorageClientWriter(addresses: List[(String, Int)], space: String)
    extends ServerBaseWriter
    with KVWriter {

  require(addresses.size != 0)

  def this(host: String, port: Int, space: String) = {
    this(List(host -> port), space)
  }

  private[this] val metaClient = new MetaClientImpl(
    addresses.map(address => HostAndPort.fromParts(address._1, address._2)).asJava)
  val client = new StorageClientImpl(metaClient)

  /**
    *
    * @param key
    * @param value
    */
  override def write(key: String, value: String): Unit = {}

  /**
    *
    * @param pairs
    */
  override def write(pairs: (String, String)*): Unit = {}

  override def close(): Unit = {
    client.close()
  }

}
