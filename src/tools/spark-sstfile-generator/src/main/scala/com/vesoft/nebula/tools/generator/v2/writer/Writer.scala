/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

package com.vesoft.nebula.tools.generator.v2.writer

import com.vesoft.nebula.tools.generator.v2.entry.{Edge, Vertex}

/**
  *
  */
trait Writer {

  /**
    *
    * @param vertex
    */
  def writeVertex(vertex: Vertex)

  /**
    *
    * @param vertices
    */
  def writeVertices(vertices: Vertex*)

  /**
    *
    * @param edge
    */
  def writeEdge(edge: Edge)

  /**
    *
    * @param edges
    */
  def writeEdges(edges: Edge*)

  def close()
}
