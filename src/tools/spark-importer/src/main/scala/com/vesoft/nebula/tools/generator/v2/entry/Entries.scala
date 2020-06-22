/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

package com.vesoft.nebula.tools.generator.v2.entry

import com.vesoft.nebula.tools.generator.v2.entry.Entry.{
  EdgeRank,
  EdgeType,
  EdgeVersion,
  PropertyNames,
  PropertyValues,
  TagID,
  TagVersion,
  VertexID
}

/**
  *  Define the type's alias in Entry object
  */
object Entry {
  type PartitionID    = Int
  type TagID          = Int
  type TagVersion     = Long
  type EdgeType       = Int
  type EdgeVersion    = Long
  type VertexID       = Long
  type EdgeRank       = Long
  type PropertyNames  = List[String]
  type PropertyValues = List[Any]
}

/**
  * Schema ID include tag id or edge type.
  * But not at the same time.
  *
  * @param tagID
  * @param edgeType
  */
case class SchemaID(tagID: Option[TagID], edgeType: Option[EdgeType])

/**
  *
  * @param tagVersion
  * @param edgeVersion
  */
case class SchemaVersion(tagVersion: Option[TagVersion], edgeVersion: Option[EdgeVersion])

/**
  *
  * @param vertexID
  * @param properties
  * @param values
  */
case class Vertex(vertexID: VertexID, properties: PropertyNames, values: List[Any]) {

  def propertyNames: String = properties.mkString(",")

  def propertyValues: String = values.mkString(",")

  override def toString: String = {
    s"Vertex ID: ${vertexID}, " +
      s"Properties: ${properties.mkString(", ")}" +
      s"Values: ${values.mkString(", ")}"
  }
}

/**
 *
 * @param vertex
 * @param policy
 * @param properties
 * @param values
 */
case class VertexWithPolicy(vertex: String,
                            policy: KeyPolicy.Value,
                            properties: PropertyNames,
                            values: List[Any]) {

  def propertyNames: String = properties.mkString(",")

  def propertyValues: String = values.mkString(",")

  override def toString: String = {
    s"Vertex: ${vertex}, Policy: ${policy} " +
      s"Properties: ${properties.mkString(", ")}, " +
      s"Values: ${values.mkString(", ")}"
  }
}

/**
  *
  * @param source       edge's start point.
  * @param destination  edge's end point.
  * @param ranking      edge's weight, the default value is 0.
  * @param properties   edge's fields.
  * @param values       edge's values.
  */
case class Edge(source: VertexID,
                destination: VertexID,
                ranking: EdgeRank,
                properties: PropertyNames,
                values: PropertyValues) {

  def this(source: VertexID,
           destination: VertexID,
           properties: PropertyNames,
           values: PropertyValues) = {
    this(source, destination, 0L, properties, values)
  }

  def propertyNames: String = properties.mkString(",")

  def propertyValues: String = values.mkString(",")

  override def toString: String = {
    s"Edge: ${source}->${destination}@${ranking} ${propertyNames} values: ${propertyValues}"
  }
}

/**
 *
 * @param source
 * @param sourcePolicy
 * @param destination
 * @param destinationPolicy
 * @param ranking
 * @param properties
 * @param values
 */
case class EdgeWithPolicy(source: String,
                          sourcePolicy: KeyPolicy.Value,
                          destination: String,
                          destinationPolicy: KeyPolicy.Value,
                          ranking: EdgeRank,
                          properties: PropertyNames,
                          values: List[Any]) {
  def this(source: String,
           sourcePolicy: KeyPolicy.Value,
           destination: String,
           destinationPolicy: KeyPolicy.Value,
           properties: PropertyNames,
           values: PropertyValues) = {
    this(source, sourcePolicy, destination, destinationPolicy, 0L, properties, values)
  }

  def propertyNames: String = properties.mkString(",")

  def propertyValues: String = values.mkString(",")

  override def toString: String = {
    s"Edge: ${sourcePolicy}(${source})->${destinationPolicy}(${destination})@${ranking}" +
      s" ${propertyNames} values: ${propertyValues}"
  }
}
