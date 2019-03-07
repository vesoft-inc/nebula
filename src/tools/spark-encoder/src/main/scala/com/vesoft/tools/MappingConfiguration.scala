package com.vesoft.tools

import play.api.libs.json.{Reads, Writes, _}

import scala.io.Source
import scala.language.implicitConversions

/**
  * column mapping
  *
  * @param columnName column
  * @param `type`     map to graph's data type
  */
case class Column(columnName: String, propertyName: String, `type`: String = "string")

object Column {
  implicit val ColumnWrites: Writes[Column] = new Writes[Column] {
    override def writes(col: Column): JsValue = {
      Json.obj(col.columnName -> Json.obj(
        "name" -> col.propertyName,
        "type" -> col.`type`
      ))
    }
  }

  implicit val ColumnReads: Reads[Column] = new Reads[Column] {
    override def reads(json: JsValue): JsResult[Column] = {
      json match {
        case JsObject(mapping) => {
          assert(mapping.keys.nonEmpty && mapping.keys.size == 1)
          assert(mapping.values.nonEmpty)

          val columnName = mapping.keys.toSeq(0)
          val mappingContent = mapping.values.toSeq(0)

          val propertyName = (mappingContent \ "name").as[String]
          val tp = (mappingContent \ "type").get.as[String]
          JsSuccess(Column(columnName, propertyName, tp))
        }
        case a@_ => throw new IllegalStateException(s"illegal mapping format:${a}")
      }
    }
  }
}

/**
  * tag section of configuration file
  *
  * @param tableName      hive table name
  * @param tagName        tag name
  * @param primaryKey     which column is PK
  * @param columnMappings map of hive table column to properties
  */
case class Tag(tableName: String, tagName: String, primaryKey: String, columnMappings: Option[Seq[Column]] = None) {
  def allColumnNames(): Option[Seq[String]] = columnMappings.map(_.map(_.columnName))

  def getColumn(name: String): Option[Column] = {
    columnMappings.map(_.filter {
      col => {
        col.columnName.equalsIgnoreCase(name)
      }
    }.headOption).head
  }
}

object Tag {
  // json implicit converter
  implicit val TagWrites: Writes[Tag] = new Writes[Tag] {
    override def writes(tag: Tag): JsValue = {
      tag.columnMappings.fold(
        Json.obj("table_name" -> JsString(tag.tableName),
          "tag_name" -> JsString(tag.tagName),
          "primary_key" -> JsString(tag.primaryKey)
        )) {
        cols =>
          Json.obj("table_name" -> JsString(tag.tableName),
            "tag_name" -> JsString(tag.tagName),
            "primary_key" -> JsString(tag.primaryKey),
            "mappings" -> Json.toJson(cols))
      }
    }
  }

  implicit val TagReads: Reads[Tag] = new Reads[Tag] {
    override def reads(json: JsValue): JsResult[Tag] = {
      val tableName = (json \ "table_name").as[String]
      val tagName = (json \ "tag_name").as[String]
      val pk = (json \ "primary_key").as[String]
      val columnMappings = (json \ "mappings").asOpt[Seq[Column]]

      JsSuccess(Tag(tableName, tagName, pk, columnMappings))
    }
  }
}


/**
  * edge section of configuration file
  *
  * @param tableName      hive table name
  * @param edgeName       edge type name
  * @param fromColumn     which column represent FROM Vertex
  * @param toColumn       which column represent END Vertex
  * @param columnMappings map of hive table column to properties
  */
case class Edge(tableName: String, edgeName: String, fromColumn: String, toColumn: String, columnMappings: Option[Seq[Column]] = None) {
  def allColumnNames(): Option[Seq[String]] = columnMappings.map(columns => columns.map(_.columnName))
}

import play.api.libs.json._

import scala.language.implicitConversions

object Edge {
  implicit val EdgeWrites: Writes[Edge] = new Writes[Edge] {
    override def writes(edge: Edge): JsValue = {
      edge.columnMappings.fold(
        Json.obj("table_name" -> JsString(edge.tableName),
          "edge_name" -> JsString(edge.edgeName),
          "from_column" -> JsString(edge.fromColumn),
          "to_column" -> JsString(edge.toColumn)
        )) {
        cols =>
          Json.obj("table_name" -> JsString(edge.tableName),
            "edge_name" -> JsString(edge.edgeName),
            "from_column" -> JsString(edge.fromColumn),
            "to_column" -> JsString(edge.toColumn),
            "mappings" -> Json.toJson(cols))
      }
    }
  }

  implicit val EdgeReads: Reads[Edge] = new Reads[Edge] {
    override def reads(json: JsValue): JsResult[Edge] = {

      val tableName = (json \ "table_name").as[String]
      val edgeName = (json \ "edge_name").as[String]
      val fromColumn = (json \ "from_column").as[String]
      val toColumn = (json \ "to_column").as[String]
      val columnMappings = (json \ "mappings").asOpt[Seq[Column]]

      JsSuccess(Edge(tableName, edgeName, fromColumn, toColumn, columnMappings))
    }
  }
}

/**
  * a mapping file in-memory representation
  *
  * @param graphSpace graphspace name this mapping configuration for
  * @param partitions partition number of this graphspace
  * @param tags       tag's mapping
  * @param edges      edge's mapping
  * @param keyPolicy  policy which used to generate unique id, default=hash_primary_key
  */
case class MappingConfiguration(graphSpace: String, partitions: Int, tags: Seq[Tag], edges: Seq[Edge], keyPolicy: Option[String] = Some("hash_primary_key"))

object MappingConfiguration {
  implicit val MappingConfigurationWrites: Writes[MappingConfiguration] = new Writes[MappingConfiguration] {
    override def writes(mapping: MappingConfiguration): JsValue = {
      Json.obj(mapping.graphSpace -> mapping.keyPolicy.fold(
        Json.obj("partitions" -> mapping.partitions,
          "tags" -> mapping.tags,
          "edges" -> mapping.edges
        )) {
        keyPolicy =>
          Json.obj("key_policy" -> JsString(keyPolicy),
            "partitions" -> mapping.partitions,
            "tags" -> mapping.tags,
            "edges" -> mapping.edges
          )
      })
    }
  }

  implicit val MappingConfigurationReads: Reads[MappingConfiguration] = new Reads[MappingConfiguration] {
    override def reads(json: JsValue): JsResult[MappingConfiguration] = {
      json match {
        case JsObject(mapping) => {
          // should be one mapping file for a single graph space
          //TODO: fold when ill-formatted
          assert(mapping.keys.nonEmpty && mapping.keys.size == 1)
          assert(mapping.values.nonEmpty)

          val graphSpaceName = mapping.keys.toSeq(0)
          val mappingContent = mapping.values.toSeq(0)

          val keyPolicy = (mappingContent \ "key_policy").asOpt[String]
          val partitions = (mappingContent \ "partitions").as[Int]
          val tags = (mappingContent \ "tags").get.as[Seq[Tag]]
          val edges = (mappingContent \ "edges").get.as[Seq[Edge]]

          JsSuccess(MappingConfiguration(graphSpaceName, partitions, tags, edges, keyPolicy))
        }
        case a@_ => throw new IllegalStateException(s"illegal mapping format:${a}")
      }
    }
  }

  /**
    * ctor from a mapping file
    *
    * @param mappingFile mapping file, could be embedded in jar, or will be provided through "--files" option
    * @return MappingConfiguration instance
    */
  def apply(mappingFile: String): MappingConfiguration = {
    val bufferedSource = Source.fromInputStream(getClass.getResourceAsStream("/" + mappingFile))
    val toString = bufferedSource.mkString
    val config = Json.parse(toString).as[MappingConfiguration]
    bufferedSource.close
    config
  }
}