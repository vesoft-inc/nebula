/* Copyright (c) 2018 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

package com.vesoft.tools

import play.api.libs.json.{Reads, Writes, _}

import scala.io.{Codec, Source}
import scala.language.implicitConversions

/**
  * column mapping
  *
  * @param columnName   hive column name
  * @param propertyName the property name this column maps to
  * @param `type`       map to certain data type of nebula graph
  */
case class Column(
    columnName: String,
    propertyName: String,
    `type`: String = "string"
)

object Column {
  implicit val ColumnWrites: Writes[Column] = new Writes[Column] {
    override def writes(col: Column): JsValue = {
      Json.obj(
        col.columnName -> Json.obj(
          "name" -> col.propertyName,
          "type" -> col.`type`
        )
      )
    }
  }

  implicit val ColumnReads: Reads[Column] = new Reads[Column] {
    override def reads(json: JsValue): JsResult[Column] = {
      json match {
        case JsObject(mapping) => {
          assert(mapping.keys.nonEmpty && mapping.keys.size == 1)
          assert(mapping.values.nonEmpty)

          val columnName     = mapping.keys.toSeq(0)
          val mappingContent = mapping.values.toSeq(0)

          val propertyName = (mappingContent \ "name").as[String]
          val tp           = (mappingContent \ "type").get.as[String]
          JsSuccess(Column(columnName, propertyName, tp))
        }
        case a @ _ =>
          throw new IllegalStateException(s"Illegal mapping format:${a}")
      }
    }
  }
}

/**
  * a trait that both Tag and Edge should extends
  */
trait WithColumnMapping {

  def tableName: String

  def name: String

  def columnMappings: Option[Seq[Column]]
}

/**
  * tag section of configuration file
  *
  * @param tableName        hive table name
  * @param name             tag name
  * @param primaryKey       show the PK column
  * @param datePartitionKey date partition column,Hive table in production is usually Date partitioned
  * @param typePartitionKey type partition columns, when different vertex/edge's properties are identical,they are stored in one hive table, and partitioned by a `type` column
  * @param columnMappings   map of hive table column to properties
  */
case class Tag(
    override val tableName: String,
    override val name: String,
    primaryKey: String,
    datePartitionKey: Option[String] = None,
    typePartitionKey: Option[String] = None,
    override val columnMappings: Option[Seq[Column]] = None
) extends WithColumnMapping {
  def allColumnNames(): Option[Seq[String]] =
    columnMappings.map(_.map(_.columnName))

  def getColumn(name: String): Option[Column] = {
    columnMappings
      .map(_.filter { col =>
        {
          col.columnName.equalsIgnoreCase(name)
        }
      }.headOption)
      .head
  }
}

object Tag {
  // json implicit converter
  implicit val TagWrites: Writes[Tag] = new Writes[Tag] {
    override def writes(tag: Tag): JsValue = {
      tag.columnMappings.fold(
        Json.obj(
          "table_name"  -> JsString(tag.tableName),
          "tag_name"    -> JsString(tag.name),
          "primary_key" -> JsString(tag.primaryKey),
          "date_partition_key" -> (if (tag.datePartitionKey.isDefined)
                                     JsString(tag.datePartitionKey.get)
                                   else JsNull),
          "type_partition_key" -> (if (tag.typePartitionKey.isDefined)
                                     JsString(tag.typePartitionKey.get)
                                   else JsNull)
        )
      ) { cols =>
        Json.obj(
          "table_name"  -> JsString(tag.tableName),
          "tag_name"    -> JsString(tag.name),
          "primary_key" -> JsString(tag.primaryKey),
          "date_partition_key" -> (if (tag.datePartitionKey.isDefined)
                                     JsString(tag.datePartitionKey.get)
                                   else JsNull),
          "type_partition_key" -> (if (tag.typePartitionKey.isDefined)
                                     JsString(tag.typePartitionKey.get)
                                   else JsNull),
          "mappings" -> Json.toJson(cols)
        )
      }
    }
  }

  implicit val TagReads: Reads[Tag] = new Reads[Tag] {
    override def reads(json: JsValue): JsResult[Tag] = {
      val tableName        = (json \ "table_name").as[String]
      val tagName          = (json \ "tag_name").as[String]
      val pk               = (json \ "primary_key").as[String]
      val datePartitionKey = (json \ "date_partition_key").asOpt[String]
      val typePartitionKey = (json \ "type_partition_key").asOpt[String]
      val columnMappings   = (json \ "mappings").asOpt[Seq[Column]]

      JsSuccess(
        Tag(
          tableName,
          tagName,
          pk,
          datePartitionKey,
          typePartitionKey,
          columnMappings
        )
      )
    }
  }
}

/**
  * edge section of configuration file
  *
  * @param tableName            hive table name
  * @param name                 edge type name
  * @param fromForeignKeyColumn  srcID column
  * @param fromReferenceTag      Tag srcID column referenced
  * @param toForeignKeyColumn   dstID column
  * @param toReferenceTag        Tag dstID column referenced
  * @param columnMappings       map of hive table column to properties
  */
case class Edge(
    override val tableName: String,
    override val name: String,
    fromForeignKeyColumn: String,
    fromReferenceTag: String,
    toForeignKeyColumn: String,
    toReferenceTag: String,
    datePartitionKey: Option[String] = None,
    typePartitionKey: Option[String] = None,
    override val columnMappings: Option[Seq[Column]] = None
) extends WithColumnMapping {
  def allColumnNames(): Option[Seq[String]] =
    columnMappings.map(columns => columns.map(_.columnName))
}

object Edge {
  implicit val EdgeWrites: Writes[Edge] = new Writes[Edge] {
    override def writes(edge: Edge): JsValue = {
      edge.columnMappings.fold(
        Json.obj(
          "table_name" -> JsString(edge.tableName),
          "edge_name"  -> JsString(edge.name),
          "date_partition_key" -> (if (edge.datePartitionKey.isDefined)
                                     JsString(edge.datePartitionKey.get)
                                   else JsNull),
          "type_partition_key" -> (if (edge.typePartitionKey.isDefined)
                                     JsString(edge.typePartitionKey.get)
                                   else JsNull),
          "from_foreign_key_column" -> JsString(edge.fromForeignKeyColumn),
          "from_tag"                -> JsString(edge.fromReferenceTag),
          "to_foreign_key_column"   -> JsString(edge.toForeignKeyColumn),
          "to_tag"                  -> JsString(edge.toReferenceTag)
        )
      ) { cols =>
        Json.obj(
          "table_name" -> JsString(edge.tableName),
          "edge_name"  -> JsString(edge.name),
          "date_partition_key" -> (if (edge.datePartitionKey.isDefined)
                                     JsString(edge.datePartitionKey.get)
                                   else JsNull),
          "type_partition_key" -> (if (edge.typePartitionKey.isDefined)
                                     JsString(edge.typePartitionKey.get)
                                   else JsNull),
          "from_foreign_key_column" -> JsString(edge.fromForeignKeyColumn),
          "from_tag"                -> JsString(edge.fromReferenceTag),
          "to_foreign_key_column"   -> JsString(edge.toForeignKeyColumn),
          "to_tag"                  -> JsString(edge.toReferenceTag),
          "mappings"                -> Json.toJson(cols)
        )
      }
    }
  }

  implicit val EdgeReads: Reads[Edge] = new Reads[Edge] {
    override def reads(json: JsValue): JsResult[Edge] = {

      val tableName        = (json \ "table_name").as[String]
      val edgeName         = (json \ "edge_name").as[String]
      val datePartitionKey = (json \ "date_partition_key").asOpt[String]
      val typePartitionKey = (json \ "type_partition_key").asOpt[String]

      val fromForeignKeyColumn = (json \ "from_foreign_key_column").as[String]
      val fromTag              = (json \ "from_tag").as[String]
      val toForeignKeyColumn   = (json \ "to_foreign_key_column").as[String]
      val toTag                = (json \ "to_tag").as[String]

      val columnMappings = (json \ "mappings").asOpt[Seq[Column]]

      JsSuccess(
        Edge(
          tableName,
          edgeName,
          fromForeignKeyColumn,
          fromTag,
          toForeignKeyColumn,
          toTag,
          datePartitionKey,
          typePartitionKey,
          columnMappings
        )
      )
    }
  }
}

/**
  * a mapping file in-memory representation
  *
  * @param databaseName hive database name for this mapping configuration
  * @param partitions   partition number of the target graphspace
  * @param tags         tag's mapping
  * @param edges        edge's mapping
  * @param keyPolicy    policy used to generate unique id, default=hash_primary_key
  */
case class MappingConfiguration(
    databaseName: String,
    partitions: Int,
    tags: Seq[Tag],
    edges: Seq[Edge],
    keyPolicy: Option[String] = Some("hash_primary_key")
)

object MappingConfiguration {
  implicit val MappingConfigurationWrites: Writes[MappingConfiguration] =
    new Writes[MappingConfiguration] {
      override def writes(mapping: MappingConfiguration): JsValue = {
        Json.obj(
          mapping.databaseName -> mapping.keyPolicy.fold(
            Json.obj(
              "partitions" -> mapping.partitions,
              "tags"       -> mapping.tags,
              "edges"      -> mapping.edges
            )
          ) { keyPolicy =>
            Json.obj(
              "key_policy" -> JsString(keyPolicy),
              "partitions" -> mapping.partitions,
              "tags"       -> mapping.tags,
              "edges"      -> mapping.edges
            )
          }
        )
      }
    }

  implicit val MappingConfigurationReads: Reads[MappingConfiguration] =
    new Reads[MappingConfiguration] {
      override def reads(json: JsValue): JsResult[MappingConfiguration] = {
        json match {
          case JsObject(mapping) => {
            // should be one mapping file for a single graph space
            //TODO: fold when ill-formatted
            assert(mapping.keys.nonEmpty && mapping.keys.size == 1)
            assert(mapping.values.nonEmpty)

            val graphSpaceName = mapping.keys.toSeq(0)
            val mappingContent = mapping.values.toSeq(0)

            val keyPolicy  = (mappingContent \ "key_policy").asOpt[String]
            val partitions = (mappingContent \ "partitions").as[Int]
            val tags       = (mappingContent \ "tags").get.as[Seq[Tag]]
            val edges      = (mappingContent \ "edges").get.as[Seq[Edge]]

            // make sure all edge reference existing tags
            val allReferencedTag = (edges.map(_.fromReferenceTag) ++ edges.map(
              _.toReferenceTag
            )).distinct
            if (tags
                  .map(_.name)
                  .intersect(allReferencedTag)
                  .size != allReferencedTag.size) {
              throw new IllegalStateException(
                "Edge's from/to tag reference non-existing tag"
              )
            }

            JsSuccess(
              MappingConfiguration(
                graphSpaceName,
                partitions,
                tags,
                edges,
                keyPolicy
              )
            )

          }
          case a @ _ =>
            throw new IllegalStateException(s"Illegal mapping format:${a}")
        }
      }
    }

  /**
    * construct from a mapping file
    *
    * @param mappingFile mapping file should be provided through "--files" option, and specified the application arg "---mapping_file_input"(--mi for short) at the same time,
    *                    it will be consumed as a classpath resource
    * @return MappingConfiguration instance
    */
  def apply(mappingFile: String): MappingConfiguration = {
    val bufferedSource = Source.fromFile(mappingFile)(Codec("UTF-8"))
    val toString       = bufferedSource.mkString
    val config         = Json.parse(toString).as[MappingConfiguration]
    bufferedSource.close
    config
  }
}
