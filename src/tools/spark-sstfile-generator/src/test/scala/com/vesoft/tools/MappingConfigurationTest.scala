/* Copyright (c) 2018 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

package com.vesoft.tools

import org.scalatest.Matchers._
import org.scalatest.{BeforeAndAfter, FlatSpec}
import play.api.libs.json.Json

import scala.io.Source
import scala.language.implicitConversions

class MappingConfigurationTest extends FlatSpec with BeforeAndAfter {
  "an Edge" should "be serializable to and deserializable from json" in {
    // edge w/o properties
    val emptyPropertyEdge = Edge(
      "table1",
      "edge1",
      "from_column1",
      "table1",
      "to_column1",
      "table2",
      Some("dt"),
      Some("pin2mac")
    )
    val jsonString = Json.toJson(emptyPropertyEdge).toString()
    val expectedResult =
      """{"table_name":"table1","edge_name":"edge1","date_partition_key":"dt","type_partition_key":"pin2mac","from_foreign_key_column":"from_column1","from_tag":"table1","to_foreign_key_column":"to_column1","to_tag":"table2"}"""
    assert(expectedResult == jsonString)

    val deserializedEdge = Json.parse(expectedResult).as[Edge]
    assert(deserializedEdge == emptyPropertyEdge)

    // edge w/ properties
    val withPropertiesEdge = Edge(
      "table1",
      "edge1",
      "from_column1",
      "table1",
      "to_column1",
      "table2",
      Some("dt"),
      Some("pin2mac"),
      Some(Seq(Column("col1", "prop1"), Column("col2", "prop2")))
    )
    val withPropertiesEdgeJsonString =
      Json.toJson(withPropertiesEdge).toString()
    val expectedResultWithPropertiesEdge =
      """{"table_name":"table1","edge_name":"edge1","date_partition_key":"dt","type_partition_key":"pin2mac","from_foreign_key_column":"from_column1","from_tag":"table1","to_foreign_key_column":"to_column1","to_tag":"table2","mappings":[{"col1":{"name":"prop1","type":"string"}},{"col2":{"name":"prop2","type":"string"}}]}"""
    assert(expectedResultWithPropertiesEdge == withPropertiesEdgeJsonString)
  }

  "a Tag" should "be serializable to and deserializable from json" in {
    // tag w/o properties
    val emptyPropertyTag = Tag("table1", "tag1", "column1")
    val jsonString       = Json.toJson(emptyPropertyTag).toString()
    val expectedResult =
      """{"table_name":"table1","tag_name":"tag1","primary_key":"column1","date_partition_key":null,"type_partition_key":null}"""
    assert(expectedResult == jsonString)

    val deserializedTag = Json.parse(expectedResult).as[Tag]
    assert(deserializedTag == emptyPropertyTag)

    // tag w/ properties
    val withPropertiesTag = Tag(
      "table1",
      "tag1",
      "column1",
      Some("dt"),
      Some("pin2mac"),
      Some(Seq(Column("col1", "prop1"), Column("col2", "prop2")))
    )
    val withPropertiesTagJsonString = Json.toJson(withPropertiesTag).toString()
    val expectedResultWithPropertiesTag =
      """{"table_name":"table1","tag_name":"tag1","primary_key":"column1","date_partition_key":"dt","type_partition_key":"pin2mac","mappings":[{"col1":{"name":"prop1","type":"string"}},{"col2":{"name":"prop2","type":"string"}}]}"""
    assert(expectedResultWithPropertiesTag == withPropertiesTagJsonString)
  }

  "a MappingConfiguration" should "be serializable to and deserializable from json" in {
    // MappingConfiguration w/o custom ken generation policy
    val tag1 = Tag(
      "table1",
      "tag1",
      "column1",
      Some("dt"),
      Some("pin2mac"),
      Some(Seq(Column("col1", "prop1"), Column("col2", "prop2")))
    )
    val tag2 = Tag(
      "table2",
      "tag2",
      "column2",
      Some("dt"),
      Some("pin2qq"),
      Some(Seq(Column("col1", "prop1"), Column("col2", "prop2")))
    )
    val tag3 = Tag(
      "table3",
      "tag3",
      "column3",
      Some("dt"),
      Some("pin2wallet"),
      Some(Seq(Column("col1", "prop1"), Column("col2", "prop2")))
    )

    // edge w/ properties
    val edge1 = Edge(
      "table4",
      "edge1",
      "from_column1",
      "tag1",
      "to_column1",
      "tag2",
      Some("dt"),
      Some("pin2qq"),
      Some(Seq(Column("col1", "prop1"), Column("col2", "prop2")))
    )
    val edge2 = Edge(
      "table5",
      "edge2",
      "from_column1",
      "tag2",
      "to_column1",
      "tag3",
      Some("dt"),
      Some("pin2mac"),
      Some(Seq(Column("col1", "prop1"), Column("col2", "prop2")))
    )

    val mappingConfig =
      MappingConfiguration("fma", 3, Seq(tag1, tag2, tag3), Seq(edge1, edge2))
    val jsonString = Json.toJson(mappingConfig).toString()
    val expectedResult =
      """{"fma":{"key_policy":"hash_primary_key","partitions":3,"tags":[{"table_name":"table1","tag_name":"tag1","primary_key":"column1","date_partition_key":"dt","type_partition_key":"pin2mac","mappings":[{"col1":{"name":"prop1","type":"string"}},{"col2":{"name":"prop2","type":"string"}}]},{"table_name":"table2","tag_name":"tag2","primary_key":"column2","date_partition_key":"dt","type_partition_key":"pin2qq","mappings":[{"col1":{"name":"prop1","type":"string"}},{"col2":{"name":"prop2","type":"string"}}]},{"table_name":"table3","tag_name":"tag3","primary_key":"column3","date_partition_key":"dt","type_partition_key":"pin2wallet","mappings":[{"col1":{"name":"prop1","type":"string"}},{"col2":{"name":"prop2","type":"string"}}]}],"edges":[{"table_name":"table4","edge_name":"edge1","date_partition_key":"dt","type_partition_key":"pin2qq","from_foreign_key_column":"from_column1","from_tag":"tag1","to_foreign_key_column":"to_column1","to_tag":"tag2","mappings":[{"col1":{"name":"prop1","type":"string"}},{"col2":{"name":"prop2","type":"string"}}]},{"table_name":"table5","edge_name":"edge2","date_partition_key":"dt","type_partition_key":"pin2mac","from_foreign_key_column":"from_column1","from_tag":"tag2","to_foreign_key_column":"to_column1","to_tag":"tag3","mappings":[{"col1":{"name":"prop1","type":"string"}},{"col2":{"name":"prop2","type":"string"}}]}]}}"""
    assert(expectedResult == jsonString)

    val deserializedMappingConfig =
      Json.parse(expectedResult).as[MappingConfiguration]
    assert(deserializedMappingConfig == mappingConfig)

    // MappingConfiguration w/ custom ken generation policy
    val mappingConfigWithCustomPolicy = MappingConfiguration(
      "fma",
      3,
      Seq(tag1, tag2, tag3),
      Seq(edge1, edge2),
      Some("some_other_policy")
    )
    val mappingConfigWithCustomPolicyJson =
      Json.toJson(mappingConfigWithCustomPolicy).toString()
    val mappingConfigWithCustomPolicyExpected =
      """{"fma":{"key_policy":"some_other_policy","partitions":3,"tags":[{"table_name":"table1","tag_name":"tag1","primary_key":"column1","date_partition_key":"dt","type_partition_key":"pin2mac","mappings":[{"col1":{"name":"prop1","type":"string"}},{"col2":{"name":"prop2","type":"string"}}]},{"table_name":"table2","tag_name":"tag2","primary_key":"column2","date_partition_key":"dt","type_partition_key":"pin2qq","mappings":[{"col1":{"name":"prop1","type":"string"}},{"col2":{"name":"prop2","type":"string"}}]},{"table_name":"table3","tag_name":"tag3","primary_key":"column3","date_partition_key":"dt","type_partition_key":"pin2wallet","mappings":[{"col1":{"name":"prop1","type":"string"}},{"col2":{"name":"prop2","type":"string"}}]}],"edges":[{"table_name":"table4","edge_name":"edge1","date_partition_key":"dt","type_partition_key":"pin2qq","from_foreign_key_column":"from_column1","from_tag":"tag1","to_foreign_key_column":"to_column1","to_tag":"tag2","mappings":[{"col1":{"name":"prop1","type":"string"}},{"col2":{"name":"prop2","type":"string"}}]},{"table_name":"table5","edge_name":"edge2","date_partition_key":"dt","type_partition_key":"pin2mac","from_foreign_key_column":"from_column1","from_tag":"tag2","to_foreign_key_column":"to_column1","to_tag":"tag3","mappings":[{"col1":{"name":"prop1","type":"string"}},{"col2":{"name":"prop2","type":"string"}}]}]}}"""
    assert(
      mappingConfigWithCustomPolicyExpected == mappingConfigWithCustomPolicyJson
    )

    val deserializedMappingConfigWithCustomPolicy =
      Json.parse(mappingConfigWithCustomPolicyExpected).as[MappingConfiguration]
    assert(
      deserializedMappingConfigWithCustomPolicy == mappingConfigWithCustomPolicy
    )
  }

  "a MappingConfiguration" should "be constructed from a configuration file" in {
    val config = MappingConfiguration("mapping.json")
    assert(config != null)
    assert(config.databaseName == "fma")
    assert(config.keyPolicy.get == "hash_primary_key")

    val tag1 = Tag(
      "dmt_risk_graph_idmp_node_s_d",
      "mac",
      "node",
      Some("dt"),
      Some("flag"),
      Some(Seq(Column("src_pri_value", "src_pri_value", "string")))
    )
    val tag2 = Tag(
      "dmt_risk_graph_idmp_node_s_d",
      "user_pin",
      "node",
      Some("dt"),
      Some("flag"),
      Some(Seq(Column("src_pri_value", "src_pri_value", "string")))
    )

    // edge w/ properties
    val edge1 = Edge(
      "dmt_risk_graph_idmp_edge_s_d",
      "pin2mac",
      "from_node",
      "user_pin",
      "to_node",
      "mac",
      Some("dt"),
      Some("flag"),
      Some(Seq(Column("src_pri_value", "src_pri_value", "string")))
    )

    Seq(tag1, tag2) should contain theSameElementsInOrderAs config.tags
    List(edge1) should contain theSameElementsInOrderAs config.edges
  }

  "a MappingConfiguration" should "be throw exception from an ill-formatted configuration file" in {
    intercept[IllegalStateException] {
      val bufferedSource = Source.fromInputStream(
        getClass.getClassLoader.getResourceAsStream("mapping-ill-format.json")
      )
      val toString = bufferedSource.mkString
      Json.parse(toString).as[MappingConfiguration]
      bufferedSource.close
    }
  }
}
