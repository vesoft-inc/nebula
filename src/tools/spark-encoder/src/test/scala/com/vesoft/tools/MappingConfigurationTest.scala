package com.vesoft.tools

import org.scalatest.{BeforeAndAfter, FlatSpec}
import play.api.libs.json.Json
import org.scalatest.Matchers._

import scala.language.implicitConversions

class MappingConfigurationTest extends FlatSpec with BeforeAndAfter {
  "an Edge" should "be serializable to and deserializable from json" in {
    // edge w/o properties
    val emptyPropertyEdge = Edge("table1", "edge1", "from_column1", "to_column1")
    val jsonString = Json.toJson(emptyPropertyEdge).toString()
    val expectedResult ="""{"table_name":"table1","edge_name":"edge1","from_column":"from_column1","to_column":"to_column1"}"""
    assert(expectedResult == jsonString)

    val deserializedEdge = Json.parse(expectedResult).as[Edge]
    assert(deserializedEdge == emptyPropertyEdge)

    // edge w/ properties
    val withPropertiesEdge = Edge("table1", "edge1", "from_column1", "to_column1", Some(Seq(Column("col1", "prop1"), Column("col2", "prop2"))))
    val withPropertiesEdgeJsonString = Json.toJson(withPropertiesEdge).toString()
    val expectedResultWithPropertiesEdge ="""{"table_name":"table1","edge_name":"edge1","from_column":"from_column1","to_column":"to_column1","mappings":[{"col1":{"name":"prop1","type":"string"}},{"col2":{"name":"prop2","type":"string"}}]}"""
    assert(expectedResultWithPropertiesEdge == withPropertiesEdgeJsonString)
  }

  "a Tag" should "be serializable to and deserializable from json" in {
    // tag w/o properties
    val emptyPropertyTag = Tag("table1", "tag1", "column1")
    val jsonString = Json.toJson(emptyPropertyTag).toString()
    val expectedResult ="""{"table_name":"table1","tag_name":"tag1","primary_key":"column1"}"""
    assert(expectedResult == jsonString)

    val deserializedTag = Json.parse(expectedResult).as[Tag]
    assert(deserializedTag == emptyPropertyTag)

    // tag w/ properties
    val withPropertiesTag = Tag("table1", "tag1", "column1", Some(Seq(Column("col1", "prop1"), Column("col2", "prop2"))))
    val withPropertiesTagJsonString = Json.toJson(withPropertiesTag).toString()
    val expectedResultWithPropertiesTag =
      """{"table_name":"table1","tag_name":"tag1","primary_key":"column1","mappings":[{"col1":{"name":"prop1","type":"string"}},{"col2":{"name":"prop2","type":"string"}}]}"""
    assert(expectedResultWithPropertiesTag == withPropertiesTagJsonString)
  }

  "a MappingConfiguration" should "be serializable to and deserializable from json" in {
    // MappingConfiguration w/o custom ken generation policy
    val tag1 = Tag("table1", "tag1", "column1", Some(Seq(Column("col1", "prop1"), Column("col2", "prop2"))))
    val tag2 = Tag("table2", "tag2", "column2", Some(Seq(Column("col1", "prop1"), Column("col2", "prop2"))))
    val tag3 = Tag("table3", "tag3", "column3", Some(Seq(Column("col1", "prop1"), Column("col2", "prop2"))))

    // edge w/ properties
    val edge1 = Edge("table4", "edge1", "from_column1", "to_column1", Some(Seq(Column("col1", "prop1"), Column("col2", "prop2"))))
    val edge2 = Edge("table5", "edge2", "from_column1", "to_column1", Some(Seq(Column("col1", "prop1"), Column("col2", "prop2"))))

    val mappingConfig = MappingConfiguration("graph_space1", 3, Seq(tag1, tag2, tag3), Seq(edge1, edge2))
    val jsonString = Json.toJson(mappingConfig).toString()
    val expectedResult =
      """{"graph_space1":{"key_policy":"hash_primary_key","partitions":3,"tags":[{"table_name":"table1","tag_name":"tag1","primary_key":"column1","mappings":[{"col1":{"name":"prop1","type":"string"}},{"col2":{"name":"prop2","type":"string"}}]},{"table_name":"table2","tag_name":"tag2","primary_key":"column2","mappings":[{"col1":{"name":"prop1","type":"string"}},{"col2":{"name":"prop2","type":"string"}}]},{"table_name":"table3","tag_name":"tag3","primary_key":"column3","mappings":[{"col1":{"name":"prop1","type":"string"}},{"col2":{"name":"prop2","type":"string"}}]}],"edges":[{"table_name":"table4","edge_name":"edge1","from_column":"from_column1","to_column":"to_column1","mappings":[{"col1":{"name":"prop1","type":"string"}},{"col2":{"name":"prop2","type":"string"}}]},{"table_name":"table5","edge_name":"edge2","from_column":"from_column1","to_column":"to_column1","mappings":[{"col1":{"name":"prop1","type":"string"}},{"col2":{"name":"prop2","type":"string"}}]}]}}"""
    assert(expectedResult == jsonString)

    val deserializedMappingConfig = Json.parse(expectedResult).as[MappingConfiguration]
    assert(deserializedMappingConfig == mappingConfig)

    // MappingConfiguration w/ custom ken generation policy
    val mappingConfigWithCustomPolicy = MappingConfiguration("graph_space1", 3, Seq(tag1, tag2, tag3), Seq(edge1, edge2), Some("some_other_policy"))
    val mappingConfigWithCustomPolicyJson = Json.toJson(mappingConfigWithCustomPolicy).toString()
    val mappingConfigWithCustomPolicyExpected =
      """{"graph_space1":{"key_policy":"some_other_policy","partitions":3,"tags":[{"table_name":"table1","tag_name":"tag1","primary_key":"column1","mappings":[{"col1":{"name":"prop1","type":"string"}},{"col2":{"name":"prop2","type":"string"}}]},{"table_name":"table2","tag_name":"tag2","primary_key":"column2","mappings":[{"col1":{"name":"prop1","type":"string"}},{"col2":{"name":"prop2","type":"string"}}]},{"table_name":"table3","tag_name":"tag3","primary_key":"column3","mappings":[{"col1":{"name":"prop1","type":"string"}},{"col2":{"name":"prop2","type":"string"}}]}],"edges":[{"table_name":"table4","edge_name":"edge1","from_column":"from_column1","to_column":"to_column1","mappings":[{"col1":{"name":"prop1","type":"string"}},{"col2":{"name":"prop2","type":"string"}}]},{"table_name":"table5","edge_name":"edge2","from_column":"from_column1","to_column":"to_column1","mappings":[{"col1":{"name":"prop1","type":"string"}},{"col2":{"name":"prop2","type":"string"}}]}]}}"""
    assert(mappingConfigWithCustomPolicyExpected == mappingConfigWithCustomPolicyJson)

    val deserializedMappingConfigWithCustomPolicy = Json.parse(mappingConfigWithCustomPolicyExpected).as[MappingConfiguration]
    assert(deserializedMappingConfigWithCustomPolicy == mappingConfigWithCustomPolicy)
  }

  "a MappingConfiguration" should "be constructed from a configuration file" in {
    val config = MappingConfiguration("mapping.json")
    assert(config != null)
    assert(config.graphSpace == "space_one")
    assert(config.keyPolicy.get == "hash_primary_key")

    val tag1 = Tag("table1", "vertex1", "column1", Some(Seq(Column("column1", "prop1", "string"), Column("column2", "prop2", "double"))))
    val tag2 = Tag("table2", "vertex2", "column1", Some(Seq(Column("column1", "prop3", "float"), Column("column2", "prop4", "date"))))

    // edge w/ properties
    val edge1 = Edge("table3", "edge1", "column1", "column2", Some(Seq(Column("column3", "prop1", "string"), Column("column4", "prop2", "date"))))

    Seq(tag1, tag2) should contain theSameElementsInOrderAs config.tags
    List(edge1) should contain theSameElementsInOrderAs config.edges
  }


}
