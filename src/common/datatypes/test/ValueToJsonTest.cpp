/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */
#include <gtest/gtest.h>
#include <thrift/lib/cpp2/protocol/Serializer.h>

#include "common/base/Base.h"
#include "common/datatypes/CommonCpp2Ops.h"
#include "common/datatypes/DataSet.h"
#include "common/datatypes/Date.h"
#include "common/datatypes/Edge.h"
#include "common/datatypes/List.h"
#include "common/datatypes/Map.h"
#include "common/datatypes/Path.h"
#include "common/datatypes/Set.h"
#include "common/datatypes/Value.h"
#include "common/datatypes/Vertex.h"

namespace nebula {

using folly::dynamic;
using serializer = apache::thrift::CompactSerializer;

TEST(ValueToJson, vertex) {
  // Test tag to json
  auto tag1 = Tag("tagName", {{"prop", Value(2)}});
  auto tag2 =
      Tag("tagName1",
          {{"prop1", Value(2)}, {"prop2", Value(NullType::__NULL__)}, {"prop3", Value("123")}});
  {
    dynamic expectedTagJson = dynamic::object("tagName.prop", 2);
    ASSERT_EQ(expectedTagJson, tag1.toJson());
  }
  {
    dynamic expectedTagJson =
        dynamic::object("tagName1.prop1", 2)("tagName1.prop2", nullptr)("tagName1.prop3", "123");
    ASSERT_EQ(expectedTagJson, tag2.toJson());
  }

  // vertex wtih string vid
  auto vertexStrVid = Value(Vertex({"Vid",
                                    {
                                        tag1,
                                        tag2,
                                    }}));

  // integerID vertex
  auto vertexIntVid = Value(Vertex({001,
                                    {
                                        tag1,
                                        tag2,
                                    }}));
  {
    dynamic expectedVertexJson = dynamic::object("tagName.prop", 2)("tagName1.prop1", 2)(
        "tagName1.prop2", nullptr)("tagName1.prop3", "123");
    ASSERT_EQ(expectedVertexJson, vertexStrVid.toJson());

    dynamic expectedVertexMetaJson = dynamic::object("id", "Vid")("type", "vertex");
    ASSERT_EQ(expectedVertexMetaJson, vertexStrVid.getMetaData());
  }
  {
    dynamic expectedVertexJson = dynamic::object("tagName.prop", 2)("tagName1.prop1", 2)(
        "tagName1.prop2", nullptr)("tagName1.prop3", "123");
    ASSERT_EQ(expectedVertexJson, vertexIntVid.toJson());

    dynamic expectedVertexMetaJson = dynamic::object("id", 001)("type", "vertex");
    ASSERT_EQ(expectedVertexMetaJson, vertexIntVid.getMetaData());
  }
}

TEST(ValueToJson, edge) {
  // edge
  auto edge1 =
      Value(Edge("Src", "Dst", 1, "Edge", 233, {{"prop1", Value(233)}, {"prop2", Value(2.3)}}));
  // integerID edge
  auto edge2 =
      Value(Edge(101, 102, 1, "Edge", 233, {{"prop1", Value(233)}, {"prop2", Value(2.3)}}));
  {
    dynamic expectedEdgeJson = dynamic::object("prop1", 233)("prop2", 2.3);
    ASSERT_EQ(expectedEdgeJson, edge1.toJson());

    dynamic expectedEdgeMetaJson =
        dynamic::object("id",
                        dynamic::object("name", "Edge")("src", "Src")("dst", "Dst")("type", 1)(
                            "name", "Edge")("ranking", 233))("type", "edge");
    ASSERT_EQ(expectedEdgeMetaJson, edge1.getMetaData());
  }

  {
    dynamic expectedEdgeJson = dynamic::object("prop1", 233)("prop2", 2.3);
    ASSERT_EQ(expectedEdgeJson, edge2.toJson());

    dynamic expectedEdgeMetaJson =
        dynamic::object("id",
                        dynamic::object("name", "Edge")("src", 101)("dst", 102)("type", 1)(
                            "name", "Edge")("ranking", 233))("type", "edge");
    ASSERT_EQ(expectedEdgeMetaJson, edge2.getMetaData());
  }
}

TEST(ValueToJson, path) {
  auto path = Value(Path(Vertex({"v1", {Tag("tagName", {{"prop1", Value(1)}})}}),
                         {Step(Vertex({"v2",
                                       {Tag("tagName2",
                                            {{"prop1", Value(2)},
                                             {"prop2", Value(NullType::__NULL__)},
                                             {"prop3", Value("123")}})}}),
                               1,
                               "edgeName",
                               100,
                               {{"edgeProp", "edgePropVal"}})}));
  auto emptyPath = Value(Path());

  dynamic expectedPathJsonObj = dynamic::array(
      dynamic::object("tagName.prop1", 1),
      dynamic::object("edgeProp", "edgePropVal"),
      dynamic::object("tagName2.prop1", 2)("tagName2.prop2", nullptr)("tagName2.prop3", "123"));
  ASSERT_EQ(expectedPathJsonObj, path.toJson());

  dynamic expectedPathMetaJson = dynamic::array(
      dynamic::object("id", "v1")("type", "vertex"),
      dynamic::object("id",
                      dynamic::object("name", "Edge")("src", "v1")("dst", "v2")("type", 1)(
                          "name", "edgeName")("ranking", 100))("type", "edge"),
      dynamic::object("id", "v2")("type", "vertex"));
  ASSERT_EQ(expectedPathMetaJson, path.getMetaData());
}

TEST(ValueToJson, list) {
  auto list1 = Value(List({Value(2),                                  // int
                           Value(2.33),                               // float
                           Value(true),                               // bool
                           Value("str"),                              // string
                           Date(2021, 12, 21),                        // date
                           Time(13, 30, 15, 0),                       // time
                           DateTime(2021, 12, 21, 13, 30, 15, 0)}));  // datetime
  dynamic expectedListJsonObj = dynamic::array(
      2, 2.33, true, "str", "2021-12-21", "13:30:15.000000000Z", "2021-12-21T13:30:15.000000000Z");
  ASSERT_EQ(expectedListJsonObj, list1.toJson());

  dynamic expectedListMetaObj = dynamic::array(nullptr,
                                               nullptr,
                                               nullptr,
                                               nullptr,
                                               dynamic::object("type", "date"),
                                               dynamic::object("type", "time"),
                                               dynamic::object("type", "datetime"));
  ASSERT_EQ(expectedListMetaObj, list1.getMetaData());
}

TEST(ValueToJson, Set) {
  auto set = Value(Set({Value(2),                                  // int
                        Value(2.33),                               // float
                        Value(true),                               // bool
                        Value("str"),                              // string
                        Date(2021, 12, 21),                        // date
                        Time(13, 30, 15, 0),                       // time
                        DateTime(2021, 12, 21, 13, 30, 15, 0)}));  // datetime
  dynamic expectedSetJsonObj = dynamic::array(
      2, 2.33, true, "str", "2021-12-21", "13:30:15.000000000Z", "2021-12-21T13:30:15.000000000Z");
  // The underlying data structure is unordered_set, so sort before the comparison
  auto actualJson = set.toJson();
  std::sort(actualJson.begin(), actualJson.end());
  std::sort(expectedSetJsonObj.begin(), expectedSetJsonObj.end());
  ASSERT_EQ(expectedSetJsonObj, actualJson);

  // Skip meta json comparison since nested dynamic objects cannot be sorted. i.g. dynamic::object
  // inside dynamic::array
}

TEST(ValueToJson, map) {
  auto map = Value(Map({{"key1", Value(2)},                                  // int
                        {"key2", Value(2.33)},                               // float
                        {"key3", Value(true)},                               // bool
                        {"key4", Value("str")},                              // string
                        {"key5", Date(2021, 12, 21)},                        // date
                        {"key6", Time(13, 30, 15, 0)},                       // time
                        {"key7", DateTime(2021, 12, 21, 13, 30, 15, 0)}}));  // datetime
  dynamic expectedMapJsonObj =
      dynamic::object("key1", 2)("key2", 2.33)("key3", true)("key4", "str")("key5", "2021-12-21")(
          "key6", "13:30:15.000000000Z")("key7", "2021-12-21T13:30:15.000000000Z");
  ASSERT_EQ(expectedMapJsonObj, map.toJson());
  // Skip meta json comparison since nested dynamic objects cannot be sorted. i.g. dynamic::object
  // inside dynamic::array
}

TEST(ValueToJson, dataset) {
  DataSet dataset = DataSet({"col1", "col2", "col3", "col4", "col5", "col6", "col7"});
  dataset.emplace_back(List({Value(2),             // int
                             Value(2.33),          // float
                             Value(true),          // bool
                             Value("str"),         // string
                             Date(2021, 12, 21),   // date
                             Time(13, 30, 15, 0),  // time
                             DateTime(2021, 12, 21, 13, 30, 15, 0)}));
  dynamic expectedDatasetJsonObj =
      dynamic::array(dynamic::object("row",
                                     dynamic::array(2,
                                                    2.33,
                                                    true,
                                                    "str",
                                                    "2021-12-21",
                                                    "13:30:15.000000000Z",
                                                    "2021-12-21T13:30:15.000000000Z"))(
          "meta",
          dynamic::array(nullptr,
                         nullptr,
                         nullptr,
                         nullptr,
                         dynamic::object("type", "date"),
                         dynamic::object("type", "time"),
                         dynamic::object("type", "datetime"))));
  ASSERT_EQ(expectedDatasetJsonObj, dataset.toJson());
}

TEST(ValueToJson, DecodeEncode) {
  std::vector<Value> values{
      // empty
      Value(),

      // null
      Value(NullType::__NULL__),
      Value(NullType::DIV_BY_ZERO),
      Value(NullType::BAD_DATA),
      Value(NullType::ERR_OVERFLOW),
      Value(NullType::OUT_OF_RANGE),
      Value(NullType::UNKNOWN_PROP),

      // int
      Value(0),
      Value(1),
      Value(2),

      // float
      Value(3.14),
      Value(2.67),

      // string
      Value("Hello "),
      Value("World"),

      // bool
      Value(false),
      Value(true),

      // date
      Value(Date(2020, 1, 1)),
      Value(Date(2019, 12, 1)),

      // time
      Value(Time{1, 2, 3, 4}),

      // datetime
      Value(DateTime{1, 2, 3, 4, 5, 6, 7}),

      // vertex
      Value(
          Vertex({"Vid",
                  {
                      Tag("tagName", {{"prop", Value(2)}}),
                      Tag("tagName1", {{"prop1", Value(2)}, {"prop2", Value(NullType::__NULL__)}}),
                  }})),

      // integerID vertex
      Value(
          Vertex({001,
                  {
                      Tag("tagName", {{"prop", Value(2)}}),
                      Tag("tagName1", {{"prop1", Value(2)}, {"prop2", Value(NullType::__NULL__)}}),
                  }})),

      // edge
      Value(Edge("Src", "Dst", 1, "Edge", 233, {{"prop1", Value(233)}, {"prop2", Value(2.3)}})),

      // integerID edge
      Value(Edge(001, 002, 1, "Edge", 233, {{"prop1", Value(233)}, {"prop2", Value(2.3)}})),

      // Path
      Value(Path(
          Vertex({"1", {Tag("tagName", {{"prop", Value(2)}})}}),
          {Step(Vertex({"1", {Tag("tagName", {{"prop", Value(2)}})}}), 1, "1", 1, {{"1", 1}})})),
      Value(Path()),

      // List
      Value(List({Value(2), Value(true), Value(2.33)})),

      // Set
      Value(Set({Value(2), Value(true), Value(2.33)})),

      // Map
      Value(Map({{"Key1", Value(2)}, {"Key2", Value(true)}, {"Key3", Value(2.33)}})),

      // DataSet
      Value(DataSet({"col1", "col2"})),
  };
  for (const auto& val : values) {
    std::string buf;
    buf.reserve(128);
    folly::dynamic jsonObj = val.toJson();
    auto jsonString = folly::toJson(jsonObj);
    serializer::serialize(jsonString, &buf);
    std::string valCopy;
    std::size_t s = serializer::deserialize(buf, valCopy);
    ASSERT_EQ(s, buf.size());
    EXPECT_EQ(jsonString, valCopy);
  }
}

}  // namespace nebula

int main(int argc, char** argv) {
  testing::InitGoogleTest(&argc, argv);
  folly::init(&argc, &argv, true);
  google::SetStderrLogging(google::INFO);

  return RUN_ALL_TESTS();
}
