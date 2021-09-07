/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
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
    ASSERT_EQ(expectedTagJson, tag1.toJsonObj());
  }
  {
    dynamic expectedTagJson =
        dynamic::object("tagName1.prop1", 2)("tagName1.prop2", nullptr)("tagName1.prop3", "123");
    ASSERT_EQ(expectedTagJson, tag2.toJsonObj());
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
    dynamic expectedVeretxJson = dynamic::object("tagName.prop", 2)("tagName1.prop1", 2)(
        "tagName1.prop2", nullptr)("tagName1.prop3", "123");
    ASSERT_EQ(expectedVeretxJson, vertexStrVid.toJsonObj());

    dynamic expectedVeretxMetaJson = dynamic::object("id", "Vid")("type", "vertex");
    ASSERT_EQ(expectedVeretxMetaJson, vertexStrVid.getMetaData());
  }
  {
    dynamic expectedVeretxJson = dynamic::object("tagName.prop", 2)("tagName1.prop1", 2)(
        "tagName1.prop2", nullptr)("tagName1.prop3", "123");
    ASSERT_EQ(expectedVeretxJson, vertexIntVid.toJsonObj());

    dynamic expectedVeretxMetaJson = dynamic::object("id", 001)("type", "vertex");
    ASSERT_EQ(expectedVeretxMetaJson, vertexIntVid.getMetaData());
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
    ASSERT_EQ(expectedEdgeJson, edge1.toJsonObj());

    dynamic expectedEdgeMetaJson = dynamic::object(
        "id",
        dynamic::object("name", "Edge")("src", "Src")("dst", "Dst")("type", 1)("ranking", 233))(
        "type", "edge");
    ASSERT_EQ(expectedEdgeMetaJson, edge1.getMetaData());
  }

  {
    dynamic expectedEdgeJson = dynamic::object("prop1", 233)("prop2", 2.3);
    ASSERT_EQ(expectedEdgeJson, edge2.toJsonObj());

    dynamic expectedEdgeMetaJson = dynamic::object(
        "id", dynamic::object("name", "Edge")("src", 101)("dst", 102)("type", 1)("ranking", 233))(
        "type", "edge");
    ASSERT_EQ(expectedEdgeMetaJson, edge2.getMetaData());
  }
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

      // datatime
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
    serializer::serialize(val, &buf);
    Value valCopy;
    std::size_t s = serializer::deserialize(buf, valCopy);
    ASSERT_EQ(s, buf.size());
    if (val.isNull()) {
      EXPECT_EQ(valCopy.isNull(), true);
      EXPECT_EQ(val.getNull(), valCopy.getNull());
      continue;
    }
    if (val.empty()) {
      EXPECT_EQ(valCopy.empty(), true);
      continue;
    }
    EXPECT_EQ(val, valCopy);
  }
}

TEST(ValueToJson, Ctor) {
  Value vZero(0);
  EXPECT_TRUE(vZero.isInt());
  Value vFloat(3.14);
  EXPECT_TRUE(vFloat.isFloat());
  Value vStr("Hello ");
  EXPECT_TRUE(vStr.isStr());
  Value vBool(false);
  EXPECT_TRUE(vBool.isBool());
  Value vDate(Date(2020, 1, 1));
  EXPECT_TRUE(vDate.isDate());
  Value vList(List({1, 3, 2}));
  EXPECT_TRUE(vList.isList());
  Value vSet(Set({8, 7}));
  EXPECT_TRUE(vSet.isSet());
  Value vMap(Map({{"a", 9}, {"b", 10}}));
  EXPECT_TRUE(vMap.isMap());

  // Disabled
  // Lead to compile error
  // Value v(nullptr);
  // std::map<std::string, std::string> tmp;
  // Value v2(&tmp);
}

}  // namespace nebula

int main(int argc, char** argv) {
  testing::InitGoogleTest(&argc, argv);
  folly::init(&argc, &argv, true);
  google::SetStderrLogging(google::INFO);

  return RUN_ALL_TESTS();
}
