/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#include <gtest/gtest.h>

#include <cstdint>

#include "common/base/Base.h"
#include "common/datatypes/DataSet.h"
#include "common/datatypes/Geography.h"
#include "common/geo/GeoIndex.h"
#include "common/utils/IndexKeyUtils.h"

namespace nebula {

using nebula::cpp2::PropertyType;

VertexID getStringId(int64_t vId) {
  std::string id;
  id.append(reinterpret_cast<const char*>(&vId), sizeof(int64_t));
  return id;
}

std::string getIndexValues() {
  std::string values;
  values.append(IndexKeyUtils::encodeValue(Value("str")));
  values.append(IndexKeyUtils::encodeValue(Value(true)));
  values.append(IndexKeyUtils::encodeValue(Value(12L)));
  values.append(IndexKeyUtils::encodeValue(Value(12.12f)));
  Date d = {2020, 1, 20};
  values.append(IndexKeyUtils::encodeValue(Value(std::move(d))));

  DateTime dt = {2020, 4, 11, 12, 30, 22, 1111};
  values.append(IndexKeyUtils::encodeValue(std::move(dt)));
  return values;
}

bool evalInt64(int64_t val) {
  Value v(val);
  auto str = IndexKeyUtils::encodeValue(v);
  auto res = IndexKeyUtils::decodeValue(str, Value::Type::INT);

  EXPECT_EQ(Value::Type::INT, res.type());
  EXPECT_EQ(v, res);
  return val == res.getInt();
}

bool evalDouble(double val) {
  Value v(val);
  auto str = IndexKeyUtils::encodeValue(v);
  auto res = IndexKeyUtils::decodeValue(str, Value::Type::FLOAT);
  EXPECT_EQ(Value::Type::FLOAT, res.type());
  EXPECT_EQ(v, res);
  return val == res.getFloat();
}

bool evalBool(bool val) {
  Value v(val);
  auto str = IndexKeyUtils::encodeValue(v);
  auto res = IndexKeyUtils::decodeValue(str, Value::Type::BOOL);
  EXPECT_EQ(Value::Type::BOOL, res.type());
  EXPECT_EQ(v, res);
  return val == res.getBool();
}

bool evalString(std::string val, size_t len) {
  Value v(val);
  auto str = IndexKeyUtils::encodeValue(v, len);
  auto res = IndexKeyUtils::decodeValue(str, Value::Type::STRING);
  EXPECT_EQ(Value::Type::STRING, res.type());
  EXPECT_EQ(v, res);
  return val == res.getStr();
}

bool evalTime(nebula::Time val) {
  Value v(val);
  auto str = IndexKeyUtils::encodeValue(v);
  auto res = IndexKeyUtils::decodeValue(str, Value::Type::TIME);
  EXPECT_EQ(Value::Type::TIME, res.type());
  EXPECT_EQ(v, res);
  return val == res.getTime();
}

bool evalDate(nebula::Date val) {
  Value v(val);
  auto str = IndexKeyUtils::encodeValue(v);
  auto res = IndexKeyUtils::decodeValue(str, Value::Type::DATE);
  EXPECT_EQ(Value::Type::DATE, res.type());
  EXPECT_EQ(v, res);
  return val == res.getDate();
}

bool evalDateTime(nebula::DateTime val) {
  Value v(val);
  auto str = IndexKeyUtils::encodeValue(v);
  auto res = IndexKeyUtils::decodeValue(str, Value::Type::DATETIME);
  EXPECT_EQ(Value::Type::DATETIME, res.type());
  EXPECT_EQ(v, res);
  return val == res.getDateTime();
}

std::vector<uint64_t> evalGeography(nebula::Geography val) {
  geo::RegionCoverParams rc(0, 30, 8);
  auto vals = IndexKeyUtils::encodeGeography(val, rc);
  std::vector<uint64_t> got;
  for (auto& str : vals) {
    auto res = IndexKeyUtils::decodeGeography(str);
    got.emplace_back(res);
  }
  // Return the actual cellIds
  return got;
}

TEST(IndexKeyUtilsTest, encodeValue) {
  EXPECT_TRUE(evalInt64(1));
  EXPECT_TRUE(evalInt64(0));
  EXPECT_TRUE(evalInt64(std::numeric_limits<int64_t>::max()));
  EXPECT_TRUE(evalInt64(std::numeric_limits<int64_t>::min()));
  EXPECT_TRUE(evalDouble(1.1));
  EXPECT_TRUE(evalDouble(0.0));
  EXPECT_TRUE(evalDouble(std::numeric_limits<double>::max()));
  EXPECT_TRUE(evalDouble(std::numeric_limits<double>::min()));
  EXPECT_TRUE(evalDouble(-std::numeric_limits<double>::max()));
  EXPECT_TRUE(evalDouble(-(0.000000001 - std::numeric_limits<double>::min())));
  EXPECT_TRUE(evalBool(true));
  EXPECT_TRUE(evalBool(false));
  EXPECT_TRUE(evalString("test", 4));

  nebula::Time t;
  t.hour = 12;
  t.minute = 30;
  t.sec = 22;
  t.microsec = 1111;
  EXPECT_TRUE(evalTime(t));

  nebula::Date d;
  d.year = 2020;
  d.month = 4;
  d.day = 11;
  EXPECT_TRUE(evalDate(d));

  nebula::DateTime dt;
  dt.year = 2020;
  dt.month = 4;
  dt.day = 11;
  dt.hour = 12;
  dt.minute = 30;
  dt.sec = 22;
  dt.microsec = 1111;
  EXPECT_TRUE(evalDateTime(dt));
}

TEST(IndexKeyUtilsTest, encodeGeography) {
  // The expected result comes from BigQuery
  auto toUint64Vector = [](std::vector<int64_t> expect) {
    auto asUint64 = [](int64_t i) -> uint64_t {
      const char* c = reinterpret_cast<const char*>(&i);
      uint64_t u = *reinterpret_cast<const uint64_t*>(c);
      return u;
    };

    std::vector<uint64_t> transformedExpect;
    transformedExpect.reserve(expect.size());
    for (int64_t i : expect) {
      transformedExpect.push_back(asUint64(i));
    }
    return transformedExpect;
  };

  auto pt = Geography::fromWKT("POINT(108.1 32.5)").value();
  EXPECT_EQ(toUint64Vector({0x368981adc0392fe3}), evalGeography(pt));

  auto ls = Geography::fromWKT("LINESTRING(68.9 48.9,76.1 35.5,125.7 28.2)").value();
  EXPECT_EQ(toUint64Vector({0x3440000000000000,
                            0x34ff000000000000,
                            0x3640000000000000,
                            0x3684000000000000,
                            0x37c0000000000000,
                            0x3840000000000000,
                            0x38c0000000000000,
                            0x4240000000000000}),
            evalGeography(ls));

  auto pg = Geography::fromWKT(
                "POLYGON((91.2 38.6,99.7 41.9,111.2 38.9,115.6 33.2,109.5 29.0,105.8 24.1,102.9 "
                "30.5,93.0 28.1,95.4 32.8,86.1 33.6,85.3 38.8,91.2 38.6))")
                .value();
  EXPECT_EQ(toUint64Vector({0x342b000000000000,
                            0x35d0000000000000,
                            0x3700000000000000,
                            0x3830000000000000,
                            0x39d0000000000000}),
            evalGeography(pg));
}

TEST(IndexKeyUtilsTest, encodeDouble) {
  EXPECT_TRUE(evalDouble(100.5));
  EXPECT_TRUE(evalDouble(200.5));
  EXPECT_TRUE(evalDouble(300.5));
  EXPECT_TRUE(evalDouble(400.5));
  EXPECT_TRUE(evalDouble(500.5));
  EXPECT_TRUE(evalDouble(600.5));
}

TEST(IndexKeyUtilsTest, vertexIndexKeyV1) {
  auto values = getIndexValues();
  auto key = IndexKeyUtils::vertexIndexKeys(8, 1, 1, getStringId(1), {std::move(values)})[0];
  ASSERT_EQ(1, IndexKeyUtils::getIndexId(key));
  ASSERT_EQ(getStringId(1), IndexKeyUtils::getIndexVertexID(8, key));
  ASSERT_EQ(true, IndexKeyUtils::isIndexKey(key));
}

TEST(IndexKeyUtilsTest, vertexIndexKeyV2) {
  auto values = getIndexValues();
  auto keys = IndexKeyUtils::vertexIndexKeys(100, 1, 1, "vertex_1_1_1_1", {std::move(values)});
  for (auto& key : keys) {
    ASSERT_EQ(1, IndexKeyUtils::getIndexId(key));

    VertexID vid = "vertex_1_1_1_1";
    vid.append(100 - vid.size(), '\0');
    ASSERT_EQ(vid, IndexKeyUtils::getIndexVertexID(100, key));
    ASSERT_EQ(true, IndexKeyUtils::isIndexKey(key));
  }
}

TEST(IndexKeyUtilsTest, edgeIndexKeyV1) {
  auto values = getIndexValues();
  auto key = IndexKeyUtils::edgeIndexKeys(
      8, 1, 1, getStringId(1), 1, getStringId(2), {std::move(values)})[0];
  ASSERT_EQ(1, IndexKeyUtils::getIndexId(key));
  ASSERT_EQ(getStringId(1), IndexKeyUtils::getIndexSrcId(8, key));
  ASSERT_EQ(1, IndexKeyUtils::getIndexRank(8, key));
  ASSERT_EQ(getStringId(2), IndexKeyUtils::getIndexDstId(8, key));
  ASSERT_EQ(true, IndexKeyUtils::isIndexKey(key));
}

TEST(IndexKeyUtilsTest, edgeIndexKeyV2) {
  VertexID vid = "vertex_1_1_1_1";
  auto values = getIndexValues();
  auto keys = IndexKeyUtils::edgeIndexKeys(100, 1, 1, vid, 1, vid, {std::move(values)});
  for (auto& key : keys) {
    ASSERT_EQ(1, IndexKeyUtils::getIndexId(key));
    vid.append(100 - vid.size(), '\0');
    ASSERT_EQ(vid, IndexKeyUtils::getIndexSrcId(100, key));
    ASSERT_EQ(1, IndexKeyUtils::getIndexRank(100, key));
    ASSERT_EQ(vid, IndexKeyUtils::getIndexDstId(100, key));
    ASSERT_EQ(true, IndexKeyUtils::isIndexKey(key));
  }

  keys = IndexKeyUtils::edgeIndexKeys(100, 1, 1, vid, -1, vid, {std::move(values)});
  for (auto& key : keys) {
    ASSERT_EQ(-1, IndexKeyUtils::getIndexRank(100, key));
  }

  keys =
      IndexKeyUtils::edgeIndexKeys(100, 1, 1, vid, 9223372036854775807, vid, {std::move(values)});
  for (auto& key : keys) {
    ASSERT_EQ(9223372036854775807, IndexKeyUtils::getIndexRank(100, key));
  }

  keys = IndexKeyUtils::edgeIndexKeys(100, 1, 1, vid, 0, vid, {std::move(values)});
  for (auto& key : keys) {
    ASSERT_EQ(0, IndexKeyUtils::getIndexRank(100, key));
  }
}

TEST(IndexKeyUtilsTest, nullableValue) {
  auto nullCol = [](const std::string& name, const PropertyType type) {
    meta::cpp2::ColumnDef col;
    col.name = name;
    col.type.type_ref() = type;
    col.nullable_ref() = true;
    if (type == PropertyType::FIXED_STRING) {
      col.type.type_length_ref() = 10;
    }
    return col;
  };
  {
    std::vector<Value> values;
    std::vector<nebula::meta::cpp2::ColumnDef> cols;
    for (int64_t j = 1; j <= 6; j++) {
      values.emplace_back(Value(NullType::__NULL__));
      cols.emplace_back(nullCol(folly::stringPrintf("col%ld", j), PropertyType::BOOL));
    }
    // TODO(jie) Add index key tests for geography
    auto indexItem = std::make_unique<meta::cpp2::IndexItem>();
    indexItem->fields_ref() = cols;
    auto raws = IndexKeyUtils::encodeValues(std::move(values), indexItem.get());
    u_short s = 0xfc00; /* the binary is '11111100 00000000'*/
    std::string expected;
    expected.append(reinterpret_cast<const char*>(&s), sizeof(u_short));
    auto result = raws[0].substr(raws[0].size() - sizeof(u_short), sizeof(u_short));
    ASSERT_EQ(expected, result);
  }
  {
    std::vector<Value> values;
    values.emplace_back(Value(true));
    values.emplace_back(Value(NullType::__NULL__));
    std::vector<nebula::meta::cpp2::ColumnDef> cols;
    for (int64_t j = 1; j <= 2; j++) {
      cols.emplace_back(nullCol(folly::stringPrintf("col%ld", j), PropertyType::BOOL));
    }
    auto indexItem = std::make_unique<meta::cpp2::IndexItem>();
    indexItem->fields_ref() = cols;
    auto raws = IndexKeyUtils::encodeValues(std::move(values), indexItem.get());
    u_short s = 0x4000; /* the binary is '01000000 00000000'*/
    std::string expected;
    expected.append(reinterpret_cast<const char*>(&s), sizeof(u_short));
    auto result = raws[0].substr(raws[0].size() - sizeof(u_short), sizeof(u_short));
    ASSERT_EQ(expected, result);
  }
  {
    std::vector<Value> values;
    values.emplace_back(Value(true));
    values.emplace_back(Value(false));
    std::vector<nebula::meta::cpp2::ColumnDef> cols;
    for (int64_t j = 1; j <= 2; j++) {
      cols.emplace_back(nullCol(folly::stringPrintf("col%ld", j), PropertyType::BOOL));
    }
    auto indexItem = std::make_unique<meta::cpp2::IndexItem>();
    indexItem->fields_ref() = cols;
    auto raws = IndexKeyUtils::encodeValues(std::move(values), indexItem.get());
    u_short s = 0x0000; /* the binary is '01000000 00000000'*/
    std::string expected;
    expected.append(reinterpret_cast<const char*>(&s), sizeof(u_short));
    auto result = raws[0].substr(raws[0].size() - sizeof(u_short), sizeof(u_short));
    ASSERT_EQ(expected, result);
  }
  {
    std::vector<Value> values;
    std::vector<nebula::meta::cpp2::ColumnDef> cols;
    for (int64_t i = 0; i < 12; i++) {
      values.emplace_back(Value(NullType::__NULL__));
      cols.emplace_back(nullCol(folly::stringPrintf("col%ld", i), PropertyType::INT64));
    }
    auto indexItem = std::make_unique<meta::cpp2::IndexItem>();
    indexItem->fields_ref() = cols;
    auto raws = IndexKeyUtils::encodeValues(std::move(values), indexItem.get());
    u_short s = 0xfff0; /* the binary is '11111111 11110000'*/
    std::string expected;
    expected.append(reinterpret_cast<const char*>(&s), sizeof(u_short));
    auto result = raws[0].substr(raws[0].size() - sizeof(u_short), sizeof(u_short));
    ASSERT_EQ(expected, result);
  }
  {
    std::vector<PropertyType> types;
    types.emplace_back(PropertyType::BOOL);
    types.emplace_back(PropertyType::INT64);
    types.emplace_back(PropertyType::FLOAT);
    types.emplace_back(PropertyType::STRING);
    types.emplace_back(PropertyType::DATE);
    types.emplace_back(PropertyType::DATETIME);

    std::vector<Value> values;
    std::vector<nebula::meta::cpp2::ColumnDef> cols;
    std::unordered_map<Value::Type, Value> mockValues;
    {
      mockValues[Value::Type::BOOL] = Value(true);
      mockValues[Value::Type::INT] = Value(4L);
      mockValues[Value::Type::FLOAT] = Value(1.5f);
      mockValues[Value::Type::STRING] = Value("str");
      Date d = {2020, 1, 20};
      mockValues[Value::Type::DATE] = Value(d);
      DateTime dt = {2020, 4, 11, 12, 30, 22, 1111};
      mockValues[Value::Type::DATETIME] = Value(dt);
    }
    for (const auto& entry : mockValues) {
      values.emplace_back(Value(NullType::__NULL__));
      values.emplace_back(entry.second);
    }
    for (int64_t i = 0; i < 2; i++) {
      for (int64_t j = 0; j < 6; j++) {
        cols.emplace_back(nullCol(folly::stringPrintf("col_%ld_%ld", i, j), types[j]));
      }
    }
    auto indexItem = std::make_unique<meta::cpp2::IndexItem>();
    indexItem->fields_ref() = cols;
    auto raws = IndexKeyUtils::encodeValues(std::move(values), indexItem.get());
    u_short s = 0xaaa0; /* the binary is '10101010 10100000'*/
    std::string expected;
    expected.append(reinterpret_cast<const char*>(&s), sizeof(u_short));
    auto result = raws[0].substr(raws[0].size() - sizeof(u_short), sizeof(u_short));
    ASSERT_EQ(expected, result);
  }
  {
    std::vector<Value> values;
    std::vector<nebula::meta::cpp2::ColumnDef> cols;
    for (int64_t i = 0; i < 9; i++) {
      values.emplace_back(Value(NullType::__NULL__));
      cols.emplace_back(nullCol(folly::stringPrintf("col%ld", i), PropertyType::BOOL));
    }
    auto indexItem = std::make_unique<meta::cpp2::IndexItem>();
    indexItem->fields_ref() = cols;
    auto raws = IndexKeyUtils::encodeValues(std::move(values), indexItem.get());
    u_short s = 0xff80; /* the binary is '11111111 10000000'*/
    std::string expected;
    expected.append(reinterpret_cast<const char*>(&s), sizeof(u_short));
    auto result = raws[0].substr(raws[0].size() - sizeof(u_short), sizeof(u_short));
    ASSERT_EQ(expected, result);
  }
}

void verifyDecodeIndexKey(bool isEdge,
                          bool nullable,
                          size_t vIdLen,
                          const std::vector<std::pair<VertexID, std::vector<Value>>>& data,
                          const std::vector<std::string>& indexKeys,
                          const std::vector<meta::cpp2::ColumnDef>& cols) {
  for (size_t j = 0; j < data.size(); j++) {
    std::vector<Value> actual;
    actual.emplace_back(IndexKeyUtils::getValueFromIndexKey(
        vIdLen, indexKeys[j], "col_bool", cols, isEdge, nullable));
    actual.emplace_back(IndexKeyUtils::getValueFromIndexKey(
        vIdLen, indexKeys[j], "col_int", cols, isEdge, nullable));
    actual.emplace_back(IndexKeyUtils::getValueFromIndexKey(
        vIdLen, indexKeys[j], "col_float", cols, isEdge, nullable));
    actual.emplace_back(IndexKeyUtils::getValueFromIndexKey(
        vIdLen, indexKeys[j], "col_string", cols, isEdge, nullable));
    actual.emplace_back(IndexKeyUtils::getValueFromIndexKey(
        vIdLen, indexKeys[j], "col_date", cols, isEdge, nullable));
    actual.emplace_back(IndexKeyUtils::getValueFromIndexKey(
        vIdLen, indexKeys[j], "col_datetime", cols, isEdge, nullable));

    ASSERT_EQ(data[j].second, actual);
  }
}

TEST(IndexKeyUtilsTest, getValueFromIndexKeyTest) {
  size_t vIdLen = 8;
  PartitionID partId = 1;
  IndexID indexId = 1;
  Date d = {2020, 1, 20};
  DateTime dt = {2020, 4, 11, 12, 30, 22, 1111};
  auto null = Value(NullType::__NULL__);

  std::vector<meta::cpp2::ColumnDef> cols;
  size_t indexValueSize = 0;
  {
    meta::cpp2::ColumnDef col;
    col.name_ref() = "col_bool";
    col.type.type_ref() = PropertyType::BOOL;
    cols.emplace_back(col);
    indexValueSize += sizeof(bool);
  }
  {
    meta::cpp2::ColumnDef col;
    col.name_ref() = "col_int";
    col.type.type_ref() = PropertyType::INT64;
    cols.emplace_back(col);
    indexValueSize += sizeof(int64_t);
  }
  {
    meta::cpp2::ColumnDef col;
    col.name_ref() = "col_float";
    col.type.type_ref() = PropertyType::FLOAT;
    cols.emplace_back(col);
    indexValueSize += sizeof(double);
  }
  {
    meta::cpp2::ColumnDef col;
    col.name_ref() = "col_string";
    col.type.type_ref() = PropertyType::FIXED_STRING;
    col.type.type_length_ref() = 4;
    cols.emplace_back(col);
    indexValueSize += 4;
  }
  {
    meta::cpp2::ColumnDef col;
    col.name_ref() = "col_date";
    col.type.type_ref() = PropertyType::DATE;
    cols.emplace_back(col);
    indexValueSize += sizeof(int8_t) * 2 + sizeof(int16_t);
  }
  {
    meta::cpp2::ColumnDef col;
    col.name_ref() = "col_datetime";
    col.type.type_ref() = PropertyType::DATETIME;
    cols.emplace_back(col);
    indexValueSize += sizeof(int32_t) + sizeof(int16_t) + sizeof(int8_t) * 5;
  }

  // vertices test without nullable column
  {
    std::vector<std::pair<VertexID, std::vector<Value>>> vertices = {
        {"1", {Value(false), Value(1L), Value(1.1f), Value("row1"), Value(d), Value(dt)}},
        {"2", {Value(true), Value(2L), Value(2.1f), Value("row2"), Value(d), Value(dt)}},
    };
    auto expected = vertices;

    auto indexItem = std::make_unique<meta::cpp2::IndexItem>();
    indexItem->fields_ref() = cols;
    std::vector<std::string> indexKeys;
    for (auto& row : vertices) {
      auto values = IndexKeyUtils::encodeValues(std::move(row.second), indexItem.get());
      ASSERT_EQ(indexValueSize, values[0].size());
      auto keys =
          IndexKeyUtils::vertexIndexKeys(vIdLen, partId, indexId, row.first, std::move(values));
      for (auto& key : keys) {
        indexKeys.emplace_back(key);
      }
    }

    verifyDecodeIndexKey(false, false, vIdLen, expected, indexKeys, cols);
  }

  // edges test without nullable column
  {
    std::vector<std::pair<VertexID, std::vector<Value>>> edges = {
        {"1", {Value(false), Value(1L), Value(1.1f), Value("row1"), Value(d), Value(dt)}},
        {"2", {Value(true), Value(2L), Value(2.1f), Value("row2"), Value(d), Value(dt)}},
    };
    auto expected = edges;

    auto indexItem = std::make_unique<meta::cpp2::IndexItem>();
    indexItem->fields_ref() = cols;
    std::vector<std::string> indexKeys;
    for (auto& row : edges) {
      auto values = IndexKeyUtils::encodeValues(std::move(row.second), indexItem.get());
      ASSERT_EQ(indexValueSize, values[0].size());

      auto keys = IndexKeyUtils::edgeIndexKeys(
          vIdLen, partId, indexId, row.first, 0, row.first, std::move(values));
      for (auto& key : keys) {
        indexKeys.emplace_back(key);
      }
    }

    verifyDecodeIndexKey(true, false, vIdLen, expected, indexKeys, cols);
  }

  for (auto& col : cols) {
    col.nullable_ref() = (true);
  }
  // since there are nullable columns, there will be two extra bytes to save
  // nullBitSet
  indexValueSize += sizeof(ushort);

  // vertices test with nullable
  {
    std::vector<std::pair<VertexID, std::vector<Value>>> vertices = {
        {"1", {Value(false), Value(1L), Value(1.1f), Value("row1"), Value(d), Value(dt)}},
        {"2", {Value(true), Value(2L), Value(2.1f), Value("row2"), Value(d), Value(dt)}},
        {"3", {null, Value(3L), Value(3.1f), Value("row3"), Value(d), Value(dt)}},
        {"4", {Value(true), null, Value(4.1f), Value("row4"), Value(d), Value(dt)}},
        {"5", {Value(false), Value(5L), null, Value("row5"), Value(d), Value(dt)}},
        {"6", {Value(true), Value(6L), Value(6.1f), null, Value(d), Value(dt)}},
        {"7", {Value(false), Value(7L), Value(7.1f), Value("row7"), null, Value(dt)}},
        {"8", {Value(true), Value(8L), Value(8.1f), Value("row8"), Value(d), null}},
        {"9", {null, null, null, null, null, null}}};
    auto expected = vertices;

    auto indexItem = std::make_unique<meta::cpp2::IndexItem>();
    indexItem->fields_ref() = cols;
    std::vector<std::string> indexKeys;
    for (auto& row : vertices) {
      auto values = IndexKeyUtils::encodeValues(std::move(row.second), indexItem.get());
      ASSERT_EQ(indexValueSize, values[0].size());
      auto keys =
          IndexKeyUtils::vertexIndexKeys(vIdLen, partId, indexId, row.first, std::move(values));
      for (auto& key : keys) {
        indexKeys.emplace_back(key);
      }
    }
    verifyDecodeIndexKey(false, true, vIdLen, expected, indexKeys, cols);
  }

  // edges test with nullable
  {
    std::vector<std::pair<VertexID, std::vector<Value>>> edges = {
        {"1", {Value(false), Value(1L), Value(1.1f), Value("row1"), Value(d), Value(dt)}},
        {"2", {Value(true), Value(2L), Value(2.1f), Value("row2"), Value(d), Value(dt)}},
        {"3", {null, Value(3L), Value(3.1f), Value("row3"), Value(d), Value(dt)}},
        {"4", {Value(true), null, Value(4.1f), Value("row4"), Value(d), Value(dt)}},
        {"5", {Value(false), Value(5L), null, Value("row5"), Value(d), Value(dt)}},
        {"6", {Value(true), Value(6L), Value(6.1f), null, Value(d), Value(dt)}},
        {"7", {Value(false), Value(7L), Value(7.1f), Value("row7"), null, Value(dt)}},
        {"8", {Value(true), Value(8L), Value(8.1f), Value("row8"), Value(d), null}},
        {"9", {null, null, null, null, null, null}}};
    auto expected = edges;

    auto indexItem = std::make_unique<meta::cpp2::IndexItem>();
    indexItem->fields_ref() = cols;
    std::vector<std::string> indexKeys;
    for (auto& row : edges) {
      auto values = IndexKeyUtils::encodeValues(std::move(row.second), indexItem.get());
      ASSERT_EQ(indexValueSize, values[0].size());
      auto keys = IndexKeyUtils::edgeIndexKeys(
          vIdLen, partId, indexId, row.first, 0, row.first, std::move(values));
      for (auto& key : keys) {
        indexKeys.emplace_back(key);
      }
    }

    verifyDecodeIndexKey(true, true, vIdLen, expected, indexKeys, cols);
  }
}

}  // namespace nebula

int main(int argc, char** argv) {
  testing::InitGoogleTest(&argc, argv);
  folly::init(&argc, &argv, true);
  google::SetStderrLogging(google::INFO);

  return RUN_ALL_TESTS();
}
