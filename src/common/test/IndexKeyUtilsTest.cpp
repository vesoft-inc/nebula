/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "base/Base.h"
#include "common/IndexKeyUtils.h"
#include <gtest/gtest.h>

namespace nebula {

VertexID getStringId(int64_t vId) {
    std::string id;
    id.append(reinterpret_cast<const char*>(&vId), sizeof(int64_t));
    return id;
}

std::vector<Value> getIndexValues() {
    std::vector<Value> values;
    values.emplace_back(Value("str"));
    values.emplace_back(Value(true));
    values.emplace_back(Value(12L));
    values.emplace_back(Value(12.12f));
    Date d = {2020, 1, 20};
    values.emplace_back(Value(std::move(d)));

    DateTime dt = {2020, 4, 11, 12, 30, 22, 1111, 2222};
    values.emplace_back(std::move(dt));
    return values;
}

bool evalInt64(int64_t val) {
    Value v(val);
    auto str = IndexKeyUtils::encodeValue(v);
    auto res = IndexKeyUtils::decodeValue(str, Value::Type::INT);
    if (!res.ok()) {
        return false;
    }

    EXPECT_EQ(Value::Type::INT, res.value().type());
    EXPECT_EQ(v, res.value());
    return val == res.value().getInt();
}

bool evalDouble(double val) {
    Value v(val);
    auto str = IndexKeyUtils::encodeValue(v);
    auto res = IndexKeyUtils::decodeValue(str, Value::Type::FLOAT);
    if (!res.ok()) {
        return false;
    }
    EXPECT_EQ(Value::Type::FLOAT, res.value().type());
    EXPECT_EQ(v, res.value());
    return val == res.value().getFloat();
}

bool evalBool(bool val) {
    Value v(val);
    auto str = IndexKeyUtils::encodeValue(v);
    auto res = IndexKeyUtils::decodeValue(str, Value::Type::BOOL);
    if (!res.ok()) {
        return false;
    }
    EXPECT_EQ(Value::Type::BOOL, res.value().type());
    EXPECT_EQ(v, res.value());
    return val == res.value().getBool();
}

bool evalString(std::string val) {
    Value v(val);
    auto str = IndexKeyUtils::encodeValue(v);
    auto res = IndexKeyUtils::decodeValue(str, Value::Type::STRING);
    if (!res.ok()) {
        return false;
    }
    EXPECT_EQ(Value::Type::STRING, res.value().type());
    EXPECT_EQ(v, res.value());
    return val == res.value().getStr();
}

bool evalDate(nebula::Date val) {
    Value v(val);
    auto str = IndexKeyUtils::encodeValue(v);
    auto res = IndexKeyUtils::decodeValue(str, Value::Type::DATE);
    if (!res.ok()) {
        return false;
    }
    EXPECT_EQ(Value::Type::DATE, res.value().type());
    EXPECT_EQ(v, res.value());
    return val == res.value().getDate();
}

bool evalDateTime(nebula::DateTime val) {
    Value v(val);
    auto str = IndexKeyUtils::encodeValue(v);
    auto res = IndexKeyUtils::decodeValue(str, Value::Type::DATETIME);
    if (!res.ok()) {
        return false;
    }
    EXPECT_EQ(Value::Type::DATETIME, res.value().type());
    EXPECT_EQ(v, res.value());
    return val == res.value().getDateTime();
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
    EXPECT_TRUE(evalString("test"));
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
    dt.timezone = 2222;
    EXPECT_TRUE(evalDateTime(dt));
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
    auto key = IndexKeyUtils::vertexIndexKey(8, 1, 1, getStringId(1), values);
    ASSERT_EQ(1, IndexKeyUtils::getIndexId(key));
    ASSERT_EQ(getStringId(1), IndexKeyUtils::getIndexVertexID(8, key));
    ASSERT_EQ(true, IndexKeyUtils::isIndexKey(key));
}

TEST(IndexKeyUtilsTest, vertexIndexKeyV2) {
    auto values = getIndexValues();
    auto key = IndexKeyUtils::vertexIndexKey(100, 1, 1, "vertex_1_1_1_1", values);
    ASSERT_EQ(1, IndexKeyUtils::getIndexId(key));

    VertexID vid = "vertex_1_1_1_1";
    vid.append(100 - vid.size(), '\0');
    ASSERT_EQ(vid, IndexKeyUtils::getIndexVertexID(100, key));
    ASSERT_EQ(true, IndexKeyUtils::isIndexKey(key));
}

TEST(IndexKeyUtilsTest, edgeIndexKeyV1) {
    auto values = getIndexValues();
    auto key = IndexKeyUtils::edgeIndexKey(8, 1, 1, getStringId(1), 1, getStringId(2), values);
    ASSERT_EQ(1, IndexKeyUtils::getIndexId(key));
    ASSERT_EQ(getStringId(1), IndexKeyUtils::getIndexSrcId(8, key));
    ASSERT_EQ(1, IndexKeyUtils::getIndexRank(8, key));
    ASSERT_EQ(getStringId(2), IndexKeyUtils::getIndexDstId(8, key));
    ASSERT_EQ(true, IndexKeyUtils::isIndexKey(key));
}

TEST(IndexKeyUtilsTest, edgeIndexKeyV2) {
    VertexID vid = "vertex_1_1_1_1";
    auto values = getIndexValues();
    auto key = IndexKeyUtils::edgeIndexKey(100, 1, 1, vid, 1, vid, values);
    ASSERT_EQ(1, IndexKeyUtils::getIndexId(key));
    vid.append(100 - vid.size(), '\0');
    ASSERT_EQ(vid, IndexKeyUtils::getIndexSrcId(100, key));
    ASSERT_EQ(1, IndexKeyUtils::getIndexRank(100, key));
    ASSERT_EQ(vid, IndexKeyUtils::getIndexDstId(100, key));
    ASSERT_EQ(true, IndexKeyUtils::isIndexKey(key));
}

TEST(IndexKeyUtilsTest, nullableValue) {
    {
        std::string raw;
        std::vector<Value> values;
        std::vector<Value::Type> colsType;
        for (int64_t j = 2; j <= 7; j++) {
            auto type = Value::Type(j);
            values.emplace_back(Value(NullType::__NULL__));
            colsType.emplace_back(type);
        }
        IndexKeyUtils::encodeValuesWithNull(std::move(values), std::move(colsType), raw);
        u_short s = 0xfc00; /* the binary is '11111100 00000000'*/
        std::string expected;
        expected.append(reinterpret_cast<const char*>(&s), sizeof(u_short));
        auto result = raw.substr(raw.size() - sizeof(int32_t) - sizeof(u_short), sizeof(u_short));
        ASSERT_EQ(expected, result);
    }
    {
        std::string raw;
        std::vector<Value> values;
        std::vector<Value::Type> colsType;
        auto type = Value::Type::BOOL;
        values.emplace_back(Value(true));
        colsType.emplace_back(type);
        values.emplace_back(Value(NullType::__NULL__));
        colsType.emplace_back(type);
        IndexKeyUtils::encodeValuesWithNull(std::move(values), std::move(colsType), raw);
        u_short s = 0x4000; /* the binary is '01000000 00000000'*/
        std::string expected;
        expected.append(reinterpret_cast<const char*>(&s), sizeof(u_short));
        auto result = raw.substr(raw.size() - sizeof(u_short), sizeof(u_short));
        ASSERT_EQ(expected, result);
    }
    {
        std::string raw;
        std::vector<Value> values;
        values.emplace_back(Value(true));
        values.emplace_back(Value(false));
        IndexKeyUtils::encodeValues(std::move(values), raw);
        ASSERT_EQ(2, raw.size());
    }
    {
        std::string raw;
        std::vector<Value> values;
        std::vector<Value::Type> colsType;
        for (int64_t i = 0; i <2; i++) {
            for (int64_t j = 2; j <= 7; j++) {
                auto type = Value::Type(j);
                values.emplace_back(Value(NullType::__NULL__));
                colsType.emplace_back(type);
            }
        }

        IndexKeyUtils::encodeValuesWithNull(std::move(values), std::move(colsType), raw);
        u_short s = 0xfff0; /* the binary is '11111111 11110000'*/
        std::string expected;
        expected.append(reinterpret_cast<const char*>(&s), sizeof(u_short));
        auto result = raw.substr(raw.size() - sizeof(int32_t) * 2 - sizeof(u_short),
                                 sizeof(u_short));
        ASSERT_EQ(expected, result);
    }
    {
        std::string raw;
        std::vector<Value> values;
        std::unordered_map<Value::Type, Value> mockValues;
        {
            mockValues[Value::Type::BOOL] = Value(true);
            mockValues[Value::Type::INT] = Value(4L);
            mockValues[Value::Type::FLOAT] = Value(1.5f);
            mockValues[Value::Type::STRING] = Value("str");
            Date d = {2020, 1, 20};
            mockValues[Value::Type::DATE] = Value(d);
            DateTime dt = {2020, 4, 11, 12, 30, 22, 1111, 2222};
            mockValues[Value::Type::DATETIME] = Value(dt);
        }
        std::vector<Value::Type> colsType;
        for (int64_t i = 0; i <2; i++) {
            for (int64_t j = 2; j <= 7; j++) {
                auto type = Value::Type(j);
                if (j%2 == 0) {
                    values.emplace_back(Value(NullType::__NULL__));
                } else {
                    values.emplace_back(mockValues[type]);
                }
                colsType.emplace_back(type);
            }
        }

        IndexKeyUtils::encodeValuesWithNull(std::move(values), std::move(colsType), raw);
        u_short s = 0xaaa0; /* the binary is '10101010 10100000'*/
        std::string expected;
        expected.append(reinterpret_cast<const char*>(&s), sizeof(u_short));
        auto result = raw.substr(raw.size() - sizeof(int32_t) * 2 - sizeof(u_short),
                                 sizeof(u_short));
        ASSERT_EQ(expected, result);
    }
    {
        std::string raw;
        std::vector<Value> values;
        std::vector<Value::Type> colsType;
        for (int64_t i = 0; i <9; i++) {
            values.emplace_back(Value(NullType::__NULL__));
            colsType.emplace_back(Value::Type::BOOL);
        }
        IndexKeyUtils::encodeValuesWithNull(std::move(values), std::move(colsType), raw);
        u_short s = 0xff80; /* the binary is '11111111 10000000'*/
        std::string expected;
        expected.append(reinterpret_cast<const char*>(&s), sizeof(u_short));
        auto result = raw.substr(raw.size() - sizeof(u_short), sizeof(u_short));
        ASSERT_EQ(expected, result);
    }
}
}  // namespace nebula


int main(int argc, char** argv) {
    testing::InitGoogleTest(&argc, argv);
    folly::init(&argc, &argv, true);
    google::SetStderrLogging(google::INFO);

    return RUN_ALL_TESTS();
}
