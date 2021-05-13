/* Copyright (c) 2019 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "common/base/Base.h"
#include <gtest/gtest.h>
#include "kvstore/LogEncoder.h"


namespace nebula {
namespace kvstore {

TEST(LogEncoderTest, SingleValueTest) {
    // Normal value
    {
        std::string val("Hello World");
        auto encoded = encodeSingleValue(OP_REMOVE, val);
        ASSERT_EQ(sizeof(int64_t) + sizeof(uint32_t) + val.size() + 1, encoded.size());

        auto decoded = decodeSingleValue(encoded);
        ASSERT_EQ(val, decoded.toString());
    }
    // Empty value
    {
        std::string val;
        auto encoded = encodeSingleValue(OP_REMOVE, val);
        ASSERT_EQ(sizeof(int64_t) + sizeof(uint32_t) + 1, encoded.size());

        auto decoded = decodeSingleValue(encoded);
        ASSERT_TRUE(decoded.empty());
    }
}


TEST(LogEncoderTest, MultiValuesTest) {
    // Empty values
    {
        std::vector<std::string> values;
        auto encoded = encodeMultiValues(OP_MULTI_REMOVE, values);
        ASSERT_EQ(1 + sizeof(uint32_t) + sizeof(int64_t), encoded.size());

        auto decoded = decodeMultiValues(encoded);
        ASSERT_TRUE(decoded.empty());
    }

    // Multi values
    {
        std::vector<std::string> values;
        // 3 values
        for (int i = 0; i < 3; i++) {
            values.emplace_back(folly::stringPrintf("Value%03d", i));
        }
        // 2 empty values
        for (int i = 0; i < 2; i++) {
            values.emplace_back();
        }
        auto encoded = encodeMultiValues(OP_MULTI_REMOVE, values);
        auto decoded = decodeMultiValues(encoded);
        ASSERT_EQ(5, decoded.size());
        for (int i = 0; i < 3; i++) {
            ASSERT_EQ(folly::stringPrintf("Value%03d", i), decoded[i].toString());
        }
        for (int i = 3; i < 5; i++) {
            ASSERT_TRUE(decoded[i].empty());
        }
    }

    // Two values
    {
        std::string v1("Hello");
        std::string v2("World");
        std::string v3;

        auto encoded = encodeMultiValues(OP_PUT, v1, v2);
        auto decoded = decodeMultiValues(encoded);
        EXPECT_EQ(2, decoded.size());
        EXPECT_EQ(v1, decoded[0].toString());
        EXPECT_EQ(v2, decoded[1].toString());

        encoded = encodeMultiValues(OP_PUT, v3, v2);
        decoded = decodeMultiValues(encoded);
        EXPECT_EQ(2, decoded.size());
        EXPECT_TRUE(decoded[0].empty());
        EXPECT_EQ(v2, decoded[1].toString());
    }

    // Multi pairs
    {
        std::vector<KV> kvs;
        // 2 pairs
        for (int i = 0; i < 2; i++) {
            kvs.emplace_back(folly::stringPrintf("Key%03d", i),
                             folly::stringPrintf("Value%03d", i));
        }
        // 1 empty value
        kvs.emplace_back("Key002", "");
        auto encoded = encodeMultiValues(OP_MULTI_PUT, kvs);

        auto decoded = decodeMultiValues(encoded);
        // Total 3 pairs = 6 strings
        ASSERT_EQ(6, decoded.size());
        // Check keys
        for (int i = 0; i < 3; i++) {
            ASSERT_EQ(folly::stringPrintf("Key%03d", i), decoded[i * 2].toString());
        }
        // Check values
        for (int i = 0; i < 2; i++) {
            ASSERT_EQ(folly::stringPrintf("Value%03d", i), decoded[i * 2 + 1].toString());
        }
        // Check the third empty value
        ASSERT_TRUE(decoded[5].empty());
    }
}

TEST(LogEncoderTest, KVTest) {
    auto encoded = encodeKV("KV_key", "KV_val");
    auto decoded = decodeKV(encoded);
    ASSERT_EQ("KV_key", decoded.first);
    ASSERT_EQ("KV_val", decoded.second);
}

TEST(LogEncoderTest, HostTest) {
    auto encoded = encodeHost(OP_ADD_LEARNER, HostAddr("1.1.1.1", 1));
    auto decoded = decodeHost(OP_ADD_LEARNER, encoded);
    ASSERT_EQ(HostAddr("1.1.1.1", 1), decoded);
}

TEST(LogEncoderTest, BatchTest) {
    auto helper = std::make_unique<BatchHolder>();
    helper->remove("remove");
    helper->put("put_key", "put_value");
    helper->rangeRemove("begin", "end");
    helper->put("put_key_again", "put_value_again");

    auto encoded = encodeBatchValue(helper->getBatch());
    auto decoded = decodeBatchValue(encoded.c_str());

    std::vector<std::pair<BatchLogType,
                std::pair<folly::StringPiece, folly::StringPiece>>> expectd;
    expectd.emplace_back(OP_BATCH_REMOVE,
            std::pair<folly::StringPiece, folly::StringPiece>("remove", ""));
    expectd.emplace_back(OP_BATCH_PUT,
            std::pair<folly::StringPiece, folly::StringPiece>("put_key", "put_value"));
    expectd.emplace_back(OP_BATCH_REMOVE_RANGE,
            std::pair<folly::StringPiece, folly::StringPiece>("begin", "end"));
    expectd.emplace_back(OP_BATCH_PUT,
            std::pair<folly::StringPiece, folly::StringPiece>("put_key_again", "put_value_again"));
    ASSERT_EQ(expectd, decoded);
}

}  // namespace kvstore
}  // namespace nebula


int main(int argc, char** argv) {
    testing::InitGoogleTest(&argc, argv);
    folly::init(&argc, &argv, true);
    google::SetStderrLogging(google::INFO);

    return RUN_ALL_TESTS();
}

