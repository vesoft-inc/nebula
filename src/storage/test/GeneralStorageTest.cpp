/* Copyright (c) 2019 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "base/Base.h"
#include "fs/TempDir.h"
#include "storage/test/TestUtils.h"
#include "storage/PutProcessor.h"
#include "storage/GetProcessor.h"
#include "storage/RemoveProcessor.h"
#include "storage/RemoveRangeProcessor.h"
#include "storage/PrefixProcessor.h"
#include "storage/ScanProcessor.h"

#include <gtest/gtest.h>
#include <folly/Executor.h>
#include <folly/executors/CPUThreadPoolExecutor.h>

namespace nebula {
namespace storage {

TEST(GeneralStorageTest, SimpleTest) {
    fs::TempDir rootPath("/tmp/GeneralStorageTest.XXXXXX");
    auto kv = TestUtils::initKV(rootPath.path());
    std::unique_ptr<folly::Executor> executor;
    executor.reset(new folly::CPUThreadPoolExecutor(1));
    GraphSpaceID space = 0;

    {
        // Put Test
        cpp2::PutRequest req;
        req.set_space_id(space);
        auto* processor = PutProcessor::instance(kv.get(), nullptr, nullptr, executor.get());
        for (PartitionID part = 1; part <= 3; part++) {
            std::vector<nebula::cpp2::Pair> pairs;
            for (int32_t i = 0; i < 10; i++) {
                nebula::cpp2::Pair pair;
                pair.set_key(folly::stringPrintf("key_1_%d", i));
                pair.set_value(folly::stringPrintf("value_1_%d", i));
                pairs.emplace_back(std::move(pair));
            }
            req.parts.emplace(part, std::move(pairs));
        }
        auto future = processor->getFuture();
        processor->process(req);
        auto resp = std::move(future).get();
        EXPECT_EQ(0, resp.result.failed_codes.size());
    }
    {
        // Get Test
        cpp2::GetRequest req;
        req.set_space_id(space);
        auto* processor = GetProcessor::instance(kv.get(), nullptr, nullptr, executor.get());
        PartitionID part = 1;
        std::vector<std::string> keys;
        for (int32_t i = 0; i < 10; i+=2) {
            keys.emplace_back(folly::stringPrintf("key_1_%d", i));
        }
        req.parts.emplace(part, std::move(keys));
        auto future = processor->getFuture();
        processor->process(req);
        auto resp = std::move(future).get();
        EXPECT_EQ(0, resp.result.failed_codes.size());
        auto result = resp.values;
        EXPECT_EQ(5, result.size());
        EXPECT_EQ("value_1_0", result["key_1_0"]);
        EXPECT_EQ("value_1_2", result["key_1_2"]);
        EXPECT_EQ("value_1_4", result["key_1_4"]);
        EXPECT_EQ("value_1_6", result["key_1_6"]);
        EXPECT_EQ("value_1_8", result["key_1_8"]);
    }
    {
        // Remove Test
        cpp2::RemoveRequest req;
        req.set_space_id(space);
        auto* processor = RemoveProcessor::instance(kv.get(), nullptr, nullptr, executor.get());
        PartitionID part = 1;
        std::vector<std::string> keys;
        for (int32_t i = 0; i < 10; i+=2) {
            keys.emplace_back(folly::stringPrintf("key_1_%d", i));
        }
        req.parts.emplace(part, std::move(keys));
        auto future = processor->getFuture();
        processor->process(req);
        auto resp = std::move(future).get();
        EXPECT_EQ(0, resp.result.failed_codes.size());
    }
    {
        // Get Test
        cpp2::GetRequest req;
        req.set_space_id(space);
        auto* processor = GetProcessor::instance(kv.get(), nullptr, nullptr, executor.get());
        PartitionID part = 1;
        std::vector<std::string> keys;
        for (int32_t i = 0; i < 10; i+=2) {
            keys.emplace_back(folly::stringPrintf("key_1_%d", i));
        }
        req.parts.emplace(part, std::move(keys));
        auto future = processor->getFuture();
        processor->process(req);
        auto resp = std::move(future).get();
        EXPECT_EQ(1, resp.result.failed_codes.size());
        EXPECT_EQ(0, resp.values.size());
    }
    {
        // Prefix Test
        cpp2::PrefixRequest req;
        req.set_space_id(space);
        auto* processor = PrefixProcessor::instance(kv.get(), nullptr, nullptr, executor.get());
        PartitionID part = 2;
        req.parts.emplace(part, "key_2");
        auto future = processor->getFuture();
        processor->process(req);
        auto resp = std::move(future).get();
        EXPECT_EQ(0, resp.result.failed_codes.size());
        EXPECT_EQ(10, resp.values.size());
    }
    {
        // Scan Test
        cpp2:: ScanRequest req;
        req.set_space_id(space);
        auto* processor = ScanProcessor::instance(kv.get(), nullptr, nullptr, executor.get());
        PartitionID part = 2;
        nebula::cpp2::Range range;
        range.set_start("key_2_0");
        range.set_end("key_2_99");
        req.parts.emplace(part, std::move(range));
        auto future = processor->getFuture();
        processor->process(req);
        auto resp = std::move(future).get();
        EXPECT_EQ(0, resp.result.failed_codes.size());
        EXPECT_EQ(10, resp.values.size());
    }
    {
        // Remove Range Test
        cpp2:: RemoveRangeRequest req;
        req.set_space_id(space);
        auto* processor = RemoveRangeProcessor::instance(kv.get(), nullptr,
                                                         nullptr, executor.get());
        PartitionID part = 2;
        nebula::cpp2::Range range;
        range.set_start("key_2_3");
        range.set_end("key_2_6");
        req.parts.emplace(part, std::move(range));
        auto future = processor->getFuture();
        processor->process(req);
        auto resp = std::move(future).get();
        EXPECT_EQ(0, resp.result.failed_codes.size());
    }
}

}  // namespace storage
}  // namespace nebula

int main(int argc, char** argv) {
    testing::InitGoogleTest(&argc, argv);
    folly::init(&argc, &argv, true);
    google::SetStderrLogging(google::INFO);
    return RUN_ALL_TESTS();
}
