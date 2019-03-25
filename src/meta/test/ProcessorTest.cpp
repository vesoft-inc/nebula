/* Copyright (c) 2018 - present, VE Software Inc. All rights reserved
 *
 * This source code is licensed under Apache 2.0 License
 *  (found in the LICENSE.Apache file in the root directory)
 */

#include "base/Base.h"
#include <gtest/gtest.h>
#include <folly/String.h>
#include <fstream>
#include "fs/TempDir.h"
#include "meta/test/TestUtils.h"
#include "meta/processors/CreateSpaceProcessor.h"
#include "meta/processors/GetPartsAllocProcessor.h"
#include "meta/processors/ListSpacesProcessor.h"
#include "meta/processors/PutProcessor.h"
#include "meta/processors/MultiPutProcessor.h"
#include "meta/processors/GetProcessor.h"
#include "meta/processors/MultiGetProcessor.h"
#include "meta/processors/RemoveProcessor.h"
#include "meta/processors/RemoveRangeProcessor.h"
#include "meta/processors/ScanProcessor.h"

namespace nebula {
namespace meta {

TEST(ProcessorTest, AddHostsTest) {
    fs::TempDir rootPath("/tmp/AddHostsTest.XXXXXX");
    std::unique_ptr<kvstore::KVStore> kv(TestUtils::initKV(rootPath.path()));
    {
        std::vector<nebula::cpp2::HostAddr> thriftHosts;
        for (auto i = 0; i < 10; i++) {
            thriftHosts.emplace_back(apache::thrift::FragileConstructor::FRAGILE, i, i);
        }
        cpp2::AddHostsReq req;
        req.set_hosts(std::move(thriftHosts));
        auto* processor = AddHostsProcessor::instance(kv.get());
        auto f = processor->getFuture();
        processor->process(req);
        auto resp = std::move(f).get();
        EXPECT_EQ(resp.code, cpp2::ErrorCode::SUCCEEDED);
    }
    {
        cpp2::ListHostsReq req;
        auto* processor = ListHostsProcessor::instance(kv.get());
        auto f = processor->getFuture();
        processor->process(req);
        auto resp = std::move(f).get();
        EXPECT_EQ(10, resp.hosts.size());
        for (auto i = 0; i < 10; i++) {
            EXPECT_EQ(i, resp.hosts[i].ip);
            EXPECT_EQ(i, resp.hosts[i].port);
        }
    }
    {
        std::vector<nebula::cpp2::HostAddr> thriftHosts;
        for (auto i = 10; i < 20; i++) {
            thriftHosts.emplace_back(apache::thrift::FragileConstructor::FRAGILE, i, i);
        }
        cpp2::AddHostsReq req;
        req.set_hosts(std::move(thriftHosts));
        auto* processor = AddHostsProcessor::instance(kv.get());
        auto f = processor->getFuture();
        processor->process(req);
        auto resp = std::move(f).get();
        EXPECT_EQ(resp.code, cpp2::ErrorCode::SUCCEEDED);
    }
    {
        cpp2::ListHostsReq req;
        auto* processor = ListHostsProcessor::instance(kv.get());
        auto f = processor->getFuture();
        processor->process(req);
        auto resp = std::move(f).get();
        EXPECT_EQ(20, resp.hosts.size());
        for (auto i = 0; i < 20; i++) {
            EXPECT_EQ(i, resp.hosts[i].ip);
            EXPECT_EQ(i, resp.hosts[i].port);
        }
    }
}

TEST(ProcessorTest, CreateSpaceTest) {
    fs::TempDir rootPath("/tmp/CreateSpaceTest.XXXXXX");
    std::unique_ptr<kvstore::KVStore> kv(TestUtils::initKV(rootPath.path()));
    auto hostsNum = TestUtils::createSomeHosts(kv.get());
    {
        cpp2::CreateSpaceReq req;
        req.set_space_name("default_space");
        req.set_parts_num(9);
        req.set_replica_factor(3);

        auto* processor = CreateSpaceProcessor::instance(kv.get());
        auto f = processor->getFuture();
        processor->process(req);
        auto resp = std::move(f).get();
        ASSERT_EQ(resp.code, cpp2::ErrorCode::SUCCEEDED);
        ASSERT_EQ(resp.get_id().get_space_id(), 1);
    }
    {
        cpp2::ListSpacesReq req;
        auto* processor = ListSpacesProcessor::instance(kv.get());
        auto f = processor->getFuture();
        processor->process(req);
        auto resp = std::move(f).get();
        ASSERT_EQ(resp.code, cpp2::ErrorCode::SUCCEEDED);
        ASSERT_EQ(resp.spaces.size(), 1);
        ASSERT_EQ(resp.spaces[0].id.get_space_id(), 1);
        ASSERT_EQ(resp.spaces[0].name, "default_space");
    }
    // Check the result. The dispatch way from part to hosts is in a round robin fashion.
    {
        cpp2::GetPartsAllocReq req;
        req.set_space_id(1);
        auto* processor = GetPartsAllocProcessor::instance(kv.get());
        auto f = processor->getFuture();
        processor->process(req);
        auto resp = std::move(f).get();
        ASSERT_EQ(resp.code, cpp2::ErrorCode::SUCCEEDED);
        for (auto& p : resp.get_parts()) {
            auto startIndex = p.first;
            for (auto& h : p.second) {
                ASSERT_EQ(startIndex++ % hostsNum, h.get_ip());
                ASSERT_EQ(h.get_ip(), h.get_port());
            }
        }
    }
}

TEST(ProcessorTest, KVOperationTest) {
    fs::TempDir rootPath("/tmp/KVOperationTest.XXXXXX");
    std::unique_ptr<kvstore::KVStore> kv(TestUtils::initKV(rootPath.path()));
    auto hostsNum = TestUtils::createSomeHosts(kv.get());
    UNUSED(hostsNum);

    {
        cpp2::CreateSpaceReq req;
        req.set_space_name("default_space");
        req.set_parts_num(9);
        req.set_replica_factor(3);

        auto* processor = CreateSpaceProcessor::instance(kv.get());
        auto f = processor->getFuture();
        processor->process(req);
        auto resp = std::move(f).get();
        ASSERT_EQ(resp.code, cpp2::ErrorCode::SUCCEEDED);
        ASSERT_EQ(resp.get_id().get_space_id(), 1);
    }
    {
        // Put Test
        cpp2::PutReq req;
        req.set_key("key");
        req.set_value("value");

        auto* processor = PutProcessor::instance(kv.get());
        auto f = processor->getFuture();
        processor->process(req);
        auto resp = std::move(f).get();
        ASSERT_EQ(resp.code, cpp2::ErrorCode::SUCCEEDED);
    }
    {
        // Multi Put Test
        std::vector<cpp2::Pair> pairs;
        for (auto i = 0; i < 10; i++) {
            pairs.emplace_back(apache::thrift::FragileConstructor::FRAGILE,
                               folly::stringPrintf("key_%d", i),
                               folly::stringPrintf("value_%d", i));
        }

        cpp2::MultiPutReq req;
        req.set_pairs(std::move(pairs));

        auto* processor = MultiPutProcessor::instance(kv.get());
        auto f = processor->getFuture();
        processor->process(req);
        auto resp = std::move(f).get();
        ASSERT_EQ(resp.code, cpp2::ErrorCode::SUCCEEDED);
    }
    {
        // Get Test
        cpp2::GetReq req;
        req.set_key("key");

        auto* processor = GetProcessor::instance(kv.get());
        auto f = processor->getFuture();
        processor->process(req);
        auto resp = std::move(f).get();
        ASSERT_EQ(resp.code, cpp2::ErrorCode::SUCCEEDED);
        ASSERT_EQ(resp.value, "value");
    }
    {
        // Multi Get Test
        std::vector<std::string> keys;
        for (auto i = 0; i < 2; i++) {
            keys.emplace_back(std::move(folly::stringPrintf("key_%d", i)));
        }

        cpp2::MultiGetReq req;
        req.set_keys(std::move(keys));

        auto* processor = MultiGetProcessor::instance(kv.get());
        auto f = processor->getFuture();
        processor->process(req);
        auto resp = std::move(f).get();
        ASSERT_EQ(resp.code, cpp2::ErrorCode::SUCCEEDED);
        ASSERT_EQ(resp.values.size(), 2);
        ASSERT_EQ(resp.values[0], "value_0");
        ASSERT_EQ(resp.values[1], "value_1");
    }
    {
        // Scan Test
        cpp2::ScanReq req;
        req.set_start("key_1");
        req.set_end("key_4");
        auto* processor = ScanProcessor::instance(kv.get());
        auto f = processor->getFuture();
        processor->process(req);
        auto resp = std::move(f).get();
        ASSERT_EQ(resp.code, cpp2::ErrorCode::SUCCEEDED);
        ASSERT_EQ(resp.values.size(), 3);
        ASSERT_EQ(resp.values[0], "value_1");
        ASSERT_EQ(resp.values[1], "value_2");
        ASSERT_EQ(resp.values[2], "value_3");
    }
    {
        // Remove Test
        cpp2::RemoveReq req;
        req.set_key("key");

        auto* processor = RemoveProcessor::instance(kv.get());
        auto f = processor->getFuture();
        processor->process(req);
        auto resp = std::move(f).get();
        ASSERT_EQ(resp.code, cpp2::ErrorCode::SUCCEEDED);
    }
    {
        // Remove Range Test
        cpp2::RemoveRangeReq req;
        req.set_start("key_0");
        req.set_end("key_4");

        auto* processor = RemoveRangeProcessor::instance(kv.get());
        auto f = processor->getFuture();
        processor->process(req);
        auto resp = std::move(f).get();
        ASSERT_EQ(resp.code, cpp2::ErrorCode::SUCCEEDED);
    }
}

}  // namespace meta
}  // namespace nebula

int main(int argc, char** argv) {
    testing::InitGoogleTest(&argc, argv);
    folly::init(&argc, &argv, true);
    google::SetStderrLogging(google::INFO);
    return RUN_ALL_TESTS();
}


