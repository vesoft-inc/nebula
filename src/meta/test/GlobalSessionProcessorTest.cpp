/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "base/Base.h"
#include <gtest/gtest.h>
#include <folly/String.h>
#include <fstream>
#include "fs/TempDir.h"
#include "meta/test/TestUtils.h"
#include "meta/processors/sessionMan/GlobalSessionProcessor.h"

DECLARE_int32(heartbeat_interval_secs);

namespace nebula {
namespace meta {

TEST(GlobalSessionProcessorTest, SessionTest) {
    fs::TempDir rootPath("/tmp/SessionTest.XXXXXX");
    std::unique_ptr<kvstore::KVStore> kv(TestUtils::initKV(rootPath.path()));
    // Add session
    {
        cpp2::AddSessionReq req;
        std::vector<cpp2::SessionItem> items;
        cpp2::SessionItem item;
        item.set_addr("127.0.0.1:1000");
        item.set_session_id(1);
        item.set_start_time(1586270916);
        items.emplace_back(item);
        req.set_session_items(items);

        auto* processor = AddSessionProcessor::instance(kv.get());
        auto f = processor->getFuture();
        processor->process(req);
        auto resp = std::move(f).get();
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, resp.get_code());
    }
    {
        cpp2::AddSessionReq req;
        std::vector<cpp2::SessionItem> items;
        cpp2::SessionItem item;
        item.set_addr("127.0.0.1:1000");
        item.set_session_id(2);
        item.set_start_time(1586270926);
        items.emplace_back(item);
        req.set_session_items(items);

        auto* processor = AddSessionProcessor::instance(kv.get());
        auto f = processor->getFuture();
        processor->process(req);
        auto resp = std::move(f).get();
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, resp.get_code());
    }

    // List session
    {
        cpp2::ListSessionsReq req;
        auto* processor = ListSessionsProcessor::instance(kv.get());
        auto f = processor->getFuture();
        processor->process(req);
        auto resp = std::move(f).get();
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, resp.get_code());
        auto items = resp.get_items();
        ASSERT_EQ(2, items.size());

        std::sort(items.begin(), items.end(), [] (const auto& a, const auto& b) {
            // sort with addr and ip
            if (a.get_addr() == b.get_addr()) {
                return a.get_session_id() < b.get_session_id();
            }
            return a.get_addr() < b.get_addr();
        });

        {
            auto singleItem = items[0];
            ASSERT_EQ("127.0.0.1:1000", singleItem.get_addr());
            ASSERT_EQ(1, singleItem.get_session_id());
            ASSERT_EQ(1586270916, singleItem.get_start_time());
        }
        {
            auto singleItem = items[1];
            ASSERT_EQ("127.0.0.1:1000", singleItem.get_addr());
            ASSERT_EQ(2, singleItem.get_session_id());
            ASSERT_EQ(1586270926, singleItem.get_start_time());
        }
    }

    sleep(FLAGS_heartbeat_interval_secs * 3);
    // Update session
    {
        cpp2::UpdateSessionReq req;
        std::vector<cpp2::SessionItem> items;
        cpp2::SessionItem item;
        item.set_addr("127.0.0.1:1000");
        item.set_session_id(2);
        item.set_start_time(1586270926);
        items.emplace_back(item);

        cpp2::SessionItem item2;
        item2.set_addr("127.0.0.1:1000");
        item2.set_session_id(3);
        item2.set_start_time(1586270976);
        items.emplace_back(item2);

        req.set_session_items(items);

        auto* processor = UpdateSessionProcessor::instance(kv.get());
        auto f = processor->getFuture();
        processor->process(req);
        auto resp = std::move(f).get();
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, resp.get_code());
    }

    // List session
    {
        cpp2::ListSessionsReq req;
        auto* processor = ListSessionsProcessor::instance(kv.get());
        auto f = processor->getFuture();
        processor->process(req);
        auto resp = std::move(f).get();
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, resp.get_code());
        auto items = resp.get_items();
        ASSERT_EQ(2, items.size());

        std::sort(items.begin(), items.end(), [] (const auto& a, const auto& b) {
            // sort with addr and ip
            if (a.get_addr() == b.get_addr()) {
                return a.get_session_id() < b.get_session_id();
            }
            return a.get_addr() < b.get_addr();
        });

        {
            auto singleItem = items[0];
            ASSERT_EQ("127.0.0.1:1000", singleItem.get_addr());
            ASSERT_EQ(2, singleItem.get_session_id());
            ASSERT_EQ(1586270926, singleItem.get_start_time());
        }
        {
            auto singleItem = items[1];
            ASSERT_EQ("127.0.0.1:1000", singleItem.get_addr());
            ASSERT_EQ(3, singleItem.get_session_id());
            ASSERT_EQ(1586270976, singleItem.get_start_time());
        }
    }

    // Remove session
    {
        cpp2::RemoveSessionReq req;
        std::vector<cpp2::SessionItem> items;
        cpp2::SessionItem item;
        item.set_addr("127.0.0.1:1000");
        item.set_session_id(2);
        item.set_start_time(1586270926);
        items.emplace_back(item);
        req.set_session_items(items);

        auto* processor = RemoveSessionProcessor::instance(kv.get());
        auto f = processor->getFuture();
        processor->process(req);
        auto resp = std::move(f).get();
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, resp.get_code());
    }

    // List session
    {
        cpp2::ListSessionsReq req;
        auto* processor = ListSessionsProcessor::instance(kv.get());
        auto f = processor->getFuture();
        processor->process(req);
        auto resp = std::move(f).get();
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, resp.get_code());
        auto items = resp.get_items();
        ASSERT_EQ(1, items.size());
        {
            auto singleItem = items[0];
            ASSERT_EQ("127.0.0.1:1000", singleItem.get_addr());
            ASSERT_EQ(3, singleItem.get_session_id());
            ASSERT_EQ(1586270976, singleItem.get_start_time());
        }
    }

    // Remove session
    {
        cpp2::RemoveSessionReq req;
        std::vector<cpp2::SessionItem> items;
        cpp2::SessionItem item;
        item.set_addr("127.0.0.1:1000");
        item.set_session_id(3);
        item.set_start_time(1586270976);
        items.emplace_back(item);
        req.set_session_items(items);

        auto* processor = RemoveSessionProcessor::instance(kv.get());
        auto f = processor->getFuture();
        processor->process(req);
        auto resp = std::move(f).get();
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, resp.get_code());
    }

    // List session
    {
        cpp2::ListSessionsReq req;
        auto* processor = ListSessionsProcessor::instance(kv.get());
        auto f = processor->getFuture();
        processor->process(req);
        auto resp = std::move(f).get();
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, resp.get_code());
        auto items = resp.get_items();
        ASSERT_EQ(0, items.size());
    }
}

}   // namespace meta
}   // namespace nebula

int main(int argc, char** argv) {
    testing::InitGoogleTest(&argc, argv);
    folly::init(&argc, &argv, true);
    google::SetStderrLogging(google::INFO);
    return RUN_ALL_TESTS();
}
