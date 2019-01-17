/* Copyright (c) 2018 - present, VE Software Inc. All rights reserved
 *
 * This source code is licensed under Apache 2.0 License
 *  (found in the LICENSE.Apache file in the root directory)
 */

#include "base/Base.h"
#include <gtest/gtest.h>
#include <rocksdb/db.h>
#include "fs/TempDir.h"
#include "meta/test/TestUtils.h"
#include "meta/RemoveNodeProcessor.h"
#include "meta/GetNodeProcessor.h"

namespace nebula {
namespace meta {

TEST(RemoveNodeTest, SimpleTest) {
    fs::TempDir rootPath("/tmp/RemoveNodeTest.XXXXXX");
    std::unique_ptr<kvstore::KVStore> kv(TestUtils::initKV(rootPath.path()));
    folly::RWSpinLock lock;
    TestUtils::createSomeNodes(kv.get(), &lock, {"/", "/abc", "/abc/abc"});
    {
        auto* processor = RemoveNodeProcessor::instance(kv.get(), &lock);
        cpp2::RemoveNodeRequest req;
        req.set_path("/");
        auto f = processor->getFuture();
        processor->process(req);
        auto resp = std::move(f).get();
        EXPECT_EQ(resp.code, cpp2::ErrorCode::E_CHILD_EXISTED);
    }
    {
        {
            auto* processor = RemoveNodeProcessor::instance(kv.get(), &lock);
            cpp2::RemoveNodeRequest req;
            req.set_path("/abc/abc");
            auto f = processor->getFuture();
            processor->process(req);
            auto resp = std::move(f).get();
            EXPECT_EQ(resp.code, cpp2::ErrorCode::SUCCEEDED);
        }
        {
            auto* processor = GetNodeProcessor::instance(kv.get(), &lock);
            cpp2::GetNodeRequest req;
            req.set_path("/abc/abc");
            auto f = processor->getFuture();
            processor->process(req);
            auto resp = std::move(f).get();
            EXPECT_EQ(resp.code, cpp2::ErrorCode::E_NODE_NOT_FOUND);
        }
    }
    {
        auto* processor = RemoveNodeProcessor::instance(kv.get(), &lock);
        cpp2::RemoveNodeRequest req;
        req.set_path("/abc/d");
        auto f = processor->getFuture();
        processor->process(req);
        auto resp = std::move(f).get();
        EXPECT_EQ(resp.code, cpp2::ErrorCode::E_NODE_NOT_FOUND);
    }
    {
        auto* processor = RemoveNodeProcessor::instance(kv.get(), &lock);
        cpp2::RemoveNodeRequest req;
        req.set_path("////abc//");
        auto f = processor->getFuture();
        processor->process(req);
        auto resp = std::move(f).get();
        EXPECT_EQ(resp.code, cpp2::ErrorCode::SUCCEEDED);
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


