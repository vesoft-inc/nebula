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
#include "meta/ListChildrenProcessor.h"


namespace nebula {
namespace meta {

TEST(ListChildrenTest, SimpleTest) {
    fs::TempDir rootPath("/tmp/ListChildrenTest.XXXXXX");
    std::unique_ptr<kvstore::KVStore> kv(TestUtils::initKV(rootPath.path()));
    folly::RWSpinLock lock;
    TestUtils::createSomeNodes(kv.get(), &lock,
                               {"/", "/a", "/b", "/c",
                                "/a/aa", "/a/ab", "/a/ac",
                                "/b/bb", "/b/bc", "/c/cc"});
    {
        auto* processor = ListChildrenProcessor::instance(kv.get(), &lock);
        cpp2::ListChildrenRequest req;
        req.set_path("/a");
        auto f = processor->getFuture();
        processor->process(req);
        auto resp = std::move(f).get();
        EXPECT_EQ(resp.code, cpp2::ErrorCode::SUCCEEDED);
        EXPECT_EQ(3, resp.children.size());
        EXPECT_EQ("/a/aa", resp.children[0]);
        EXPECT_EQ("/a/ab", resp.children[1]);
        EXPECT_EQ("/a/ac", resp.children[2]);
    }
    {
        auto* processor = ListChildrenProcessor::instance(kv.get(), &lock);
        cpp2::ListChildrenRequest req;
        req.set_path("/a/b/c");
        auto f = processor->getFuture();
        processor->process(req);
        auto resp = std::move(f).get();
        EXPECT_EQ(resp.code, cpp2::ErrorCode::E_NODE_NOT_FOUND);
    }
    {
        auto* processor = ListChildrenProcessor::instance(kv.get(), &lock);
        cpp2::ListChildrenRequest req;
        req.set_path("///b");
        auto f = processor->getFuture();
        processor->process(req);
        auto resp = std::move(f).get();
        EXPECT_EQ(resp.code, cpp2::ErrorCode::SUCCEEDED);
        EXPECT_EQ(2, resp.children.size());
        EXPECT_EQ("/b/bb", resp.children[0]);
        EXPECT_EQ("/b/bc", resp.children[1]);
    }
    {
        auto* processor = ListChildrenProcessor::instance(kv.get(), &lock);
        cpp2::ListChildrenRequest req;
        req.set_path("/");
        auto f = processor->getFuture();
        processor->process(req);
        auto resp = std::move(f).get();
        EXPECT_EQ(resp.code, cpp2::ErrorCode::SUCCEEDED);
        EXPECT_EQ(3, resp.children.size());
        EXPECT_EQ("/a", resp.children[0]);
        EXPECT_EQ("/b", resp.children[1]);
        EXPECT_EQ("/c", resp.children[2]);
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


