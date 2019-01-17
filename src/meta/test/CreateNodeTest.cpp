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
#include "meta/CreateNodeProcessor.h"


namespace nebula {
namespace meta {

TEST(CreateNodeTest, SimpleTest) {
    fs::TempDir rootPath("/tmp/CreateNodeTest.XXXXXX");
    std::unique_ptr<kvstore::KVStore> kv(TestUtils::initKV(rootPath.path()));
    folly::RWSpinLock lock;
    {
        auto* processor = CreateNodeProcessor::instance(kv.get(), &lock);
        cpp2::CreateNodeRequest req;
        req.set_path("/");
        req.set_value(folly::stringPrintf("%d_%s", 0, "/"));
        auto f = processor->getFuture();
        processor->process(req);
        auto resp = std::move(f).get();
        EXPECT_EQ(resp.code, cpp2::ErrorCode::SUCCEEDED);
        std::string val;
        CHECK_EQ(kvstore::ResultCode::SUCCESSED,
                 kv->get(0, 0, MetaUtils::metaKey(0, "/"), &val));
        CHECK_EQ(folly::stringPrintf("%d_%s", 0, "/"), val);
    }
    {
        auto* processor = CreateNodeProcessor::instance(kv.get(), &lock);
        cpp2::CreateNodeRequest req;
        req.set_path("/a/b/c");
        req.set_value(folly::stringPrintf("%d_%s", 3, "/a/b/c"));
        auto f = processor->getFuture();
        processor->process(req);
        auto resp = std::move(f).get();
        EXPECT_EQ(resp.code, cpp2::ErrorCode::E_NODE_NOT_FOUND);
    }
    {
        auto* processor = CreateNodeProcessor::instance(kv.get(), &lock);
        cpp2::CreateNodeRequest req;
        req.set_path("/a");
        req.set_value(folly::stringPrintf("%d_%s", 1, "/a"));
        auto f = processor->getFuture();
        processor->process(req);
        auto resp = std::move(f).get();
        EXPECT_EQ(resp.code, cpp2::ErrorCode::SUCCEEDED);
    }
    {
        auto* processor = CreateNodeProcessor::instance(kv.get(), &lock);
        cpp2::CreateNodeRequest req;
        req.set_path("/a");
        req.set_value(folly::stringPrintf("%d_%s", 1, "/a"));
        auto f = processor->getFuture();
        processor->process(req);
        auto resp = std::move(f).get();
        EXPECT_EQ(resp.code, cpp2::ErrorCode::E_NODE_EXISTED);
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


