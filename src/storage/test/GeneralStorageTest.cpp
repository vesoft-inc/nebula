/* Copyright (c) 2019 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "base/Base.h"
#include <gtest/gtest.h>
#include "fs/TempDir.h"
#include "storage/test/TestUtils.h"
#include "storage/PutProcessor.h"
#include "storage/GetProcessor.h"
#include "storage/RemoveProcessor.h"
#include "storage/RemoveRangeProcessor.h"
#include "storage/PrefixProcessor.h"
#include "storage/ScanProcessor.h"

namespace nebula {
namespace storage {

TEST(GeneralStorageTest, SimpleTest) {
    fs::TempDir rootPath("/tmp/GeneralStorageTest.XXXXXX");
    auto kv = TestUtils::initKV(rootPath.path());
    GraphSpaceID space = 0;

    {
        cpp2::PutRequest req;
        req.set_space_id(space);
        auto* processor = PutProcessor::instance(kv.get(), nullptr);
        for (PartitionID part = 1; part <= 3; part++) {
            std::vector<nebula::cpp2::Pair> pairs;
            for (int32_t i = 0; i < 10; i++) {
                pairs.emplace_back(apache::thrift::FragileConstructor::FRAGILE,
                                   folly::stringPrintf("key_%d_%d", part, i),
                                   folly::stringPrintf("value_%d_%d", part, i));
            }
            req.parts.emplace(part, std::move(pairs));
        }
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
