/* Copyright (c) 2018 - present, VE Software Inc. All rights reserved
 *
 * This source code is licensed under Apache 2.0 License
 *  (found in the LICENSE.Apache file in the root directory)
 */

#include "base/Base.h"
#include <gtest/gtest.h>
#include <rocksdb/db.h>
#include "fs/TempDir.h"
#include "meta/MetaClient.h"
#include "meta/test/TestUtils.h"
#include "network/NetworkUtils.h"
#include "meta/MetaUtils.h"

namespace nebula {
namespace meta {

TEST(MetaClientTest, SimpleTest) {
    fs::TempDir rootPath("/tmp/MetaClientTest.XXXXXX");
    auto sc = TestUtils::mockServer(10001, rootPath.path());

    auto threadPool = std::make_shared<folly::IOThreadPoolExecutor>(1);
    uint32_t localIp;
    network::NetworkUtils::ipv4ToInt("127.0.0.1", localIp);
    auto client = std::make_shared<MetaClient>(threadPool,
                                               std::vector<HostAddr>{HostAddr(localIp, 10001)});
    std::vector<std::string> paths = {"/", "/abc", "/abc/abc"};
    for (auto& path : paths) {
        ASSERT_EQ(Status::OK(),
                  client->createNode(path,
                      folly::stringPrintf("%s_%d", path.c_str(), MetaUtils::layer(path))));
        auto ret = client->getNode(path);
        ASSERT_TRUE(ret.ok());
        ASSERT_EQ(folly::stringPrintf("%s_%d", path.c_str(), MetaUtils::layer(path)), ret.value());
    }

    auto ret = client->listChildren("/");
    ASSERT_TRUE(ret.ok());
    ASSERT_EQ(1, ret.value().size());
    ASSERT_EQ("/abc", ret.value()[0]);
}


}  // namespace meta
}  // namespace nebula


int main(int argc, char** argv) {
    testing::InitGoogleTest(&argc, argv);
    folly::init(&argc, &argv, true);
    google::SetStderrLogging(google::INFO);
    return RUN_ALL_TESTS();
}


