/* Copyright (c) 2018 - present, VE Software Inc. All rights reserved
 *
 * This source code is licensed under Apache 2.0 License
 *  (found in the LICENSE.Apache file in the root directory)
 */
#include "base/Base.h"
#include <gtest/gtest.h>
#include <rocksdb/db.h>
#include "fs/TempDir.h"
#include "meta/client/MetaClient.h"
#include "meta/test/TestUtils.h"
#include "network/NetworkUtils.h"
#include "meta/MetaUtils.h"

namespace nebula {
namespace meta {

TEST(MetaClientTest, InterfacesTest) {
    fs::TempDir rootPath("/tmp/MetaClientTest.XXXXXX");
    auto sc = TestUtils::mockServer(10001, rootPath.path());

    GraphSpaceID spaceId = 0;
    auto threadPool = std::make_shared<folly::IOThreadPoolExecutor>(1);
    uint32_t localIp;
    network::NetworkUtils::ipv4ToInt("127.0.0.1", localIp);
    auto client = std::make_shared<MetaClient>(threadPool,
                                            std::vector<HostAddr>{HostAddr(localIp, 10001)});
    client->init();
    {
        // Test addHost, listHosts interface.
        std::vector<HostAddr> hosts = {{0, 0}, {1, 1}, {2, 2}, {3, 3}};
        ASSERT_EQ(Status::OK(), client->addHosts(hosts));
        auto ret = client->listHosts();
        ASSERT_TRUE(ret.ok());
        ASSERT_EQ(hosts, ret.value());
    }
    {
        // Test createSpace, listSpaces, getPartsAlloc.
        {
            auto ret = client->createSpace("default_space", 9, 3);
            ASSERT_TRUE(ret.ok()) << ret.status();
            spaceId = ret.value();
        }
        {
            auto ret = client->listSpaces();
            ASSERT_TRUE(ret.ok()) << ret.status();
            ASSERT_EQ(ret.value().size(), 1);
            ASSERT_EQ(ret.value()[0].first, 1);
            ASSERT_EQ(ret.value()[0].second, "default_space");
        }
        {
            auto ret = client->getPartsAlloc(spaceId);
            ASSERT_TRUE(ret.ok()) << ret.status();
            for (auto it = ret.value().begin(); it != ret.value().end(); it++) {
                auto startIndex = it->first;
                for (auto& h : it->second) {
                    ASSERT_EQ(startIndex++ % 4, h.first);
                    ASSERT_EQ(h.first, h.second);
                }
            }
        }
    }
    // waiting for the cache being filled.
    sleep(1);
    {
        // Test cache interfaces
        // For Host(0, 0) the parts should be 2, 3, 4, 6, 7, 8
        auto ret = client->getPartsFromCache(spaceId, HostAddr(0, 0));
        ASSERT_TRUE(ret.ok()) << ret.status();
        int32_t index = 0;
        ASSERT_EQ(6, ret.value().size());
        std::vector<PartitionID> expected = {8, 7, 6, 4, 3, 2};
        for (auto partId : ret.value()) {
            EXPECT_EQ(expected[index++], partId);
        }
    }
    {
        auto ret = client->getHostsFromCache(spaceId, 1);
        ASSERT_TRUE(ret.ok()) << ret.status();
        int32_t startIndex = 1;
        for (auto& h : ret.value()) {
            ASSERT_EQ(startIndex++ % 4, h.first);
            ASSERT_EQ(h.first, h.second);
        }
    }
    {
        auto ret = client->getSpaceIdByNameFromCache("default_space");
        ASSERT_TRUE(ret.ok()) << ret.status();
        ASSERT_EQ(1, ret.value());
    }
    {
        auto ret = client->getSpaceIdByNameFromCache("default_space_1");
        ASSERT_FALSE(ret.ok());
        ASSERT_EQ(Status::SpaceNotFound(), ret.status());
    }
    {
        // Put Test
        auto ret = client->put("key", "value");
        ASSERT_TRUE(ret.ok());
    }
    {
        // Multi Put Test
        std::vector<std::pair<std::string, std::string>> pairs;
        for (auto i = 0; i < 10; i++) {
            pairs.emplace_back(folly::stringPrintf("key_%d", i),
                               folly::stringPrintf("value_%d", i));
        }
        auto ret = client->multiPut(pairs);
        ASSERT_TRUE(ret.ok());
    }
    {
        // Get Test
        auto ret = client->get("key");
        ASSERT_TRUE(ret.ok());
        ASSERT_EQ(ret.value(), "value");
    }
    {
        // Multi Get Test
        std::vector<std::string> keys;
        for (auto i = 0; i < 2; i++) {
            keys.emplace_back(std::move(folly::stringPrintf("key_%d", i)));
        }
        auto ret = client->multiGet(keys);
        ASSERT_TRUE(ret.ok());
        ASSERT_EQ(ret.value().size(), 2);
        ASSERT_EQ(ret.value()[0], "value_0");
        ASSERT_EQ(ret.value()[1], "value_1");
    }
    {
        // Scan Test
        auto ret = client->scan("key_0", "key_3");
        ASSERT_TRUE(ret.ok());
        ASSERT_EQ(ret.value().size(), 3);
        ASSERT_EQ(ret.value()[0], "value_0");
        ASSERT_EQ(ret.value()[1], "value_1");
        ASSERT_EQ(ret.value()[2], "value_2");
    }
    {
        // Remove Test
        auto ret = client->remove("key");
        ASSERT_TRUE(ret.ok());
    }
    {
        // Remove Range Test
        auto ret = client->removeRange("key_0", "key_4");
        ASSERT_TRUE(ret.ok());
    }
    client.reset();
}

}  // namespace meta
}  // namespace nebula


int main(int argc, char** argv) {
    testing::InitGoogleTest(&argc, argv);
    folly::init(&argc, &argv, true);
    google::SetStderrLogging(google::INFO);
    return RUN_ALL_TESTS();
}


