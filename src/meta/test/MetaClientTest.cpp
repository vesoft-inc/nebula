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
#include "meta/MetaServiceUtils.h"

DECLARE_int32(load_data_interval_second);

namespace nebula {
namespace meta {

TEST(MetaClientTest, InterfacesTest) {
    FLAGS_load_data_interval_second = 1;
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
        auto r = client->addHosts(hosts).get();
        ASSERT_TRUE(r.ok());
        auto ret = client->listHosts().get();
        ASSERT_TRUE(ret.ok());
        ASSERT_EQ(hosts, ret.value());
    }
    {
        // Test createSpace, listSpaces, getPartsAlloc.
        {
            auto ret = client->createSpace("default_space", 9, 3).get();
            ASSERT_TRUE(ret.ok()) << ret.status();
            spaceId = ret.value();
        }
        {
            auto ret = client->listSpaces().get();
            ASSERT_TRUE(ret.ok()) << ret.status();
            ASSERT_EQ(1, ret.value().size());
            ASSERT_EQ(1, ret.value()[0].first);
            ASSERT_EQ("default_space", ret.value()[0].second);
        }
        {
            auto ret = client->getPartsAlloc(spaceId).get();
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
    sleep(FLAGS_load_data_interval_second + 1);
    {
        // Test cache interfaces
        // For Host(0, 0) the parts should be 2, 3, 4, 6, 7, 8
        auto partsMap = client->getPartsMapFromCache(HostAddr(0, 0));
        ASSERT_EQ(1, partsMap.size());
        ASSERT_EQ(6, partsMap[spaceId].size());
    }
    {
        auto partMeta = client->getPartMetaFromCache(spaceId, 1);
        int32_t startIndex = 1;
        for (auto& h : partMeta.peers_) {
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
        // Multi Put Test
        std::vector<std::pair<std::string, std::string>> pairs;
        for (auto i = 0; i < 10; i++) {
            pairs.emplace_back(folly::stringPrintf("key_%d", i),
                               folly::stringPrintf("value_%d", i));
        }
        auto ret = client->multiPut("test", pairs).get();
        ASSERT_TRUE(ret.ok());
    }
    {
        // Get Test
        auto ret = client->get("test", "key_0").get();
        ASSERT_TRUE(ret.ok());
        ASSERT_EQ("value_0", ret.value());

        auto missedRet = client->get("test", "missed_key").get();
        ASSERT_FALSE(missedRet.ok());

        auto emptyRet = client->get("test", "").get();
        ASSERT_FALSE(emptyRet.ok());
    }
    {
        // Multi Get Test
        std::vector<std::string> keys;
        for (auto i = 0; i < 2; i++) {
            keys.emplace_back(folly::stringPrintf("key_%d", i));
        }
        auto ret = client->multiGet("test", keys).get();
        ASSERT_TRUE(ret.ok());
        ASSERT_EQ(2, ret.value().size());
        ASSERT_EQ("value_0", ret.value()[0]);
        ASSERT_EQ("value_1", ret.value()[1]);

        std::vector<std::string> emptyKeys;
        auto emptyRet = client->multiGet("test", emptyKeys).get();
        ASSERT_FALSE(emptyRet.ok());
    }
    {
        // Scan Test
        auto ret = client->scan("test", "key_0", "key_3").get();
        ASSERT_TRUE(ret.ok());
        ASSERT_EQ(3, ret.value().size());
        ASSERT_EQ("value_0", ret.value()["testkey_0"]);
        ASSERT_EQ("value_1", ret.value()["testkey_1"]);
        ASSERT_EQ("value_2", ret.value()["testkey_2"]);
    }
    {
        // Partial Scan Key Test
        auto ret = client->partialScan("test", "key_0", "key_3", "key").get();
        ASSERT_TRUE(ret.ok());
        ASSERT_EQ(3, ret.value().size());
        ASSERT_EQ("testkey_0", ret.value()[0]);
        ASSERT_EQ("testkey_1", ret.value()[1]);
        ASSERT_EQ("testkey_2", ret.value()[2]);
    }
    {
        // Partial Scan Value Test
        auto ret = client->partialScan("test", "key_0", "key_3", "value").get();
        ASSERT_TRUE(ret.ok());
        ASSERT_EQ(3, ret.value().size());
        ASSERT_EQ("value_0", ret.value()[0]);
        ASSERT_EQ("value_1", ret.value()[1]);
        ASSERT_EQ("value_2", ret.value()[2]);
    }
    {
        // Partial Scan Failed Test
        auto ret = client->partialScan("test", "key_0", "key_3", "missed_type").get();
        ASSERT_FALSE(ret.ok());
    }
    {
        // Remove Test
        auto ret = client->remove("test", "key_9").get();
        ASSERT_TRUE(ret.ok());
    }
    {
        // Remove Range Test
        auto ret = client->removeRange("test", "key_0", "key_4").get();
        ASSERT_TRUE(ret.ok());
    }
    {
        auto ret = client->remove("_test_", "key_8").get();
        ASSERT_FALSE(ret.ok());
    }
    {
        auto ret = client->dropSpace("default_space").get();
        ASSERT_TRUE(ret.ok());
        auto ret1 = client->listSpaces().get();
        ASSERT_TRUE(ret1.ok()) << ret1.status();
        ASSERT_EQ(0, ret1.value().size());
    }
    {
        std::vector<HostAddr> hosts = {{0, 0}, {1, 1}, {2, 2}, {3, 3}};
        auto ret = client->removeHosts(hosts).get();
        ASSERT_TRUE(ret.ok());
        auto ret1 = client->listHosts().get();
        ASSERT_TRUE(ret1.ok());
        ASSERT_EQ(0, ret1.value().size());
    }
    client.reset();
}

class TestListener : public MetaChangedListener {
public:
    void onSpaceAdded(GraphSpaceID spaceId) override {
        LOG(INFO) << "Space " << spaceId << " added";
        spaceNum++;
    }

    void onSpaceRemoved(GraphSpaceID spaceId) override {
        LOG(INFO) << "Space " << spaceId << " removed";
        spaceNum--;
    }

    void onPartAdded(const PartMeta& partMeta) override {
        LOG(INFO) << "[" << partMeta.spaceId_ << ", " << partMeta.partId_ << "] added!";
        partNum++;
    }

    void onPartRemoved(GraphSpaceID spaceId, PartitionID partId) override {
        LOG(INFO) << "[" << spaceId << ", " << partId << "] removed!";
        partNum--;
    }

    void onPartUpdated(const PartMeta& partMeta) override {
        LOG(INFO) << "[" << partMeta.spaceId_ << ", " << partMeta.partId_ << "] updated!";
        partChanged++;
    }

    HostAddr getLocalHost() override {
        return HostAddr(0, 0);
    }

    int32_t spaceNum = 0;
    int32_t partNum = 0;
    int32_t partChanged = 0;
};

TEST(MetaClientTest, DiffTest) {
    FLAGS_load_data_interval_second = 1;
    fs::TempDir rootPath("/tmp/MetaClientTest.XXXXXX");
    auto sc = TestUtils::mockServer(10001, rootPath.path());

    auto threadPool = std::make_shared<folly::IOThreadPoolExecutor>(1);
    uint32_t localIp;
    network::NetworkUtils::ipv4ToInt("127.0.0.1", localIp);
    auto listener = std::make_unique<TestListener>();
    auto client = std::make_shared<MetaClient>(threadPool,
                                               std::vector<HostAddr>{HostAddr(localIp, 10001)});
    client->registerListener(listener.get());
    client->init();
    {
        // Test addHost, listHosts interface.
        std::vector<HostAddr> hosts = {{0, 0}};
        auto r = client->addHosts(hosts).get();
        ASSERT_TRUE(r.ok());
        auto ret = client->listHosts().get();
        ASSERT_TRUE(ret.ok());
        ASSERT_EQ(hosts, ret.value());
    }
    {
        auto ret = client->createSpace("default_space", 9, 1).get();
        ASSERT_TRUE(ret.ok()) << ret.status();
    }
    sleep(FLAGS_load_data_interval_second + 1);
    ASSERT_EQ(1, listener->spaceNum);
    ASSERT_EQ(9, listener->partNum);
    {
        auto ret = client->createSpace("default_space_1", 5, 1).get();
        ASSERT_TRUE(ret.ok()) << ret.status();
    }
    sleep(FLAGS_load_data_interval_second + 1);
    ASSERT_EQ(2, listener->spaceNum);
    ASSERT_EQ(14, listener->partNum);
}

}  // namespace meta
}  // namespace nebula


int main(int argc, char** argv) {
    testing::InitGoogleTest(&argc, argv);
    folly::init(&argc, &argv, true);
    google::SetStderrLogging(google::INFO);
    return RUN_ALL_TESTS();
}


