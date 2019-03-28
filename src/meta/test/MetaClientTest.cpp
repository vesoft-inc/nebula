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
        auto ret = client->dropSpace("default_space");
        ASSERT_EQ(Status::OK(), ret);
        auto ret1 = client->listSpaces();
        ASSERT_TRUE(ret1.ok()) << ret1.status();
        ASSERT_EQ(ret1.value().size(), 0);
    }
    {
        std::vector<HostAddr> hosts = {{0, 0}, {1, 1}, {2, 2}, {3, 3}};
        ASSERT_EQ(Status::OK(), client->removeHosts(hosts));
        auto ret = client->listHosts();
        ASSERT_TRUE(ret.ok());
        ASSERT_EQ(ret.value().size(), 0);
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
        ASSERT_EQ(Status::OK(), client->addHosts(hosts));
        auto ret = client->listHosts();
        ASSERT_TRUE(ret.ok());
        ASSERT_EQ(hosts, ret.value());
    }
    {
        auto ret = client->createSpace("default_space", 9, 1);
        ASSERT_TRUE(ret.ok()) << ret.status();
    }
    sleep(FLAGS_load_data_interval_second + 1);
    ASSERT_EQ(listener->spaceNum, 1);
    ASSERT_EQ(listener->partNum, 9);
    {
        auto ret = client->createSpace("default_space_1", 5, 1);
        ASSERT_TRUE(ret.ok()) << ret.status();
    }
    sleep(FLAGS_load_data_interval_second + 1);
    ASSERT_EQ(listener->spaceNum, 2);
    ASSERT_EQ(listener->partNum, 14);
}

}  // namespace meta
}  // namespace nebula


int main(int argc, char** argv) {
    testing::InitGoogleTest(&argc, argv);
    folly::init(&argc, &argv, true);
    google::SetStderrLogging(google::INFO);
    return RUN_ALL_TESTS();
}


