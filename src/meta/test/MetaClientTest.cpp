/* Copyright (c) 2018 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "base/Base.h"
#include <gtest/gtest.h>
#include <rocksdb/db.h>
#include "fs/TempDir.h"
#include "meta/client/MetaClient.h"
#include "meta/test/TestUtils.h"
#include "network/NetworkUtils.h"
#include "meta/MetaServiceUtils.h"
#include "meta/ServerBasedSchemaManager.h"
#include "dataman/ResultSchemaProvider.h"
#include "meta/test/TestUtils.h"
#include "meta/ClusterManager.h"

DECLARE_int32(load_data_interval_secs);
DECLARE_int32(heartbeat_interval_secs);


namespace nebula {
namespace meta {

using nebula::cpp2::SupportedType;
using nebula::cpp2::ValueType;
using apache::thrift::FragileConstructor::FRAGILE;

TEST(MetaClientTest, InterfacesTest) {
    FLAGS_load_data_interval_secs = 1;
    fs::TempDir rootPath("/tmp/MetaClientTest.XXXXXX");

    // Let the system choose an available port for us
    uint32_t localMetaPort = 0;
    auto sc = TestUtils::mockMetaServer(localMetaPort, rootPath.path());

    GraphSpaceID spaceId = 0;
    auto threadPool = std::make_shared<folly::IOThreadPoolExecutor>(1);
    IPv4 localIp;
    network::NetworkUtils::ipv4ToInt("127.0.0.1", localIp);
    auto clientPort = network::NetworkUtils::getAvailablePort();
    HostAddr localHost{localIp, clientPort};
    auto client = std::make_shared<MetaClient>(threadPool,
                                               std::vector<HostAddr>{HostAddr(localIp, sc->port_)},
                                               localHost);
    client->waitForMetadReady();
    {
        // Test addHost, listHosts interface.
        std::vector<HostAddr> hosts = {{0, 0}, {1, 1}, {2, 2}, {3, 3}};
        auto r = client->addHosts(hosts).get();
        ASSERT_TRUE(r.ok());
        TestUtils::registerHB(hosts);
        auto ret = client->listHosts().get();
        ASSERT_TRUE(ret.ok());
        for (auto i = 0u; i < hosts.size(); i++) {
            ASSERT_EQ(hosts[i], ret.value()[i].first);
        }
    }
    {
        // Test createSpace, listSpaces, getPartsAlloc.
        {
            auto ret = client->createSpace("default_space", 8, 3).get();
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
            ASSERT_EQ(8, ret.value().size());
            for (auto it = ret.value().begin(); it != ret.value().end(); it++) {
                ASSERT_EQ(3, it->second.size());
            }
        }
        {
            // createTagSchema
            nebula::cpp2::Schema schema;
            for (auto i = 0 ; i < 5; i++) {
                nebula::cpp2::ColumnDef column;
                column.name = "tagItem" + std::to_string(i);
                column.type.type = nebula::cpp2::SupportedType::STRING;
                schema.columns.emplace_back(std::move(column));
            }
            auto ret = client->createTagSchema(spaceId, "tagName", schema).get();
            ASSERT_TRUE(ret.ok()) << ret.status();
        }
        {
            // createEdgeSchema
            nebula::cpp2::Schema schema;
            for (auto i = 0 ; i < 5; i++) {
                nebula::cpp2::ColumnDef column;
                column.name = "edgeItem" + std::to_string(i);
                column.type.type = nebula::cpp2::SupportedType::STRING;
                schema.columns.emplace_back(std::move(column));
            }
            auto ret = client->createEdgeSchema(spaceId, "edgeName", schema).get();
            ASSERT_TRUE(ret.ok()) << ret.status();
        }

        auto schemaMan = std::make_unique<ServerBasedSchemaManager>();
        schemaMan->init(client.get());
        {
            // listTagSchemas
            auto ret1 = client->listTagSchemas(spaceId).get();
            ASSERT_TRUE(ret1.ok()) << ret1.status();
            ASSERT_EQ(ret1.value().size(), 1);
            ASSERT_NE(ret1.value().begin()->tag_id, 0);
            ASSERT_EQ(ret1.value().begin()->schema.columns.size(), 5);

            // getTagSchemaFromCache
            sleep(FLAGS_load_data_interval_secs + 1);
            auto ret = client->getNewestTagVerFromCache(spaceId,
                                                        ret1.value().begin()->tag_id);
            CHECK(ret.ok());
            auto ver = ret.value();
            auto ret2 = client->getTagSchemaFromCache(spaceId,
                                                      ret1.value().begin()->tag_id, ver);
            ASSERT_TRUE(ret2.ok()) << ret2.status();
            ASSERT_EQ(ret2.value()->getNumFields(), 5);

            // ServerBasedSchemaManager test
            auto status = schemaMan->toTagID(spaceId, "tagName");
            ASSERT_TRUE(status.ok());
            auto tagId = status.value();
            ASSERT_NE(-1, tagId);
            auto outSchema = schemaMan->getTagSchema(spaceId, tagId);
            ASSERT_EQ(5, outSchema->getNumFields());
            ASSERT_STREQ("tagItem0", outSchema->getFieldName(0));
            ASSERT_EQ(nullptr, outSchema->getFieldName(-1));
            ASSERT_EQ(nullptr, outSchema->getFieldName(5));
            ASSERT_EQ(nullptr, outSchema->getFieldName(6));
            auto retVer = schemaMan->getNewestTagSchemaVer(spaceId, tagId);
            ASSERT_TRUE(retVer.ok());
            auto version = retVer.value();
            ASSERT_EQ(0, version);
            auto outSchema1 = schemaMan->getTagSchema(spaceId, tagId, version);
            ASSERT_TRUE(outSchema1 != nullptr);
            ASSERT_EQ(5, outSchema1->getNumFields());
            ASSERT_EQ(nullptr, outSchema1->getFieldName(-1));
            ASSERT_EQ(nullptr, outSchema1->getFieldName(5));
            ASSERT_EQ(nullptr, outSchema1->getFieldName(6));
            ASSERT_STREQ("tagItem0", outSchema1->getFieldName(0));
        }
        {
            // listEdgeSchemas
            auto ret1 = client->listEdgeSchemas(spaceId).get();
            ASSERT_TRUE(ret1.ok()) << ret1.status();
            ASSERT_EQ(ret1.value().size(), 1);
            ASSERT_NE(ret1.value().begin()->edge_type, 0);

            // getEdgeSchemaFromCache
            auto retVer = client->getNewestEdgeVerFromCache(spaceId,
                                                            ret1.value().begin()->edge_type);
            CHECK(retVer.ok());
            auto ver = retVer.value();
            auto ret2 = client->getEdgeSchemaFromCache(spaceId,
                                                       ret1.value().begin()->edge_type, ver);
            ASSERT_TRUE(ret2.ok()) << ret2.status();
            ASSERT_EQ(ret2.value()->getNumFields(), 5);

            // ServerBasedSchemaManager test
            auto status = schemaMan->toEdgeType(spaceId, "edgeName");
            auto edgeType = status.value();
            ASSERT_NE(-1, edgeType);
            auto outSchema = schemaMan->getEdgeSchema(spaceId, edgeType);
            ASSERT_EQ(5, outSchema->getNumFields());
            ASSERT_STREQ("edgeItem0", outSchema->getFieldName(0));
            ASSERT_EQ(nullptr, outSchema->getFieldName(-1));
            ASSERT_EQ(nullptr, outSchema->getFieldName(5));
            ASSERT_EQ(nullptr, outSchema->getFieldName(6));
            auto versionRet = schemaMan->getNewestEdgeSchemaVer(spaceId, edgeType);
            ASSERT_TRUE(versionRet.ok());
            auto version = versionRet.value();
            ASSERT_EQ(0, version);
            auto outSchema1 = schemaMan->getEdgeSchema(spaceId, edgeType, version);
            ASSERT_TRUE(outSchema1 != nullptr);
            ASSERT_EQ(5, outSchema1->getNumFields());
            ASSERT_EQ(nullptr, outSchema1->getFieldName(-1));
            ASSERT_EQ(nullptr, outSchema1->getFieldName(5));
            ASSERT_EQ(nullptr, outSchema1->getFieldName(6));
            ASSERT_STREQ("edgeItem0", outSchema1->getFieldName(0));
        }
    }
    sleep(FLAGS_load_data_interval_secs + 1);
    {
        // Test cache interfaces
        auto partsMap = client->getPartsMapFromCache(HostAddr(0, 0));
        ASSERT_EQ(1, partsMap.size());
        ASSERT_EQ(6, partsMap[spaceId].size());
    }
    {
        auto partMeta = client->getPartMetaFromCache(spaceId, 1);
        ASSERT_EQ(3, partMeta.peers_.size());
        for (auto& h : partMeta.peers_) {
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
        ASSERT_EQ("value_0", ret.value()[0]);
        ASSERT_EQ("value_1", ret.value()[1]);
        ASSERT_EQ("value_2", ret.value()[2]);
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

TEST(MetaClientTest, TagTest) {
    FLAGS_load_data_interval_secs = 1;
    fs::TempDir rootPath("/tmp/MetaClientTagTest.XXXXXX");

    // Let the system choose an available port for us
    int32_t localMetaPort = 0;
    auto sc = TestUtils::mockMetaServer(localMetaPort, rootPath.path());

    GraphSpaceID spaceId = 0;
    auto threadPool = std::make_shared<folly::IOThreadPoolExecutor>(1);
    IPv4 localIp;
    network::NetworkUtils::ipv4ToInt("127.0.0.1", localIp);
    auto localhosts = std::vector<HostAddr>{HostAddr(localIp, sc->port_)};
    auto client = std::make_shared<MetaClient>(threadPool, localhosts);
    std::vector<HostAddr> hosts = {{0, 0}, {1, 1}, {2, 2}, {3, 3}};
    auto r = client->addHosts(hosts).get();
    ASSERT_TRUE(r.ok());
    client->waitForMetadReady();
    TestUtils::registerHB(hosts);
    auto ret = client->createSpace("default_space", 9, 3).get();
    ASSERT_TRUE(ret.ok()) << ret.status();
    spaceId = ret.value();
    TagID id;
    int64_t version;

    {
        std::vector<nebula::cpp2::ColumnDef> columns;
        columns.emplace_back(FRAGILE, "column_i",
                             ValueType(FRAGILE, SupportedType::INT, nullptr, nullptr));
        columns.emplace_back(FRAGILE, "column_d",
                             ValueType(FRAGILE, SupportedType::DOUBLE, nullptr, nullptr));
        columns.emplace_back(FRAGILE, "column_s",
                             ValueType(FRAGILE, SupportedType::STRING, nullptr, nullptr));
        nebula::cpp2::Schema schema;
        auto result = client->createTagSchema(spaceId, "test_tag", schema).get();
        ASSERT_TRUE(result.ok());
        id = result.value();
    }
    {
        auto result = client->listTagSchemas(spaceId).get();
        ASSERT_TRUE(result.ok());
        auto tags = result.value();
        ASSERT_EQ(1, tags.size());
        ASSERT_EQ(id, tags[0].get_tag_id());
        ASSERT_EQ("test_tag", tags[0].get_tag_name());
        version = tags[0].get_version();
    }
    {
        auto result1 = client->getTagSchema(spaceId, "test_tag", version).get();
        ASSERT_TRUE(result1.ok());
        auto result2 = client->getTagSchema(spaceId, "test_tag").get();
        ASSERT_TRUE(result2.ok());
        ASSERT_EQ(result1.value().columns.size(), result2.value().columns.size());
        for (auto i = 0u; i < result1.value().columns.size(); i++) {
            ASSERT_EQ(result1.value().columns[i].name, result2.value().columns[i].name);
            ASSERT_EQ(result1.value().columns[i].type, result2.value().columns[i].type);
        }
    }
    // Get wrong version
    {
        auto result = client->getTagSchema(spaceId, "test_tag", 100).get();
        ASSERT_FALSE(result.ok());
    }
    {
        auto result = client->dropTagSchema(spaceId, "test_tag").get();
        ASSERT_TRUE(result.ok());
    }
    {
        auto result = client->getTagSchema(spaceId, "test_tag", version).get();
        ASSERT_FALSE(result.ok());
    }
}

class TestListener : public MetaChangedListener {
public:
    virtual ~TestListener() = default;
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

    HostAddr getLocalHost() {
        return HostAddr(0, 0);
    }

    int32_t spaceNum = 0;
    int32_t partNum = 0;
    int32_t partChanged = 0;
};

TEST(MetaClientTest, DiffTest) {
    FLAGS_load_data_interval_secs = 1;
    fs::TempDir rootPath("/tmp/MetaClientTest.XXXXXX");

    // Let the system choose an available port for us
    int32_t localMetaPort = 0;
    auto sc = TestUtils::mockMetaServer(localMetaPort, rootPath.path());

    auto threadPool = std::make_shared<folly::IOThreadPoolExecutor>(1);
    IPv4 localIp;
    network::NetworkUtils::ipv4ToInt("127.0.0.1", localIp);
    auto listener = std::make_unique<TestListener>();
    auto client = std::make_shared<MetaClient>(threadPool,
                                               std::vector<HostAddr>{HostAddr(localIp, sc->port_)});
    client->waitForMetadReady();
    client->registerListener(listener.get());
    {
        // Test addHost, listHosts interface.
        std::vector<HostAddr> hosts = {{0, 0}};
        auto r = client->addHosts(hosts).get();
        ASSERT_TRUE(r.ok());
        TestUtils::registerHB(hosts);
        auto ret = client->listHosts().get();
        ASSERT_TRUE(ret.ok());
        for (auto i = 0u; i < hosts.size(); i++) {
            ASSERT_EQ(hosts[i], ret.value()[i].first);
        }
    }
    {
        // Test Create Space and List Spaces
        auto ret = client->createSpace("default_space", 9, 1).get();
        ASSERT_TRUE(ret.ok()) << ret.status();
    }
    sleep(FLAGS_load_data_interval_secs + 1);
    ASSERT_EQ(1, listener->spaceNum);
    ASSERT_EQ(9, listener->partNum);
    {
        auto ret = client->createSpace("default_space_1", 5, 1).get();
        ASSERT_TRUE(ret.ok()) << ret.status();
    }
    sleep(FLAGS_load_data_interval_secs + 1);
    ASSERT_EQ(2, listener->spaceNum);
    ASSERT_EQ(14, listener->partNum);
    {
        // Test Drop Space
        auto ret = client->dropSpace("default_space_1").get();
        ASSERT_TRUE(ret.ok()) << ret.status();
    }
    sleep(FLAGS_load_data_interval_secs + 1);
    ASSERT_EQ(1, listener->spaceNum);
    ASSERT_EQ(9, listener->partNum);
}

TEST(MetaClientTest, HeartbeatTest) {
    ActiveHostsMan::instance()->reset();
    FLAGS_load_data_interval_secs = 5;
    FLAGS_heartbeat_interval_secs = 1;
    const nebula::ClusterID kClusterId = 10;
    fs::TempDir rootPath("/tmp/MetaClientTest.XXXXXX");
    auto sc = TestUtils::mockMetaServer(10001, rootPath.path(), kClusterId);

    auto threadPool = std::make_shared<folly::IOThreadPoolExecutor>(1);
    IPv4 localIp;
    network::NetworkUtils::ipv4ToInt("127.0.0.1", localIp);
    auto listener = std::make_unique<TestListener>();
    auto clientPort = network::NetworkUtils::getAvailablePort();
    HostAddr localHost{localIp, clientPort};
    auto clusterMan
        = std::make_unique<nebula::meta::ClusterManager>("", "", kClusterId);
    auto client = std::make_shared<MetaClient>(threadPool,
                                               std::vector<HostAddr>{HostAddr(localIp, 10001)},
                                               localHost,
                                               clusterMan.get(),
                                               true);  // send heartbeat
    client->addHosts({localHost});
    client->waitForMetadReady();
    client->registerListener(listener.get());
    {
        // Test addHost, listHosts interface.
        std::vector<HostAddr> hosts = {localHost};
        auto r = client->addHosts(hosts).get();
        ASSERT_TRUE(r.ok());
        auto ret = client->listHosts().get();
        ASSERT_TRUE(ret.ok());
        for (auto i = 0u; i < hosts.size(); i++) {
            ASSERT_EQ(hosts[i], ret.value()[i].first);
        }
    }
    sleep(FLAGS_heartbeat_interval_secs + 1);
    ASSERT_EQ(1, ActiveHostsMan::instance()->getActiveHosts().size());
}

}  // namespace meta
}  // namespace nebula


int main(int argc, char** argv) {
    testing::InitGoogleTest(&argc, argv);
    folly::init(&argc, &argv, true);
    google::SetStderrLogging(google::INFO);
    return RUN_ALL_TESTS();
}


