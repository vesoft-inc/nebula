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
#include "meta/ClientBasedGflagsManager.h"

DECLARE_int32(load_data_interval_secs);
DECLARE_int32(heartbeat_interval_secs);
DECLARE_string(rocksdb_db_options);


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
        TestUtils::registerHB(sc->kvStore_.get(), hosts);
        auto ret = client->listHosts().get();
        ASSERT_TRUE(ret.ok());
        for (auto i = 0u; i < hosts.size(); i++) {
            auto tHost = ret.value()[i].hostAddr;
            auto hostAddr = HostAddr(tHost.ip, tHost.port);
            ASSERT_EQ(hosts[i], hostAddr);
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
    client->waitForMetadReady();
    TestUtils::registerHB(sc->kvStore_.get(), hosts);
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
        schema.set_columns(std::move(columns));
        auto result = client->createTagSchema(spaceId, "test_tag", std::move(schema)).get();
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
        ASSERT_EQ(3, result2.value().columns.size());
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

    void onSpaceOptionUpdated(GraphSpaceID spaceId,
                              const std::unordered_map<std::string, std::string>& update)
                              override {
        UNUSED(spaceId);
        for (const auto& kv : update) {
            options[kv.first] = kv.second;
        }
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
    std::unordered_map<std::string, std::string> options;
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
        TestUtils::registerHB(sc->kvStore_.get(), hosts);
        auto ret = client->listHosts().get();
        ASSERT_TRUE(ret.ok());
        for (auto i = 0u; i < hosts.size(); i++) {
            auto tHost = ret.value()[i].hostAddr;
            auto hostAddr = HostAddr(tHost.ip, tHost.port);
            ASSERT_EQ(hosts[i], hostAddr);
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

    auto client = std::make_shared<MetaClient>(threadPool,
                                               std::vector<HostAddr>{HostAddr(localIp, 10001)},
                                               localHost,
                                               kClusterId,
                                               true);  // send heartbeat
    client->waitForMetadReady();
    client->registerListener(listener.get());
    {
        // Test addHost, listHosts interface.
        std::vector<HostAddr> hosts = {localHost};
        auto ret = client->listHosts().get();
        ASSERT_TRUE(ret.ok());
        for (auto i = 0u; i < hosts.size(); i++) {
            auto tHost = ret.value()[i].hostAddr;
            auto hostAddr = HostAddr(tHost.ip, tHost.port);
            ASSERT_EQ(hosts[i], hostAddr);
        }
    }
    sleep(FLAGS_heartbeat_interval_secs + 1);
    ASSERT_EQ(1, ActiveHostsMan::getActiveHosts(sc->kvStore_.get()).size());
}

class TestMetaService : public cpp2::MetaServiceSvIf {
public:
    folly::Future<cpp2::ExecResp>
    future_addHosts(const cpp2::AddHostsReq& req) override {
        UNUSED(req);
        folly::Promise<cpp2::ExecResp> pro;
        auto f = pro.getFuture();
        cpp2::ExecResp resp;
        resp.set_code(cpp2::ErrorCode::SUCCEEDED);
        pro.setValue(std::move(resp));
        return f;
    }
};

class TestMetaServiceRetry : public cpp2::MetaServiceSvIf {
public:
    void setLeader(HostAddr leader) {
        leader_.set_ip(leader.first);
        leader_.set_port(leader.second);
    }

    void setAddr(HostAddr addr) {
        addr_.set_ip(addr.first);
        addr_.set_port(addr.second);
    }

    folly::Future<cpp2::ExecResp>
    future_addHosts(const cpp2::AddHostsReq& req) override {
        UNUSED(req);
        folly::Promise<cpp2::ExecResp> pro;
        auto f = pro.getFuture();
        cpp2::ExecResp resp;
        if (addr_ == leader_) {
            resp.set_code(cpp2::ErrorCode::SUCCEEDED);
        } else {
            resp.set_code(cpp2::ErrorCode::E_LEADER_CHANGED);
            resp.set_leader(leader_);
        }
        pro.setValue(std::move(resp));
        return f;
    }

private:
    nebula::cpp2::HostAddr leader_;
    nebula::cpp2::HostAddr addr_;
};

TEST(MetaClientTest, SimpleTest) {
    IPv4 localIp;
    network::NetworkUtils::ipv4ToInt("127.0.0.1", localIp);

    auto sc = std::make_unique<test::ServerContext>();
    auto handler = std::make_shared<TestMetaService>();
    sc->mockCommon("meta", 0, handler);

    auto threadPool = std::make_shared<folly::IOThreadPoolExecutor>(1);
    auto clientPort = network::NetworkUtils::getAvailablePort();
    HostAddr localHost{localIp, clientPort};
    auto client = std::make_shared<MetaClient>(threadPool,
                                               std::vector<HostAddr>{HostAddr(localIp, sc->port_)},
                                               localHost);
    {
        LOG(INFO) << "Test add hosts...";
        folly::Baton<true, std::atomic> baton;
        client->addHosts({{0, 0}}).then([&baton] (auto&& status) {
            ASSERT_TRUE(status.ok());
            baton.post();
        });
        baton.wait();
    }
}

TEST(MetaClientTest, RetryWithExceptionTest) {
    IPv4 localIp;
    network::NetworkUtils::ipv4ToInt("127.0.0.1", localIp);

    auto threadPool = std::make_shared<folly::IOThreadPoolExecutor>(1);
    auto clientPort = network::NetworkUtils::getAvailablePort();
    HostAddr localHost{localIp, clientPort};
    auto client = std::make_shared<MetaClient>(threadPool,
                                               std::vector<HostAddr>{HostAddr(0, 0)},
                                               localHost);
    // Retry with exception, then failed
    {
        LOG(INFO) << "Test add hosts...";
        folly::Baton<true, std::atomic> baton;
        client->addHosts({{0, 0}}).then([&baton] (auto&& status) {
            ASSERT_TRUE(!status.ok());
            baton.post();
        });
        baton.wait();
    }
}


TEST(MetaClientTest, RetryOnceTest) {
    IPv4 localIp;
    network::NetworkUtils::ipv4ToInt("127.0.0.1", localIp);

    std::vector<std::unique_ptr<test::ServerContext>> contexts;
    std::vector<std::shared_ptr<TestMetaServiceRetry>> handlers;
    std::vector<HostAddr> addrs;
    for (size_t i = 0; i < 3; i++) {
        auto sc = std::make_unique<test::ServerContext>();
        auto handler = std::make_shared<TestMetaServiceRetry>();
        sc->mockCommon("meta", 0, handler);
        addrs.emplace_back(localIp, sc->port_);
        contexts.emplace_back(std::move(sc));
        handlers.emplace_back(handler);
    }
    // set leaders to first service
    for (size_t i = 0; i < addrs.size(); i++) {
        handlers[i]->setLeader(addrs[0]);
        handlers[i]->setAddr(addrs[i]);
    }

    auto threadPool = std::make_shared<folly::IOThreadPoolExecutor>(1);
    auto clientPort = network::NetworkUtils::getAvailablePort();
    HostAddr localHost{localIp, clientPort};
    auto client = std::make_shared<MetaClient>(threadPool,
                                               std::vector<HostAddr>{addrs[1]},
                                               localHost);
    // First get leader changed and then succeeded
    {
        LOG(INFO) << "Test add hosts...";
        folly::Baton<true, std::atomic> baton;
        client->addHosts({{0, 0}}).then([&baton] (auto&& status) {
            ASSERT_TRUE(status.ok());
            baton.post();
        });
        baton.wait();
    }
}

TEST(MetaClientTest, RetryUntilLimitTest) {
    IPv4 localIp;
    network::NetworkUtils::ipv4ToInt("127.0.0.1", localIp);

    std::vector<std::unique_ptr<test::ServerContext>> contexts;
    std::vector<std::shared_ptr<TestMetaServiceRetry>> handlers;
    std::vector<HostAddr> addrs;
    for (size_t i = 0; i < 3; i++) {
        auto sc = std::make_unique<test::ServerContext>();
        auto handler = std::make_shared<TestMetaServiceRetry>();
        sc->mockCommon("meta", 0, handler);
        addrs.emplace_back(localIp, sc->port_);
        contexts.emplace_back(std::move(sc));
        handlers.emplace_back(handler);
    }
    for (size_t i = 0; i < addrs.size(); i++) {
        handlers[i]->setLeader(addrs[(i + 1) % addrs.size()]);
        handlers[i]->setAddr(addrs[i]);
    }

    auto threadPool = std::make_shared<folly::IOThreadPoolExecutor>(1);
    auto clientPort = network::NetworkUtils::getAvailablePort();
    HostAddr localHost{localIp, clientPort};
    auto client = std::make_shared<MetaClient>(threadPool,
                                               std::vector<HostAddr>{addrs[1]},
                                               localHost);
    // always get response of leader changed, then failed
    {
        LOG(INFO) << "Test add hosts...";
        folly::Baton<true, std::atomic> baton;
        client->addHosts({{0, 0}}).then([&baton] (auto&& status) {
            ASSERT_TRUE(!status.ok());
            baton.post();
        });
        baton.wait();
    }
}

TEST(MetaClientTest, RocksdbOptionsTest) {
    FLAGS_load_data_interval_secs = 1;
    fs::TempDir rootPath("/tmp/RocksdbOptionsTest.XXXXXX");
    uint32_t localMetaPort = 0;
    auto sc = TestUtils::mockMetaServer(localMetaPort, rootPath.path());
    auto threadPool = std::make_shared<folly::IOThreadPoolExecutor>(1);
    IPv4 localIp;
    network::NetworkUtils::ipv4ToInt("127.0.0.1", localIp);

    auto listener = std::make_unique<TestListener>();
    auto module = cpp2::ConfigModule::STORAGE;
    auto type = cpp2::ConfigType::NESTED;
    auto mode = meta::cpp2::ConfigMode::MUTABLE;

    auto client = std::make_shared<MetaClient>(threadPool,
        std::vector<HostAddr>{HostAddr(localIp, sc->port_)});
    client->waitForMetadReady();
    client->registerListener(listener.get());
    client->gflagsModule_ = module;

    ClientBasedGflagsManager cfgMan(client.get());
    // mock some rocksdb gflags to meta
    {
        std::vector<cpp2::ConfigItem> configItems;
        FLAGS_rocksdb_db_options = R"({
            "disable_auto_compactions":"false",
            "write_buffer_size":"1048576"
        })";
        configItems.emplace_back(toThriftConfigItem(
            module, "rocksdb_db_options", type,
            mode, toThriftValueStr(type, FLAGS_rocksdb_db_options)));
        cfgMan.registerGflags(configItems);
    }
    {
        std::vector<HostAddr> hosts = {{0, 0}};
        TestUtils::registerHB(sc->kvStore_.get(), hosts);
        client->createSpace("default_space", 9, 1).get();
        sleep(FLAGS_load_data_interval_secs + 1);
    }
    {
        std::string name = "rocksdb_db_options";
        std::string updateValue = "write_buffer_size=2097152,"
                                  "disable_auto_compactions=true,"
                                  "level0_file_num_compaction_trigger=4";
        // update config
        auto setRet = cfgMan.setConfig(module, name, type, updateValue).get();
        ASSERT_TRUE(setRet.ok());

        // get from meta server
        auto getRet = cfgMan.getConfig(module, name).get();
        ASSERT_TRUE(getRet.ok());
        auto item = getRet.value().front();
        auto value = boost::get<std::string>(item.get_value());

        sleep(FLAGS_load_data_interval_secs + 1);
        ASSERT_EQ(FLAGS_rocksdb_db_options, value);
        ASSERT_EQ(listener->options["write_buffer_size"], "2097152");
        ASSERT_EQ(listener->options["disable_auto_compactions"], "true");
        ASSERT_EQ(listener->options["level0_file_num_compaction_trigger"], "4");
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


