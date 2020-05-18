/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include <gtest/gtest.h>
#include <rocksdb/db.h>

#include "base/Base.h"
#include "clients/meta/MetaClient.h"
#include "common/NebulaKeyUtils.h"
#include "fs/TempDir.h"
#include "meta/ClientBasedGflagsManager.h"
#include "meta/GflagsManager.h"
#include "meta/MetaServiceUtils.h"
#include "meta/ServerBasedSchemaManager.h"
#include "meta/test/TestUtils.h"
#include "meta/test/TestUtils.h"
#include "mock/MockCluster.h"
#include "mock/MockData.h"
#include "network/NetworkUtils.h"

DECLARE_int32(heartbeat_interval_secs);
DECLARE_string(rocksdb_db_options);
// DEFINE_int32(heartbeat_interval_secs, 1, "Heartbeat interval");

DEFINE_int32(storage_client_timeout_ms, 60 * 1000, "storage client timeout");

namespace nebula {
namespace storage {

using nebula::meta::cpp2::PropertyType;
using nebula::meta::cpp2::HBResp;
using nebula::Value;

class TestMetaService : public nebula::meta::cpp2::MetaServiceSvIf {
public:
    folly::Future<HBResp>
    future_heartBeat(const meta::cpp2::HBReq& req) override {
        UNUSED(req);
        folly::Promise<HBResp> pro;
        auto f = pro.getFuture();
        HBResp resp;
        resp.set_code(meta::cpp2::ErrorCode::SUCCEEDED);
        pro.setValue(std::move(resp));
        return f;
    }
};

TEST(ShuffleIpTest, ResolveHostTest) {
    std::string hostname{"cocacola"};
    LOG(INFO) << folly::sformat("hostname = {}", hostname);
    auto saddr = folly::SocketAddress{hostname, 0, true};
    LOG(INFO) << folly::sformat("hostname = {}", saddr.describe());
}

TEST(ShuffleIpTest, setupData) {
    // fs::TempDir rootPath("/tmp/ShuffleIpTest.XXXXXX");
    // fs::TempDir rootPath2("/tmp/ShuffleIpTest2.XXXXXX");
    // mock::MockCluster cluster;

    // auto metaName = network::NetworkUtils::getHostname();
    // // auto metaPort = network::NetworkUtils::getAvailablePort();
    // auto metaPort = 0;
    // // HostAddr metaAddr{metaName, metaPort};

    // std::string storageName{"pepsi"};
    // auto storagePort = network::NetworkUtils::getAvailablePort();
    // HostAddr storageAddr{storageName, storagePort};

    // meta::MetaClientOptions options;
    // options.localHost_ = storageAddr;
    // options.inStoraged_ = true;

    // cluster.startMeta(metaPort, rootPath.path(), metaName);
    // cluster.initMetaClient(options);
    // cluster.startStorage(storageAddr, rootPath2.path());
    // // auto* env = cluster.storageEnv_.get();

    // auto storageClient = cluster.initStorageClient();
    // // CHECK_NE(storageClient, nullptr);

    // GraphSpaceID spaceId = 1;
    // std::vector<cpp2::NewVertex> newVertexes;  //  =
    // auto mockVertexes = mock::MockData::mockVertices();
    // for (auto& mockVertex : mockVertexes) {
    //     // nebula::storage::cpp2::NewVertex newVertex;
    //     newVertexes.emplace_back();
    //     nebula::storage::cpp2::NewTag newTag;

    //     newTag.set_tag_id(mockVertex.tId_);
    //     newTag.set_props(std::move(mockVertex.props_));

    //     newVertexes.back().id = mockVertex.vId_;
    //     newVertexes.back().set_tags({newTag});
    // }
    // std::unordered_map<TagID, std::vector<std::string>> propNames;
    // bool overwritable{true};

    // cpp2::AddVerticesRequest req = mock::MockData::mockAddVerticesReq();
    // auto fut = storageClient->addVertices(spaceId,
    //                                       newVertexes,
    //                                       propNames,
    //                                       overwritable);

    // auto cb = [this] (auto &&resp) {
    //     LOG(INFO) << "storageClient->addVertices() succeed";
    // };

    // auto error = [this] (auto &&e) {
    //     LOG(ERROR) << "storageClient->addVertices() exception " << e.what();
    // };

    // auto threadPool = std::make_shared<folly::IOThreadPoolExecutor>(1);
    // fut.via(threadPool.get()).thenValue(cb).thenError(error);
}

/*
 * modify /etc/hosts
 * */

TEST(ShuffleIpTest, readData) {  // cleanup
    // auto hostname = network::NetworkUtils::getHostname();
    std::string hostname{"cocacola"};
    LOG(INFO) << folly::sformat("hostname = {}", hostname);
    // auto port = network::NetworkUtils::getAvailablePort();
    auto saddr = folly::SocketAddress{hostname, 0, true};
    LOG(INFO) << folly::sformat("hostname = {}", saddr.describe());
}

class TestListener : public meta::MetaChangedListener {
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

    void onPartAdded(const meta::PartHosts& partMeta) override {
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

    void onPartUpdated(const meta::PartHosts& partMeta) override {
        LOG(INFO) << "[" << partMeta.spaceId_ << ", " << partMeta.partId_ << "] updated!";
        partChanged++;
    }

    void fetchLeaderInfo(std::unordered_map<GraphSpaceID,
                                            std::vector<PartitionID>>& leaderIds) override {
        LOG(INFO) << "Get leader distribution!";
        UNUSED(leaderIds);
    }

    HostAddr getLocalHost() {
        return HostAddr("0", 0);
    }

    int32_t spaceNum = 0;
    int32_t partNum = 0;
    int32_t partChanged = 0;
    std::unordered_map<std::string, std::string> options;
};

}  // namespace storage
}  // namespace nebula


int main(int argc, char** argv) {
    testing::InitGoogleTest(&argc, argv);
    folly::init(&argc, &argv, true);
    google::SetStderrLogging(google::INFO);
    return RUN_ALL_TESTS();
}
