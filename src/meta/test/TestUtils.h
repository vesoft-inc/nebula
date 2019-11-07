/* Copyright (c) 2018 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef META_TEST_TESTUTILS_H_
#define META_TEST_TESTUTILS_H_

#include "base/Base.h"
#include "test/ServerContext.h"
#include "kvstore/KVStore.h"
#include "kvstore/PartManager.h"
#include "kvstore/NebulaStore.h"
#include "meta/processors/partsMan/AddHostsProcessor.h"
#include "meta/processors/partsMan/ListHostsProcessor.h"
#include "meta/MetaServiceHandler.h"
#include <thrift/lib/cpp2/server/ThriftServer.h>
#include <folly/synchronization/Baton.h>
#include <folly/executors/CPUThreadPoolExecutor.h>
#include "meta/processors/usersMan/AuthenticationProcessor.h"
#include "interface/gen-cpp2/common_types.h"
#include "time/WallClock.h"
#include "meta/ActiveHostsMan.h"
#include <thrift/lib/cpp/concurrency/ThreadManager.h>

DECLARE_string(part_man_type);

namespace nebula {
namespace meta {

using nebula::cpp2::SupportedType;
using apache::thrift::FragileConstructor::FRAGILE;

class TestFaultInjector : public FaultInjector {
public:
    explicit TestFaultInjector(std::vector<Status> sts)
        : statusArray_(std::move(sts)) {
        executor_.reset(new folly::CPUThreadPoolExecutor(1));
    }

    ~TestFaultInjector() {
    }

    folly::Future<Status> response(int index) {
        folly::Promise<Status> pro;
        auto f = pro.getFuture();
        LOG(INFO) << "Response " << index;
        executor_->add([this, p = std::move(pro), index]() mutable {
            LOG(INFO) << "Call callback";
            p.setValue(this->statusArray_[index]);
        });
        return f;
    }

    folly::Future<Status> transLeader() override {
        return response(0);
    }

    folly::Future<Status> addPart() override {
        return response(1);
    }

    folly::Future<Status> addLearner() override {
        return response(2);
    }

    folly::Future<Status> waitingForCatchUpData() override {
        return response(3);
    }

    folly::Future<Status> memberChange() override {
        return response(4);
    }

    folly::Future<Status> updateMeta() override {
        return response(5);
    }

    folly::Future<Status> removePart() override {
        return response(6);
    }

    folly::Future<Status> getLeaderDist(HostLeaderMap* hostLeaderMap) override {
        (*hostLeaderMap)[HostAddr(0, 0)][1] = {1, 2, 3, 4, 5};
        (*hostLeaderMap)[HostAddr(1, 1)][1] = {6, 7, 8};
        (*hostLeaderMap)[HostAddr(2, 2)][1] = {9};
        return response(7);
    }

    void reset(std::vector<Status> sts) {
        statusArray_ = std::move(sts);
    }

private:
    std::vector<Status> statusArray_;
    std::unique_ptr<folly::Executor> executor_;
};


class TestUtils {
public:
    static std::unique_ptr<kvstore::KVStore> initKV(const char* rootPath, uint16_t port = 0) {
        auto ioPool = std::make_shared<folly::IOThreadPoolExecutor>(4);
        auto partMan = std::make_unique<kvstore::MemPartManager>();
        auto workers = apache::thrift::concurrency::PriorityThreadManager::newPriorityThreadManager(
                                 1, true /*stats*/);
        workers->setNamePrefix("executor");
        workers->start();

        // GraphSpaceID =>  {PartitionIDs}
        // 0 => {0}
        auto& partsMap = partMan->partsMap();
        partsMap[0][0] = PartMeta();

        std::vector<std::string> paths;
        paths.emplace_back(folly::stringPrintf("%s/disk1", rootPath));

        kvstore::KVOptions options;
        options.dataPaths_ = std::move(paths);
        options.partMan_ = std::move(partMan);
        IPv4 localIp;
        network::NetworkUtils::ipv4ToInt("127.0.0.1", localIp);
        if (port == 0) {
            port = network::NetworkUtils::getAvailablePort();
        }
        HostAddr localhost = HostAddr(localIp, port);

        auto store = std::make_unique<kvstore::NebulaStore>(std::move(options),
                                                            ioPool,
                                                            localhost,
                                                            workers);
        store->init();
        while (true) {
            auto retLeader = store->partLeader(0, 0);
            if (ok(retLeader)) {
                auto leader = value(std::move(retLeader));
                LOG(INFO) << leader;
                if (leader == localhost) {
                    break;
                }
            }
            usleep(100000);
        }
        return std::move(store);
    }

    static nebula::cpp2::ColumnDef columnDef(int32_t index, nebula::cpp2::SupportedType st) {
        nebula::cpp2::ColumnDef column;
        column.set_name(folly::stringPrintf("col_%d", index));
        nebula::cpp2::ValueType vType;
        vType.set_type(st);
        column.set_type(std::move(vType));
        return column;
    }

    static void registerHB(kvstore::KVStore* kv, const std::vector<HostAddr>& hosts) {
        auto now = time::WallClock::fastNowInMilliSec();
        for (auto& h : hosts) {
            auto ret = ActiveHostsMan::updateHostInfo(kv, h, HostInfo(now));
            CHECK_EQ(ret, kvstore::ResultCode::SUCCEEDED);
        }
     }

    static int32_t createSomeHosts(kvstore::KVStore* kv,
                                   std::vector<HostAddr> hosts
                                       = {{0, 0}, {1, 1}, {2, 2}, {3, 3}}) {
        std::vector<nebula::cpp2::HostAddr> thriftHosts;
        thriftHosts.resize(hosts.size());
        std::transform(hosts.begin(), hosts.end(), thriftHosts.begin(), [](const auto& h) {
            nebula::cpp2::HostAddr th;
            th.set_ip(h.first);
            th.set_port(h.second);
            return th;
        });
        {
            cpp2::AddHostsReq req;
            req.set_hosts(std::move(thriftHosts));
            auto* processor = AddHostsProcessor::instance(kv);
            auto f = processor->getFuture();
            processor->process(req);
            auto resp = std::move(f).get();
            EXPECT_EQ(cpp2::ErrorCode::SUCCEEDED, resp.code);
        }
        registerHB(kv, hosts);
        {
            cpp2::ListHostsReq req;
            auto* processor = ListHostsProcessor::instance(kv);
            auto f = processor->getFuture();
            processor->process(req);
            auto resp = std::move(f).get();
            EXPECT_EQ(hosts.size(), resp.hosts.size());
            for (decltype(hosts.size()) i = 0; i < hosts.size(); i++) {
                EXPECT_EQ(hosts[i].first, resp.hosts[i].hostAddr.ip);
                EXPECT_EQ(hosts[i].second, resp.hosts[i].hostAddr.port);
            }
        }
        return hosts.size();
    }

    static bool assembleSpace(kvstore::KVStore* kv, GraphSpaceID id, int32_t partitionNum,
                              int32_t replica = 1, int32_t totalHost = 1) {
        // mock the part distribution like create space
        bool ret = false;
        cpp2::SpaceProperties properties;
        properties.set_space_name("test_space");
        properties.set_partition_num(partitionNum);
        properties.set_replica_factor(replica);
        auto spaceVal = MetaServiceUtils::spaceVal(properties);
        std::vector<nebula::kvstore::KV> data;
        data.emplace_back(MetaServiceUtils::spaceKey(id), MetaServiceUtils::spaceVal(properties));

        std::vector<nebula::cpp2::HostAddr> allHosts;
        for (int i = 0; i < totalHost; i++) {
            allHosts.emplace_back(apache::thrift::FragileConstructor::FRAGILE, i, i);
        }

        for (auto partId = 1; partId <= partitionNum; partId++) {
            std::vector<nebula::cpp2::HostAddr> hosts;
            size_t idx = partId;
            for (int32_t i = 0; i < replica; i++, idx++) {
                hosts.emplace_back(allHosts[idx % totalHost]);
            }
            data.emplace_back(MetaServiceUtils::partKey(id, partId),
                              MetaServiceUtils::partVal(hosts));
        }
        folly::Baton<true, std::atomic> baton;
        kv->asyncMultiPut(0, 0, std::move(data),
                          [&] (kvstore::ResultCode code) {
                              ret = (code == kvstore::ResultCode::SUCCEEDED);
                              baton.post();
                          });
        baton.wait();
        return ret;
    }

    static void mockTag(kvstore::KVStore* kv, int32_t tagNum, SchemaVer version = 0) {
        std::vector<nebula::kvstore::KV> tags;
        SchemaVer ver = version;
        for (auto t = 0; t < tagNum; t++) {
            TagID tagId = t;
            nebula::cpp2::Schema srcsch;
            for (auto i = 0; i < 2; i++) {
                nebula::cpp2::ColumnDef column;
                column.name = folly::stringPrintf("tag_%d_col_%d", tagId, i);
                column.type.type = i < 1 ? SupportedType::INT : SupportedType::STRING;
                srcsch.columns.emplace_back(std::move(column));
            }
            auto tagName = folly::stringPrintf("tag_%d", tagId);
            auto tagIdVal = std::string(reinterpret_cast<const char*>(&tagId), sizeof(tagId));
            tags.emplace_back(MetaServiceUtils::indexTagKey(1, tagName), tagIdVal);
            tags.emplace_back(MetaServiceUtils::schemaTagKey(1, tagId, ver++),
                              MetaServiceUtils::schemaTagVal(tagName, srcsch));
        }
        folly::Baton<true, std::atomic> baton;
        kv->asyncMultiPut(0, 0, std::move(tags),
                          [&] (kvstore::ResultCode code) {
                                ASSERT_EQ(kvstore::ResultCode::SUCCEEDED, code);
                                baton.post();
                          });
        baton.wait();
    }

    static void mockEdge(kvstore::KVStore* kv, int32_t edgeNum, SchemaVer version = 0) {
        std::vector<nebula::kvstore::KV> edges;
        SchemaVer ver = version;
        for (auto t = 0; t < edgeNum; t++) {
            EdgeType edgeType = t;
            nebula::cpp2::Schema srcsch;
            for (auto i = 0; i < 2; i++) {
                nebula::cpp2::ColumnDef column;
                column.name = folly::stringPrintf("edge_%d_col_%d", edgeType, i);
                column.type.type = i < 1 ? SupportedType::INT : SupportedType::STRING;
                srcsch.columns.emplace_back(std::move(column));
            }
            auto edgeName = folly::stringPrintf("edge_%d", edgeType);
            auto edgeTypeVal = std::string(reinterpret_cast<const char*>(&edgeType),
                                           sizeof(edgeType));
            edges.emplace_back(MetaServiceUtils::indexEdgeKey(1, edgeName), edgeTypeVal);
            edges.emplace_back(MetaServiceUtils::schemaEdgeKey(1, edgeType, ver++),
                               MetaServiceUtils::schemaEdgeVal(edgeName, srcsch));
        }

        folly::Baton<true, std::atomic> baton;
        kv->asyncMultiPut(0, 0, std::move(edges), [&] (kvstore::ResultCode code) {
            ASSERT_EQ(kvstore::ResultCode::SUCCEEDED, code);
            baton.post();
        });
        baton.wait();
    }

    static std::unique_ptr<test::ServerContext> mockMetaServer(uint16_t port,
                                                               const char* dataPath,
                                                               ClusterID clusterId = 0) {
        LOG(INFO) << "Initializing KVStore at \"" << dataPath << "\"";

        auto sc = std::make_unique<test::ServerContext>();
        sc->kvStore_ = TestUtils::initKV(dataPath, port);

        auto handler = std::make_shared<nebula::meta::MetaServiceHandler>(sc->kvStore_.get(),
                                                                          clusterId);
        sc->mockCommon("meta", port, handler);
        LOG(INFO) << "The Meta Daemon started on port " << sc->port_
                  << ", data path is at \"" << dataPath << "\"";

        return sc;
    }

    static StatusOr<UserID> createUser(kvstore::KVStore* kv,
                                       bool missingOk,
                                       folly::StringPiece account,
                                       folly::StringPiece password,
                                       bool               isLock,
                                       int32_t            maxQueries,
                                       int32_t            maxUpdates,
                                       int32_t            maxConnections,
                                       int32_t            maxConnectors) {
        cpp2::CreateUserReq req;
        req.set_missing_ok(missingOk);
        req.set_encoded_pwd(password.str());
        decltype(req.user) user;
        user.set_account(account.str());
        user.set_is_lock(isLock);
        user.set_max_queries_per_hour(maxQueries);
        user.set_max_updates_per_hour(maxUpdates);
        user.set_max_connections_per_hour(maxConnections);
        user.set_max_user_connections(maxConnectors);
        req.set_user(std::move(user));
        auto* processor = CreateUserProcessor::instance(kv);
        auto f = processor->getFuture();
        processor->process(req);
        auto resp = std::move(f).get();
        if (resp.get_code() == cpp2::ErrorCode::SUCCEEDED) {
            return resp.get_id().get_user_id();
        } else {
            return Status::Error("Create user fail");
        }
    }
};

}  // namespace meta
}  // namespace nebula

#endif  // META_TEST_TESTUTILS_H_
