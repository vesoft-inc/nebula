/* Copyright (c) 2018 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef META_TEST_TESTUTILS_H_
#define META_TEST_TESTUTILS_H_

#include "base/Base.h"
#include "mock/MockCluster.h"
#include "kvstore/KVStore.h"
#include "kvstore/PartManager.h"
#include "kvstore/NebulaStore.h"
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

using nebula::meta::cpp2::PropertyType;
using mock::MockCluster;

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

    folly::Future<Status> checkPeers() override {
        return response(7);
    }

    folly::Future<Status> getLeaderDist(HostLeaderMap* hostLeaderMap) override {
        (*hostLeaderMap)[HostAddr(0, 0)][1] = {1, 2, 3, 4, 5};
        (*hostLeaderMap)[HostAddr(1, 1)][1] = {6, 7, 8};
        (*hostLeaderMap)[HostAddr(2, 2)][1] = {9};
        return response(8);
    }


    folly::Future<Status> createSnapshot() override {
        return response(8);
    }

    folly::Future<Status> dropSnapshot() override {
        return response(9);
    }

    folly::Future<Status> blockingWrites() override {
        return response(10);
    }

    folly::Future<Status> rebuildTagIndex() override {
        return response(11);
    }

    folly::Future<Status> rebuildEdgeIndex() override {
        return response(12);
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
    static cpp2::ColumnDef columnDef(int32_t index, cpp2::PropertyType type) {
        cpp2::ColumnDef column;
        column.set_name(folly::stringPrintf("col_%d", index));
        column.set_type(type);
        return column;
    }

    static cpp2::ColumnDef columnDefWithDefault(int32_t index,
                                                cpp2::PropertyType type) {
        cpp2::ColumnDef column;
        column.set_name(folly::stringPrintf("col_%d", index));
        column.set_type(type);
        nebula::Value defaultValue;
        switch (type) {
            case cpp2::PropertyType::BOOL:
                defaultValue.setBool(true);
                break;
            case cpp2::PropertyType::INT64:
                defaultValue.setInt(1);
                break;
            case cpp2::PropertyType::DOUBLE:
                defaultValue.setFloat(3.14);
                break;
            case cpp2::PropertyType::STRING:
                defaultValue.setStr("default value");
                break;
            default:
                LOG(ERROR) << "Unsupoort type";
        }
        column.set_default_value(std::move(defaultValue));
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
        std::vector<HostAddr> thriftHosts(hosts);
        registerHB(kv, hosts);
        {
            cpp2::ListHostsReq req;
            auto* processor = ListHostsProcessor::instance(kv);
            auto f = processor->getFuture();
            processor->process(req);
            auto resp = std::move(f).get();
            EXPECT_EQ(hosts.size(), resp.hosts.size());
            for (decltype(hosts.size()) i = 0; i < hosts.size(); i++) {
                EXPECT_EQ(hosts[i], resp.hosts[i].hostAddr);
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

        std::vector<HostAddr> allHosts;
        for (int i = 0; i < totalHost; i++) {
            allHosts.emplace_back(i, i);
        }

        for (auto partId = 1; partId <= partitionNum; partId++) {
            std::vector<HostAddr> hosts;
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
            cpp2::Schema srcsch;
            for (auto i = 0; i < 2; i++) {
                cpp2::ColumnDef column;
                column.name = folly::stringPrintf("tag_%d_col_%d", tagId, i);
                column.type = i < 1 ? PropertyType::INT64 : PropertyType::STRING;
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
            cpp2::Schema srcsch;
            for (auto i = 0; i < 2; i++) {
                cpp2::ColumnDef column;
                column.name = folly::stringPrintf("edge_%d_col_%d", edgeType, i);
                column.type = i < 1 ? PropertyType::INT64 : PropertyType::STRING;
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

    static bool verifySchema(cpp2::Schema &result,
                             cpp2::Schema &expected) {
        if (result.get_columns().size() != expected.get_columns().size()) {
            return false;
        }

        std::vector<cpp2::ColumnDef> resultColumns = result.get_columns();
        std::vector<cpp2::ColumnDef> expectedColumns = expected.get_columns();
        std::sort(resultColumns.begin(), resultColumns.end(),
                  [](cpp2::ColumnDef col0, cpp2::ColumnDef col1) {
                      return col0.get_name() < col1.get_name();
                  });
        std::sort(expectedColumns.begin(), expectedColumns.end(),
                  [](cpp2::ColumnDef col0, cpp2::ColumnDef col1) {
                      return col0.get_name() < col1.get_name();
                  });

        int32_t size = resultColumns.size();
        for (auto i = 0; i < size; i++) {
            if (resultColumns[i].get_name() != expectedColumns[i].get_name() ||
                resultColumns[i].get_type() != expectedColumns[i].get_type()) {
                return false;
            }
        }
        return true;
    }

    static bool verifyMap(std::map<std::string, std::vector<std::string>> &result,
                          std::map<std::string, std::vector<std::string>> &expected) {
        if (result.size() != expected.size()) {
            return false;
        }

        for (auto resultIter = result.begin(), expectedIter = expected.begin();
             resultIter != result.end() && expectedIter != expected.end();
             resultIter++, expectedIter++) {
            if (resultIter->first != expectedIter->first &&
                !verifyResult(resultIter->second, expectedIter->second)) {
                return false;
            }
        }

        return true;
    }

    static bool verifyMap(std::map<std::string, std::vector<cpp2::ColumnDef>> &result,
                          std::map<std::string, std::vector<cpp2::ColumnDef>> &expected) {
        if (result.size() != expected.size()) {
            return false;
        }

        for (auto resultIter = result.begin(), expectedIter = expected.begin();
             resultIter != result.end() && expectedIter != expected.end();
             resultIter++, expectedIter++) {
            if (resultIter->first != expectedIter->first &&
                !verifyResult(resultIter->second, expectedIter->second)) {
                return false;
            }
        }
        return true;
    }

    static bool verifyResult(std::vector<std::string> &result,
                             std::vector<std::string> &expected) {
        if (result.size() != expected.size()) {
            return false;
        }

        std::sort(result.begin(), result.end());
        std::sort(expected.begin(), expected.end());
        int32_t size = result.size();
        for (int32_t i = 0; i < size; i++) {
            if (result[i] != expected[i]) {
                return false;
            }
        }

        return true;
    }

    static bool verifyResult(std::vector<cpp2::ColumnDef> &result,
                             std::vector<cpp2::ColumnDef> &expected) {
        if (result.size() != expected.size()) {
            return false;
        }

        std::sort(result.begin(), result.end(),
                  [](cpp2::ColumnDef& x, cpp2::ColumnDef& y) {
                      return x.get_name().compare(y.get_name());
                  });
        std::sort(expected.begin(), expected.end(),
                  [](cpp2::ColumnDef& x, cpp2::ColumnDef& y) {
                      return x.get_name().compare(y.get_name());
                  });
        int32_t size = result.size();
        for (int32_t i = 0; i < size; i++) {
            if (result[i].get_name() != expected[i].get_name() ||
                result[i].get_type() != expected[i].get_type()) {
                return false;
            }
        }
        return true;
    }
};

}  // namespace meta
}  // namespace nebula

#endif  // META_TEST_TESTUTILS_H_
