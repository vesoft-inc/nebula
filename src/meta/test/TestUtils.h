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
#include "interface/gen-cpp2/common_types.h"
#include "time/TimeUtils.h"
#include "meta/ActiveHostsMan.h"

DECLARE_string(part_man_type);

namespace nebula {
namespace meta {

using nebula::cpp2::SupportedType;

class TestUtils {
public:
    static std::unique_ptr<kvstore::KVStore> initKV(const char* rootPath) {
        auto workers = std::make_shared<thread::GenericThreadPool>();
        workers->start(4);
        auto ioPool = std::make_shared<folly::IOThreadPoolExecutor>(4);
        auto partMan = std::make_unique<kvstore::MemPartManager>();

        // GraphSpaceID =>  {PartitionIDs}
        // 0 => {0}
        auto& partsMap = partMan->partsMap();
        partsMap[0][0] = PartMeta();

        std::vector<std::string> paths;
        paths.push_back(folly::stringPrintf("%s/disk1", rootPath));

        kvstore::KVOptions options;
        options.dataPaths_ = std::move(paths);
        options.partMan_ = std::move(partMan);
        HostAddr localhost = HostAddr(0, 0);

        auto store = std::make_unique<kvstore::NebulaStore>(std::move(options),
                                                            ioPool,
                                                            workers,
                                                            localhost);
        sleep(1);
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

    static void registerHB(const std::vector<HostAddr>& hosts) {
        ActiveHostsManHolder::hostsMan()->reset();
        auto now = time::TimeUtils::nowInSeconds();
        for (auto& h : hosts) {
            ActiveHostsManHolder::hostsMan()->updateHostInfo(h, HostInfo(now));
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
        registerHB(hosts);
        {
            cpp2::ListHostsReq req;
            auto* processor = ListHostsProcessor::instance(kv);
            auto f = processor->getFuture();
            processor->process(req);
            auto resp = std::move(f).get();
            EXPECT_EQ(hosts.size(), resp.hosts.size());
            for (decltype(hosts.size()) i = 0; i < hosts.size(); i++) {
                EXPECT_EQ(hosts[i].first, resp.hosts[i].ip);
                EXPECT_EQ(hosts[i].second, resp.hosts[i].port);
            }
        }
        return hosts.size();
    }

    static bool assembleSpace(kvstore::KVStore* kv, GraphSpaceID id) {
        bool ret = false;
        std::vector<nebula::kvstore::KV> data;
        data.emplace_back(MetaServiceUtils::spaceKey(id), "test_space");
        kv->asyncMultiPut(0, 0, std::move(data),
                          [&] (kvstore::ResultCode code) {
                              ret = (code == kvstore::ResultCode::SUCCEEDED);
                          });
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

        kv->asyncMultiPut(0, 0, std::move(tags),
                          [] (kvstore::ResultCode code) {
                                ASSERT_EQ(kvstore::ResultCode::SUCCEEDED, code);
                          });
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

        kv->asyncMultiPut(0, 0, std::move(edges), [] (kvstore::ResultCode code) {
            ASSERT_EQ(kvstore::ResultCode::SUCCEEDED, code);
        });
    }

    static std::unique_ptr<test::ServerContext> mockMetaServer(uint16_t port,
                                                               const char* dataPath) {
        LOG(INFO) << "Initializing KVStore at \"" << dataPath << "\"";

        auto sc = std::make_unique<test::ServerContext>();
        sc->kvStore_ = TestUtils::initKV(dataPath);

        auto handler = std::make_shared<nebula::meta::MetaServiceHandler>(sc->kvStore_.get());
        sc->mockCommon("meta", port, handler);
        LOG(INFO) << "The Meta Daemon started on port " << sc->port_
                  << ", data path is at \"" << dataPath << "\"";

        return sc;
    }
};

}  // namespace meta
}  // namespace nebula

#endif  // META_TEST_TESTUTILS_H_
