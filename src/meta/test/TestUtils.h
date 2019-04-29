/* Copyright (c) 2018 - present, VE Software Inc. All rights reserved
 *
 * This source code is licensed under Apache 2.0 License
 *  (found in the LICENSE.Apache file in the root directory)
 */

#include "base/Base.h"
#include "kvstore/KVStore.h"
#include "kvstore/PartManager.h"
#include "kvstore/NebulaStore.h"
#include "meta/processors/AddHostsProcessor.h"
#include "meta/processors/ListHostsProcessor.h"
#include "meta/MetaServiceHandler.h"
#include <thrift/lib/cpp2/server/ThriftServer.h>
#include "interface/gen-cpp2/common_types.h"

DECLARE_string(part_man_type);

namespace nebula {
namespace meta {

using nebula::cpp2::SupportedType;

class TestUtils {
public:
    static kvstore::KVStore* initKV(const char* rootPath) {
        auto partMan = std::make_unique<kvstore::MemPartManager>();
        // GraphSpaceID =>  {PartitionIDs}
        // 0 => {0}
        auto& partsMap = partMan->partsMap();
        partsMap[0][0] = PartMeta();

        std::vector<std::string> paths;
        paths.push_back(folly::stringPrintf("%s/disk1", rootPath));

        kvstore::KVOptions options;
        options.local_ = HostAddr(0, 0);
        options.dataPaths_ = std::move(paths);
        options.partMan_ = std::move(partMan);

        kvstore::NebulaStore* kv = static_cast<kvstore::NebulaStore*>(
                                     kvstore::KVStore::instance(std::move(options)));
        return kv;
    }

    static nebula::cpp2::ColumnDef columnDef(int32_t index, nebula::cpp2::SupportedType st) {
        nebula::cpp2::ColumnDef column;
        column.set_name(folly::stringPrintf("col_%d", index));
        nebula::cpp2::ValueType vType;
        vType.set_type(st);
        column.set_type(std::move(vType));
        return column;
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
                          [&] (kvstore::ResultCode code, HostAddr leader) {
            ret = (code == kvstore::ResultCode::SUCCEEDED);
            UNUSED(leader);
        });
        return ret;
    }

    static void mockTag(kvstore::KVStore* kv, int32_t tagNum, int64_t version = 0) {
        std::vector<nebula::kvstore::KV> tags;
        int64_t ver = version;
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
            tags.emplace_back(MetaServiceUtils::indexKey(EntryType::TAG, tagName), tagIdVal);
            tags.emplace_back(MetaServiceUtils::schemaTagKey(1, tagId, ver++),
                              MetaServiceUtils::schemaTagVal(tagName, srcsch));
        }

        kv->asyncMultiPut(0, 0, std::move(tags),
                                [] (kvstore::ResultCode code, HostAddr leader) {
            ASSERT_EQ(kvstore::ResultCode::SUCCEEDED, code);
            UNUSED(leader);
        });
    }

    struct ServerContext {
        ~ServerContext() {
            server_->stop();
            serverT_->join();
            VLOG(3) << "~ServerContext";
        }

        std::unique_ptr<apache::thrift::ThriftServer> server_;
        std::unique_ptr<std::thread> serverT_;
    };

    static std::unique_ptr<ServerContext> mockServer(uint32_t port, const char* dataPath) {
        auto sc = std::make_unique<ServerContext>();
        sc->server_ = std::make_unique<apache::thrift::ThriftServer>();
        sc->serverT_ = std::make_unique<std::thread>([&]() {
            LOG(INFO) << "Starting the meta Daemon on port " << port << ", path " << dataPath;
            std::unique_ptr<kvstore::KVStore> kv(TestUtils::initKV(dataPath));
            auto handler = std::make_shared<nebula::meta::MetaServiceHandler>(kv.get());
            CHECK(!!sc->server_) << "Failed to create the thrift server";
            sc->server_->setInterface(handler);
            sc->server_->setPort(port);
            sc->server_->serve();  // Will wait until the server shuts down

            LOG(INFO) << "Stop the server...";
        });
        sleep(1);
        return sc;
    }
};

}  // namespace meta
}  // namespace nebula

