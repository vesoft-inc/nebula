/* Copyright (c) 2018 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef STORAGE_TEST_TESTUTILS_H_
#define STORAGE_TEST_TESTUTILS_H_

#include "AdHocSchemaManager.h"
#include "test/ServerContext.h"
#include "base/Base.h"
#include "kvstore/KVStore.h"
#include "kvstore/PartManager.h"
#include "kvstore/NebulaStore.h"
#include "meta/SchemaManager.h"
#include "meta/SchemaProviderIf.h"
#include "dataman/ResultSchemaProvider.h"
#include "storage/StorageServiceHandler.h"
#include <thrift/lib/cpp2/server/ThriftServer.h>
#include <folly/synchronization/Baton.h>
#include <folly/executors/ThreadPoolExecutor.h>
#include <folly/executors/CPUThreadPoolExecutor.h>
#include "dataman/RowReader.h"
#include <thrift/lib/cpp/concurrency/ThreadManager.h>


namespace nebula {
namespace storage {

class TestUtils {
public:
    static std::unique_ptr<kvstore::KVStore> initKV(
            const char* rootPath,
            int32_t partitionNumber = 6,
            HostAddr localhost = {0, 0},
            meta::MetaClient* mClient = nullptr,
            bool useMetaServer = false,
            std::unique_ptr<kvstore::CompactionFilterFactoryBuilder> cffBuilder = nullptr) {
        auto ioPool = std::make_shared<folly::IOThreadPoolExecutor>(4);
        auto workers = apache::thrift::concurrency::PriorityThreadManager::newPriorityThreadManager(
                                 1, true /*stats*/);
        workers->setNamePrefix("executor");
        workers->start();


        kvstore::KVOptions options;
        if (useMetaServer) {
            options.partMan_ = std::make_unique<kvstore::MetaServerBasedPartManager>(
                localhost,
                mClient);
        } else {
            auto memPartMan = std::make_unique<kvstore::MemPartManager>();
            // GraphSpaceID =>  {PartitionIDs}
            // 0 => {0, 1, 2, 3, 4, 5}
            auto& partsMap = memPartMan->partsMap();
            for (auto partId = 0; partId < partitionNumber; partId++) {
                partsMap[0][partId] = PartMeta();
            }

            options.partMan_ = std::move(memPartMan);
        }

        std::vector<std::string> paths;
        paths.emplace_back(folly::stringPrintf("%s/disk1", rootPath));
        paths.emplace_back(folly::stringPrintf("%s/disk2", rootPath));

        // Prepare KVStore
        options.dataPaths_ = std::move(paths);
        options.cffBuilder_ = std::move(cffBuilder);
        auto store = std::make_unique<kvstore::NebulaStore>(std::move(options),
                                                            ioPool,
                                                            localhost,
                                                            workers);
        store->init();
        sleep(1);
        return store;
    }

    static std::unique_ptr<meta::SchemaManager> mockSchemaMan(GraphSpaceID spaceId = 0) {
        auto* schemaMan = new AdHocSchemaManager();
        for (auto edgeType = 101; edgeType < 110; edgeType++) {
            schemaMan->addEdgeSchema(spaceId /*space id*/, edgeType /*edge type*/,
                                     TestUtils::genEdgeSchemaProvider(10, 10));
        }
        for (auto tagId = 3001; tagId < 3010; tagId++) {
            schemaMan->addTagSchema(
                spaceId /*space id*/, tagId, TestUtils::genTagSchemaProvider(tagId, 3, 3));
        }
        std::unique_ptr<meta::SchemaManager> sm(schemaMan);
        return sm;
    }

    static std::vector<cpp2::Vertex> setupVertices(
            const PartitionID partitionID,
            const int64_t verticesNum,
            const int32_t tagsNum) {
        // partId => List<Vertex>
        // Vertex => {Id, List<VertexProp>}
        // VertexProp => {tagId, tags}
        std::vector<cpp2::Vertex> vertices;
        VertexID vertexID = 0;
        while (vertexID < verticesNum) {
              TagID tagID = 0;
              std::vector<cpp2::Tag> tags;
              while (tagID < tagsNum) {
                    tags.emplace_back(apache::thrift::FragileConstructor::FRAGILE,
                                      tagID,
                                      folly::stringPrintf("%d_%ld_%d",
                                                          partitionID, vertexID, tagID++));
               }
               vertices.emplace_back(apache::thrift::FragileConstructor::FRAGILE,
                                     vertexID++,
                                     std::move(tags));
        }
        return vertices;
    }


    /**
     * It will generate SchemaProvider with some int fields and string fields
     * */
    static std::shared_ptr<meta::SchemaProviderIf> genEdgeSchemaProvider(
            int32_t intFieldsNum,
            int32_t stringFieldsNum) {
        nebula::cpp2::Schema schema;
        for (auto i = 0; i < intFieldsNum; i++) {
            nebula::cpp2::ColumnDef column;
            column.name = folly::stringPrintf("col_%d", i);
            column.type.type = nebula::cpp2::SupportedType::INT;
            schema.columns.emplace_back(std::move(column));
        }
        for (auto i = intFieldsNum; i < intFieldsNum + stringFieldsNum; i++) {
            nebula::cpp2::ColumnDef column;
            column.name = folly::stringPrintf("col_%d", i);
            column.type.type = nebula::cpp2::SupportedType::STRING;
            schema.columns.emplace_back(std::move(column));
        }
        return std::make_shared<ResultSchemaProvider>(std::move(schema));
    }


    /**
     * It will generate tag SchemaProvider with some int fields and string fields
     * */
    static std::shared_ptr<meta::SchemaProviderIf> genTagSchemaProvider(
            TagID tagId,
            int32_t intFieldsNum,
            int32_t stringFieldsNum) {
        nebula::cpp2::Schema schema;
        for (auto i = 0; i < intFieldsNum; i++) {
            nebula::cpp2::ColumnDef column;
            column.name = folly::stringPrintf("tag_%d_col_%d", tagId, i);
            column.type.type = nebula::cpp2::SupportedType::INT;
            schema.columns.emplace_back(std::move(column));
        }
        for (auto i = intFieldsNum; i < intFieldsNum + stringFieldsNum; i++) {
            nebula::cpp2::ColumnDef column;
            column.name = folly::stringPrintf("tag_%d_col_%d", tagId, i);
            column.type.type = nebula::cpp2::SupportedType::STRING;
            schema.columns.emplace_back(std::move(column));
        }
        return std::make_shared<ResultSchemaProvider>(std::move(schema));
    }

    static cpp2::PropDef vertexPropDef(std::string name, TagID tagId) {
        cpp2::PropDef prop;
        prop.set_name(std::move(name));
        prop.set_owner(cpp2::PropOwner::SOURCE);
        prop.id.set_tag_id(tagId);
        return prop;
    }

    static cpp2::PropDef edgePropDef(std::string name, EdgeType eType) {
        cpp2::PropDef prop;
        prop.set_name(std::move(name));
        prop.set_owner(cpp2::PropOwner::EDGE);
        prop.id.set_edge_type(eType);
        return prop;
    }

    static cpp2::PropDef vertexPropDef(std::string name, cpp2::StatType type, TagID tagId) {
        auto prop = TestUtils::vertexPropDef(std::move(name), tagId);
        prop.set_stat(type);
        return prop;
    }

    static cpp2::PropDef edgePropDef(std::string name, cpp2::StatType type, EdgeType eType) {
        auto prop = TestUtils::edgePropDef(std::move(name), eType);
        prop.set_stat(type);
        return prop;
    }

    // If kvstore should init files in dataPath, input port can't be 0
    static std::unique_ptr<test::ServerContext> mockStorageServer(meta::MetaClient* mClient,
                                                                  const char* dataPath,
                                                                  uint32_t ip,
                                                                  uint32_t port = 0,
                                                                  bool useMetaServer = false) {
        auto sc = std::make_unique<test::ServerContext>();
        // Always use the Meta Service in this case
        sc->kvStore_ = TestUtils::initKV(dataPath, 6, {ip, port}, mClient, true);

        if (!useMetaServer) {
            sc->schemaMan_ = TestUtils::mockSchemaMan(1);
        } else {
            LOG(INFO) << "Create real schemaManager";
            sc->schemaMan_ = meta::SchemaManager::create();
            sc->schemaMan_->init(mClient);
        }

        auto handler = std::make_shared<nebula::storage::StorageServiceHandler>(
            sc->kvStore_.get(), sc->schemaMan_.get(), mClient);
        sc->mockCommon("storage", port, handler);
        auto ptr = dynamic_cast<kvstore::MetaServerBasedPartManager*>(
            sc->kvStore_->partManager());
        if (ptr) {
            ptr->setLocalHost(HostAddr(ip, sc->port_));
        } else {
            VLOG(1) << "Not using a MetaServerBasedPartManager";
        }

        // Sleep one second to wait for the leader election
        sleep(1);

        LOG(INFO) << "The storage daemon started on port " << sc->port_
                  << ", data path is at \"" << dataPath << "\"";
        return sc;
    }

    static void waitUntilAllElected(kvstore::KVStore* kvstore, GraphSpaceID spaceId, int partNum) {
        auto* nKV = static_cast<kvstore::NebulaStore*>(kvstore);
        // wait until all part leader exists
        while (true) {
            int readyNum = 0;
            for (auto partId = 1; partId <= partNum; partId++) {
                auto retLeader = nKV->partLeader(spaceId, partId);
                if (ok(retLeader)) {
                    auto leader = value(std::move(retLeader));
                    if (leader != HostAddr(0, 0)) {
                        readyNum++;
                    }
                }
            }
            if (readyNum == partNum) {
                LOG(INFO) << "All leaders have been elected!";
                break;
            }
            usleep(100000);
        }
    }
};

template <typename T>
void checkTagData(const std::vector<cpp2::TagData>& data,
                  TagID tid,
                  const std::string col_name,
                  const std::unordered_map<TagID, nebula::cpp2::Schema> *schema,
                  T expected) {
    auto it = std::find_if(data.cbegin(), data.cend(), [tid](auto& td) {
        if (td.tag_id == tid) {
            return true;
        }
        return false;
    });
    DCHECK(it != data.cend());
    auto it2 = schema->find(tid);
    DCHECK(it2 != schema->end());
    auto tagProvider = std::make_shared<ResultSchemaProvider>(it2->second);
    auto tagReader   = RowReader::getRowReader(it->data, tagProvider);
    auto r = RowReader::getPropByName(tagReader.get(), col_name);
    CHECK(ok(r));
    auto col = boost::get<T>(value(r));
    EXPECT_EQ(col, expected);
}

}  // namespace storage
}  // namespace nebula

#endif  // STORAGE_TEST_TESTUTILS_H_
