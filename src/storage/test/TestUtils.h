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
#include "meta/SchemaProviderIf.h"
#include "dataman/ResultSchemaProvider.h"
#include "storage/StorageServiceHandler.h"
#include <thrift/lib/cpp2/server/ThriftServer.h>


namespace nebula {
namespace storage {

class TestUtils {
public:
    static std::unique_ptr<kvstore::KVStore> initKV(
            const char* rootPath,
            HostAddr localhost = {0, 0},
            meta::MetaClient* mClient = nullptr,
            bool useMetaServer = false) {
        auto workers = std::make_shared<thread::GenericThreadPool>();
        workers->start(4);
        auto ioPool = std::make_shared<folly::IOThreadPoolExecutor>(4);

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
            for (auto partId = 0; partId < 6; partId++) {
                partsMap[0][partId] = PartMeta();
            }

            options.partMan_ = std::move(memPartMan);
        }

        std::vector<std::string> paths;
        paths.push_back(folly::stringPrintf("%s/disk1", rootPath));
        paths.push_back(folly::stringPrintf("%s/disk2", rootPath));

        // Prepare KVStore
        options.dataPaths_ = std::move(paths);
        auto store = std::make_unique<kvstore::NebulaStore>(std::move(options),
                                                            ioPool,
                                                            workers,
                                                            localhost);
        sleep(1);
        return store;
    }

    static std::unique_ptr<meta::SchemaManager> mockSchemaMan(GraphSpaceID spaceId = 0) {
        auto* schemaMan = new AdHocSchemaManager();
        schemaMan->addEdgeSchema(
            spaceId /*space id*/, 101 /*edge type*/, TestUtils::genEdgeSchemaProvider(10, 10));
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
        return std::shared_ptr<meta::SchemaProviderIf>(
            new ResultSchemaProvider(std::move(schema)));
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
        return std::shared_ptr<meta::SchemaProviderIf>(
            new ResultSchemaProvider(std::move(schema)));
    }


    static cpp2::PropDef propDef(cpp2::PropOwner owner,
                                 std::string name,
                                 TagID tagId = -1) {
        cpp2::PropDef prop;
        prop.set_name(std::move(name));
        prop.set_owner(owner);
        if (tagId != -1) {
            prop.set_tag_id(tagId);
        }
        return prop;
    }


    static cpp2::PropDef propDef(cpp2::PropOwner owner,
                                 std::string name,
                                 cpp2::StatType type,
                                 TagID tagId = -1) {
        auto prop = TestUtils::propDef(owner, name, tagId);
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
        sc->kvStore_ = TestUtils::initKV(dataPath, {ip, port}, mClient, true);

        std::unique_ptr<meta::SchemaManager> schemaMan;
        if (!useMetaServer) {
            schemaMan = TestUtils::mockSchemaMan(1);
        } else {
            LOG(INFO) << "Create real schemaManager";
            schemaMan = meta::SchemaManager::create();
            schemaMan->init(mClient);
        }

        auto handler = std::make_shared<nebula::storage::StorageServiceHandler>(
            sc->kvStore_.get(), std::move(schemaMan));
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
};

}  // namespace storage
}  // namespace nebula

#endif  // STORAGE_TEST_TESTUTILS_H_
