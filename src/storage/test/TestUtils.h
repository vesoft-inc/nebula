/* Copyright (c) 2018 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "base/Base.h"
#include "kvstore/KVStore.h"
#include "kvstore/PartManager.h"
#include "kvstore/NebulaStore.h"
#include "meta/SchemaProviderIf.h"
#include "dataman/ResultSchemaProvider.h"
#include "storage/StorageServiceHandler.h"
#include <thrift/lib/cpp2/server/ThriftServer.h>
#include "AdHocSchemaManager.h"


DECLARE_string(part_man_type);

namespace nebula {
namespace storage {

class TestUtils {
public:
    static kvstore::KVStore* initKV(const char* rootPath) {
        auto partMan = std::make_unique<kvstore::MemPartManager>();
        // GraphSpaceID =>  {PartitionIDs}
        // 0 => {0, 1, 2, 3, 4, 5}
        auto& partsMap = partMan->partsMap();
        for (auto partId = 0; partId < 6; partId++) {
            partsMap[0][partId] = PartMeta();
        }
        std::vector<std::string> paths;
        paths.push_back(folly::stringPrintf("%s/disk1", rootPath));
        paths.push_back(folly::stringPrintf("%s/disk2", rootPath));

        kvstore::KVOptions options;
        options.local_ = HostAddr(0, 0);
        options.dataPaths_ = std::move(paths);
        options.partMan_ = std::move(partMan);

        kvstore::NebulaStore* kv = static_cast<kvstore::NebulaStore*>(
                                        kvstore::KVStore::instance(std::move(options)));
        return kv;
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

    struct ServerContext {
         ~ServerContext() {
             server_->stop();
             serverT_->join();
             VLOG(3) << "~ServerContext";
         }

         std::unique_ptr<apache::thrift::ThriftServer> server_;
         std::unique_ptr<std::thread> serverT_;
         uint32_t port_;
     };

     static std::unique_ptr<ServerContext> mockServer(const char* dataPath,
                                                      uint32_t ip,
                                                      uint32_t port = 0) {
         auto sc = std::make_unique<ServerContext>();
         sc->server_ = std::make_unique<apache::thrift::ThriftServer>();
         sc->serverT_ = std::make_unique<std::thread>([&]() {
            std::vector<std::string> paths;
            paths.push_back(folly::stringPrintf("%s/disk1", dataPath));
            paths.push_back(folly::stringPrintf("%s/disk2", dataPath));
            kvstore::KVOptions options;
            options.local_ = HostAddr(ip, port);
            options.dataPaths_ = std::move(paths);
            options.partMan_
                = std::make_unique<kvstore::MetaServerBasedPartManager>(options.local_);
            kvstore::NebulaStore* kvPtr = static_cast<kvstore::NebulaStore*>(
                                        kvstore::KVStore::instance(std::move(options)));
             std::unique_ptr<kvstore::KVStore> kv(kvPtr);
             auto schemaMan = TestUtils::mockSchemaMan(1);
             auto handler
                    = std::make_shared<nebula::storage::StorageServiceHandler>(
                                                                            kv.get(),
                                                                            std::move(schemaMan));
             CHECK(!!sc->server_) << "Failed to create the thrift server";
             sc->server_->setInterface(handler);
             sc->server_->setPort(port);
             sc->server_->serve();  // Will wait until the server shuts down
             LOG(INFO) << "Stop the server...";
         });
         while (!sc->server_->getServeEventBase()
                 || !sc->server_->getServeEventBase()->isRunning()) {
         }
         sc->port_ = sc->server_->getAddress().getPort();
         LOG(INFO) << "Starting the storage Daemon on port " << sc->port_
                   << ", path " << dataPath;
         return sc;
     }
};

}  // namespace storage
}  // namespace nebula

