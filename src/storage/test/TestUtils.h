/* Copyright (c) 2018 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef STORAGE_TEST_TESTUTILS_H_
#define STORAGE_TEST_TESTUTILS_H_

#include "AdHocSchemaManager.h"
#include "AdHocIndexManager.h"
#include "test/ServerContext.h"
#include "base/Base.h"
#include "kvstore/KVStore.h"
#include "kvstore/PartManager.h"
#include "kvstore/NebulaStore.h"
#include "meta/SchemaProviderIf.h"
#include "dataman/ResultSchemaProvider.h"
#include "meta/NebulaSchemaProvider.h"
#include "storage/StorageServiceHandler.h"
#include <thrift/lib/cpp2/server/ThriftServer.h>
#include <folly/synchronization/Baton.h>
#include <folly/executors/ThreadPoolExecutor.h>
#include <folly/executors/CPUThreadPoolExecutor.h>
#include "dataman/RowReader.h"
#include "dataman/RowWriter.h"
#include <thrift/lib/cpp/concurrency/ThreadManager.h>


namespace nebula {
namespace storage {

class TestUtils {
public:
    static std::unique_ptr<kvstore::KVStore>
    initKV(const char* rootPath,
           int32_t partitionNumber = 6,
           HostAddr localhost = {0, network::NetworkUtils::getAvailablePort()},
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
            for (PartitionID partId = 0; partId < partitionNumber; partId++) {
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

        // When useMetaServer is true, we deal with it using waitUntilAllElected.
        if (useMetaServer) {
            sleep(1);
        } else {
            // Wait until all part leader exist, here spaceId can only be 0.
            GraphSpaceID spaceId = 0;
            while (true) {
                int readyNum = 0;
                for (auto partId = 0; partId < partitionNumber; partId++) {
                    auto retLeader = store->partLeader(spaceId, partId);
                    if (ok(retLeader)) {
                        auto leader = value(std::move(retLeader));
                        if (leader == localhost) {
                            readyNum++;
                        }
                    }
                }
                if (readyNum == partitionNumber) {
                    LOG(INFO) << "All leaders have been elected!";
                    break;
                }
                usleep(100000);
            }
        }
        return store;
    }

    static std::unique_ptr<AdHocSchemaManager> mockSchemaMan(GraphSpaceID spaceId = 0) {
        auto schemaMan = std::make_unique<AdHocSchemaManager>();
        for (TagID tagId = 3001; tagId < 3010; tagId++) {
            schemaMan->addTagSchema(spaceId, tagId, TestUtils::genTagSchemaProvider(tagId, 3, 3));
        }
        for (EdgeType edgeType = 101; edgeType < 110; edgeType++) {
            schemaMan->addEdgeSchema(spaceId, edgeType, TestUtils::genEdgeSchemaProvider(10, 10));
        }
        return schemaMan;
    }

    static std::unique_ptr<AdHocSchemaManager> mockSchemaWithTTLMan(GraphSpaceID spaceId = 0) {
        auto schemaMan = std::make_unique<AdHocSchemaManager>();
        auto tagId = 3001;
        schemaMan->addTagSchema(spaceId,
                                tagId,
                                TestUtils::genTagSchemaWithTTLProvider(tagId, 3, 3));

        schemaMan->addEdgeSchema(spaceId, 101, TestUtils::genEdgeSchemaWithTTLProvider(10, 10));
        return schemaMan;
    }

    static std::unique_ptr<meta::IndexManager> mockIndexMan(GraphSpaceID spaceId = 0,
                                                            TagID startTag = 3001,
                                                            TagID endTag = 3010,
                                                            EdgeType startEdge = 101,
                                                            EdgeType endEdge = 110) {
        auto* indexMan = new AdHocIndexManager();
        for (TagID tagId = startTag; tagId < endTag; tagId++) {
            std::vector<nebula::cpp2::ColumnDef> columns;
            for (int32_t i = 0; i < 3; i++) {
                nebula::cpp2::ColumnDef column;
                column.name = folly::stringPrintf("tag_%d_col_%d", tagId, i);
                column.type.type = nebula::cpp2::SupportedType::INT;
                columns.emplace_back(std::move(column));
            }
            for (int32_t i = 3; i < 6; i++) {
                nebula::cpp2::ColumnDef column;
                column.name = folly::stringPrintf("tag_%d_col_%d", tagId, i);
                column.type.type = nebula::cpp2::SupportedType::STRING;
                columns.emplace_back(std::move(column));
            }
            indexMan->addTagIndex(spaceId, tagId + 1000, tagId, std::move(columns));
        }

        for (EdgeType edgeType = startEdge; edgeType < endEdge; edgeType++) {
            std::vector<nebula::cpp2::ColumnDef> columns;
            for (int32_t i = 0; i < 10; i++) {
                nebula::cpp2::ColumnDef column;
                column.name = folly::stringPrintf("col_%d", i);
                column.type.type = nebula::cpp2::SupportedType::INT;
                columns.emplace_back(std::move(column));
            }
            for (int32_t i = 10; i < 20; i++) {
                nebula::cpp2::ColumnDef column;
                column.name = folly::stringPrintf("col_%d", i);
                column.type.type = nebula::cpp2::SupportedType::STRING;
                columns.emplace_back(std::move(column));
            }
            indexMan->addEdgeIndex(spaceId, edgeType + 100, edgeType, std::move(columns));
        }

        std::unique_ptr<meta::IndexManager> im(indexMan);
        return im;
    }

    static std::unique_ptr<meta::IndexManager> mockMultiIndexMan(GraphSpaceID spaceId = 0,
                                                                 TagID startTag = 3001,
                                                                 TagID endTag = 3010,
                                                                 EdgeType startEdge = 101,
                                                                 EdgeType endEdge = 110) {
        auto* indexMan = new AdHocIndexManager();
        for (TagID tagId = startTag; tagId < endTag; tagId++) {
            for (int32_t i = 0; i < 3; i++) {
                std::vector<nebula::cpp2::ColumnDef> columns;
                nebula::cpp2::ColumnDef column;
                column.name = folly::stringPrintf("tag_%d_col_%d", tagId, i);
                column.type.type = nebula::cpp2::SupportedType::INT;
                columns.emplace_back(std::move(column));

                column.name = folly::stringPrintf("tag_%d_col_%d", tagId, i + 3);
                column.type.type = nebula::cpp2::SupportedType::STRING;
                columns.emplace_back(std::move(column));
                indexMan->addTagIndex(spaceId, tagId + 1000, tagId, std::move(columns));
            }
        }

        for (EdgeType edgeType = startEdge; edgeType < endEdge; edgeType++) {
            for (int32_t i = 0; i < 10; i++) {
                std::vector<nebula::cpp2::ColumnDef> columns;
                nebula::cpp2::ColumnDef column;
                column.name = folly::stringPrintf("col_%d", i);
                column.type.type = nebula::cpp2::SupportedType::INT;
                columns.emplace_back(std::move(column));
                column.name = folly::stringPrintf("col_%d", i + 10);
                column.type.type = nebula::cpp2::SupportedType::STRING;
                columns.emplace_back(std::move(column));
                indexMan->addEdgeIndex(spaceId, edgeType + 100, edgeType, std::move(columns));
            }
        }

        std::unique_ptr<meta::IndexManager> im(indexMan);
        return im;
    }

    static std::vector<cpp2::Vertex>
    setupVertices(PartitionID partitionID,
                  VertexID vIdFrom,
                  VertexID vIdTo,
                  TagID tagFrom = 0,
                  TagID tagTo = 10) {
        std::vector<cpp2::Vertex> vertices;
        for (VertexID vId = vIdFrom;  vId < vIdTo; vId++) {
              std::vector<cpp2::Tag> tags;
              for (TagID tId = tagFrom; tId < tagTo; tId++) {
                  cpp2::Tag t;
                  t.set_tag_id(tId);
                  auto val = encodeValue(partitionID, vId, tId);
                  t.set_props(std::move(val));
                  tags.emplace_back(std::move(t));
              }
              cpp2::Vertex v;
              v.set_id(vId);
              v.set_tags(std::move(tags));
              vertices.emplace_back(std::move(v));
        }
        return vertices;
    }

    static std::string encodeValue(PartitionID partitionID,
                                   VertexID vertexID,
                                   TagID tagID) {
        RowWriter writer;
        for (int32_t numInt = 0; numInt < 3; numInt++) {
            writer << numInt;
        }
        for (int32_t numString = 3; numString < 6; numString++) {
            writer << folly::stringPrintf("col_%d_%d_%ld_%d",
                                          numString,
                                          partitionID,
                                          vertexID,
                                          tagID);
        }
        return writer.encode();
    }

    static std::string encodeValue(PartitionID partitionID,
                                   VertexID sourceID,
                                   VertexID destinationID,
                                   EdgeType edgeType,
                                   SchemaVer version = 0,
                                   std::string fmt = "%d_%d_%ld_%ld_%d_%ld") {
        RowWriter writer;
        for (int32_t numInt = 0; numInt < 10; numInt++) {
            writer << numInt;
        }
        for (int32_t numString = 10; numString < 20; numString++) {
            writer << folly::stringPrintf(fmt.c_str(),
                                          numString,
                                          partitionID,
                                          sourceID,
                                          destinationID,
                                          edgeType,
                                          version);
        }
        return writer.encode();
    }

    static std::vector<cpp2::Edge>
    setupEdges(PartitionID partitionID,
               VertexID srcFrom,
               VertexID srcTo,
               EdgeType edgeType = 101,
               int32_t versionNum = 1,
               std::string fmt = "%d_%d_%ld_%ld_%d_%ld") {
        std::vector<cpp2::Edge> edges;
        for (VertexID srcId = srcFrom; srcId < srcTo; srcId++) {
            for (SchemaVer version = 0; version < versionNum; version++) {
                VertexID dstId = srcId * 100 + 2;
                EdgeRanking ranking = srcId * 100 + 3;
                cpp2::EdgeKey key;
                key.set_src(srcId);
                key.set_edge_type(edgeType);
                key.set_ranking(ranking);
                key.set_dst(dstId);
                edges.emplace_back();
                edges.back().set_key(std::move(key));
                auto val = encodeValue(partitionID, srcId, dstId, edgeType, version, fmt);
                edges.back().set_props(std::move(val));
            }
        }
        return edges;
    }

    static std::string setupEncode(int32_t intSize = 3, int32_t stringSize = 6) {
        RowWriter writer;
        for (int32_t numInt = 0; numInt < intSize; numInt++) {
            writer << numInt;
        }
        for (int32_t numString = intSize; numString < stringSize; numString++) {
            writer << folly::stringPrintf("string_col_%d", numString);
        }
        return writer.encode();
    }

    /**
     * It will generate edge SchemaProvider with some int fields and string fields
     * */
    static std::shared_ptr<meta::SchemaProviderIf>
    genEdgeSchemaProvider(int32_t intFieldsNum, int32_t stringFieldsNum) {
        std::shared_ptr<meta::NebulaSchemaProvider> schema(new meta::NebulaSchemaProvider(0));
        nebula::cpp2::Value defaultValue;
        defaultValue.set_int_value(0);
        for (int32_t i = 0; i < intFieldsNum; i++) {
            nebula::cpp2::ColumnDef column;
            column.name = folly::stringPrintf("col_%d", i);
            column.type.type = nebula::cpp2::SupportedType::INT;
            schema->addField(column.name, std::move(column.type));
            schema->addDefaultValue(column.name, defaultValue);
        }
        defaultValue.set_string_value("");
        for (int32_t i = intFieldsNum; i < intFieldsNum + stringFieldsNum; i++) {
            nebula::cpp2::ColumnDef column;
            column.name = folly::stringPrintf("col_%d", i);
            column.type.type = nebula::cpp2::SupportedType::STRING;
            schema->addField(column.name, std::move(column.type));
            schema->addDefaultValue(column.name, defaultValue);
        }
        return schema;
    }

    /**
     * It will generate edge SchemaProvider with some int fields and string fields and ttl
     * */
    static std::shared_ptr<meta::SchemaProviderIf>
    genEdgeSchemaWithTTLProvider(int32_t intFieldsNum, int32_t stringFieldsNum) {
        std::shared_ptr<meta::NebulaSchemaProvider> schema(new meta::NebulaSchemaProvider(0));
        for (int32_t i = 0; i < intFieldsNum; i++) {
            nebula::cpp2::ColumnDef column;
            column.name = folly::stringPrintf("col_%d", i);
            column.type.type = nebula::cpp2::SupportedType::INT;
            schema->addField(column.name, std::move(column.type));
        }
        for (int32_t i = intFieldsNum; i < intFieldsNum + stringFieldsNum; i++) {
            nebula::cpp2::ColumnDef column;
            column.name = folly::stringPrintf("col_%d", i);
            column.type.type = nebula::cpp2::SupportedType::STRING;
            schema->addField(column.name, std::move(column.type));
        }
        nebula::cpp2::SchemaProp prop;
        prop.set_ttl_duration(200);
        prop.set_ttl_col(folly::stringPrintf("col_0"));
        schema->setProp(prop);
        return schema;
    }


    /**
     * It will generate tag SchemaProvider with some int fields and string fields
     * */
    static std::shared_ptr<meta::SchemaProviderIf>
    genTagSchemaProvider(TagID tagId, int32_t intFieldsNum, int32_t stringFieldsNum) {
        std::shared_ptr<meta::NebulaSchemaProvider> schema(new meta::NebulaSchemaProvider(0));
        nebula::cpp2::Value defaultValue;
        defaultValue.set_int_value(0);
        for (int32_t i = 0; i < intFieldsNum; i++) {
            nebula::cpp2::ColumnDef column;
            column.name = folly::stringPrintf("tag_%d_col_%d", tagId, i);
            column.type.type = nebula::cpp2::SupportedType::INT;
            schema->addField(column.name, std::move(column.type));
            schema->addDefaultValue(column.name, defaultValue);
        }
        defaultValue.set_string_value("");
        for (int32_t i = intFieldsNum; i < intFieldsNum + stringFieldsNum; i++) {
            nebula::cpp2::ColumnDef column;
            column.name = folly::stringPrintf("tag_%d_col_%d", tagId, i);
            column.type.type = nebula::cpp2::SupportedType::STRING;
            schema->addField(column.name, std::move(column.type));
            schema->addDefaultValue(column.name, defaultValue);
        }
        return schema;
    }


    /**
     * It will generate tag SchemaProvider with some int fields and string fields and ttl
     * */
    static std::shared_ptr<meta::SchemaProviderIf> genTagSchemaWithTTLProvider(
            TagID tagId,
            int32_t intFieldsNum,
            int32_t stringFieldsNum) {
        std::shared_ptr<meta::NebulaSchemaProvider> schema(new meta::NebulaSchemaProvider(0));
        for (int32_t i = 0; i < intFieldsNum; i++) {
            nebula::cpp2::ColumnDef column;
            column.name = folly::stringPrintf("tag_%d_col_%d", tagId, i);
            column.type.type = nebula::cpp2::SupportedType::INT;
            schema->addField(column.name, std::move(column.type));
        }
        for (int32_t i = intFieldsNum; i < intFieldsNum + stringFieldsNum; i++) {
            nebula::cpp2::ColumnDef column;
            column.name = folly::stringPrintf("tag_%d_col_%d", tagId, i);
            column.type.type = nebula::cpp2::SupportedType::STRING;
            schema->addField(column.name, std::move(column.type));
        }
        nebula::cpp2::SchemaProp prop;
        prop.set_ttl_duration(200);
        prop.set_ttl_col(folly::stringPrintf("tag_%d_col_0", tagId));
        schema->setProp(prop);
        return schema;
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
    static std::unique_ptr<test::ServerContext>
    mockStorageServer(meta::MetaClient* mClient, const char* dataPath, uint32_t ip,
                      uint32_t port = 0, bool useMetaServer = false, GraphSpaceID space = 1) {
        auto sc = std::make_unique<test::ServerContext>();
        // Always use the Meta Service in this case
        sc->kvStore_ = TestUtils::initKV(dataPath, 6, {ip, port}, mClient, true);

        if (!useMetaServer) {
            sc->schemaMan_ = TestUtils::mockSchemaMan(space);
            sc->indexMan_ = TestUtils::mockIndexMan(space);
        } else {
            LOG(INFO) << "Create real SchemaManager and IndexManager";
            sc->schemaMan_ = meta::SchemaManager::create();
            sc->schemaMan_->init(mClient);

            sc->indexMan_ = meta::IndexManager::create();
            sc->indexMan_->init(mClient);
        }


        auto handler = std::make_shared<nebula::storage::StorageServiceHandler>(
            sc->kvStore_.get(), sc->schemaMan_.get(), sc->indexMan_.get(), mClient);
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

template <typename T>
void checkTagData(const std::vector<cpp2::TagData>& data,
                  TagID tid,
                  const std::string col_name,
                  const std::shared_ptr<const meta::SchemaProviderIf> schema,
                  T expected) {
    auto it = std::find_if(data.cbegin(), data.cend(), [tid](auto& td) {
        if (td.tag_id == tid) {
            return true;
        }
        return false;
    });
    DCHECK(it != data.cend()) << "Tag ID: " << tid;
    auto tagReader   = RowReader::getRowReader(it->data, schema);
    auto r = RowReader::getPropByName(tagReader.get(), col_name);
    CHECK(ok(r));
    auto col = boost::get<T>(value(r));
    EXPECT_EQ(col, expected);
}
}  // namespace storage
}  // namespace nebula

#endif  // STORAGE_TEST_TESTUTILS_H_
