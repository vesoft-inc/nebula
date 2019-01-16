/* Copyright (c) 2018 - present, VE Software Inc. All rights reserved
 *
 * This source code is licensed under Apache 2.0 License
 *  (found in the LICENSE.Apache file in the root directory)
 */

#include "base/Base.h"
#include "kvstore/include/KVStore.h"
#include "kvstore/PartManager.h"
#include "kvstore/KVStoreImpl.h"
#include "meta/SchemaManager.h"
#include "dataman/ResultSchemaProvider.h"

DECLARE_string(part_man_type);

namespace nebula {
namespace storage {

class TestUtils {
public:
    static kvstore::KVStore* initKV(const char* rootPath) {
        FLAGS_part_man_type = "memory";  // Use MemPartManager.
        kvstore::MemPartManager* partMan = static_cast<kvstore::MemPartManager*>(
                                                            kvstore::PartManager::instance());
        // GraphSpaceID =>  {PartitionIDs}
        // 0 => {0, 1, 2, 3, 4, 5}
        auto& partsMap = partMan->partsMap();
        for (auto partId = 0; partId < 6; partId++) {
            partsMap[0][partId] = kvstore::PartMeta();
        }
        auto dataPath = folly::stringPrintf("%s/disk1, %s/disk2", rootPath, rootPath);
        std::vector<std::string> paths;
        paths.push_back(folly::stringPrintf("%s/disk1", rootPath));
        paths.push_back(folly::stringPrintf("%s/disk2", rootPath));

        kvstore::KVOptions options;
        options.local_ = HostAddr(0, 0);
        options.dataPaths_ = std::move(paths);

        kvstore::KVStoreImpl* kv = static_cast<kvstore::KVStoreImpl*>(
                                        kvstore::KVStore::instance(std::move(options)));
        return kv;
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
    static SchemaProviderIf* genEdgeSchemaProvider(int32_t intFieldsNum,
                                                   int32_t stringFieldsNum) {
        cpp2::Schema schema;
        for (auto i = 0; i < intFieldsNum; i++) {
            cpp2::ColumnDef column;
            column.name = folly::stringPrintf("col_%d", i);
            column.type.type = cpp2::SupportedType::INT;
            schema.columns.emplace_back(std::move(column));
        }
        for (auto i = intFieldsNum; i < intFieldsNum + stringFieldsNum; i++) {
            cpp2::ColumnDef column;
            column.name = folly::stringPrintf("col_%d", i);
            column.type.type = cpp2::SupportedType::STRING;
            schema.columns.emplace_back(std::move(column));
        }
        return new ResultSchemaProvider(std::move(schema));
    }

    /**
     * It will generate tag SchemaProvider with some int fields and string fields
     * */
    static SchemaProviderIf* genTagSchemaProvider(TagID tagId,
                                                  int32_t intFieldsNum,
                                                  int32_t stringFieldsNum) {
        cpp2::Schema schema;
        for (auto i = 0; i < intFieldsNum; i++) {
            cpp2::ColumnDef column;
            column.name = folly::stringPrintf("tag_%d_col_%d", tagId, i);
            column.type.type = cpp2::SupportedType::INT;
            schema.columns.emplace_back(std::move(column));
        }
        for (auto i = intFieldsNum; i < intFieldsNum + stringFieldsNum; i++) {
            cpp2::ColumnDef column;
            column.name = folly::stringPrintf("tag_%d_col_%d", tagId, i);
            column.type.type = cpp2::SupportedType::STRING;
            schema.columns.emplace_back(std::move(column));
        }
        return new ResultSchemaProvider(std::move(schema));
    }

    static cpp2::PropDef propDef(cpp2::PropOwner owner, std::string name, TagID tagId = -1) {
        cpp2::PropDef prop;
        prop.set_name(std::move(name));
        prop.set_owner(owner);
        if (tagId != -1) {
            prop.set_tag_id(tagId);
        }
        return prop;
    }

    static cpp2::PropDef propDef(cpp2::PropOwner owner, std::string name,
                                 cpp2::StatType type, TagID tagId = -1) {
        auto prop = TestUtils::propDef(owner, name, tagId);
        prop.set_stat(type);
        return prop;
    }
};

}  // namespace storage
}  // namespace nebula

