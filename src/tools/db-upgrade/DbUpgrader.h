/* Copyright (c) 2021 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef TOOLS_DBUPGRADE_DBUPGRADER_H_
#define TOOLS_DBUPGRADE_DBUPGRADER_H_

#include "common/base/Base.h"
#include "common/base/Status.h"
#include "common/clients/meta/MetaClient.h"
#include "common/meta/ServerBasedSchemaManager.h"
#include "common/meta/ServerBasedIndexManager.h"
#include <rocksdb/db.h>
#include <folly/executors/CPUThreadPoolExecutor.h>
#include <folly/executors/task_queue/UnboundedBlockingQueue.h>
#include "kvstore/RocksEngine.h"
#include "codec/RowReaderWrapper.h"
#include "codec/RowWriterV2.h"

DECLARE_string(src_db_path);
DECLARE_string(dst_db_path);
DECLARE_string(upgrade_meta_server);
DECLARE_uint32(write_batch_num);
DECLARE_uint32(upgrade_version);
DECLARE_bool(compactions);
DECLARE_uint32(max_concurrent_parts);
DECLARE_uint32(max_concurrent_spaces);

namespace nebula {
namespace storage {

// Upgrade a space of data path in storage conf
class UpgraderSpace {
public:
    UpgraderSpace() = default;

    ~UpgraderSpace() {
        if (pool_) {
            pool_->join();
        }
    }

    Status init(meta::MetaClient* mclient,
                meta::ServerBasedSchemaManager*sMan,
                meta::IndexManager* iMan,
                const std::string& srcPath,
                const std::string& dstPath,
                const std::string& entry);

    // Process v1 data and upgrade to v2 Ga
    void doProcessV1();

    // Processing v2 Rc data upgrade to v2 Ga
    void doProcessV2();

    // Perform manual compact
    void doCompaction();

    // Copy the latest wal file under each part, two at most
    bool copyWal();

private:
    Status initSpace(const std::string& spaceId);

    Status buildSchemaAndIndex();

    bool isValidVidLen(VertexID srcVId, VertexID dstVId = "");

    void encodeVertexValue(PartitionID partId,
                           RowReader* reader,
                           const meta::NebulaSchemaProvider* schema,
                           std::string& newkey,
                           VertexID& strVid,
                           TagID   tagId,
                           std::vector<kvstore::KV>& data);

    // Used for vertex and edge
    std::string encodeRowVal(const RowReader* reader,
                             const meta::NebulaSchemaProvider* schema,
                             std::vector<std::string>& fieldName);

    std::string indexVertexKey(PartitionID partId,
                               VertexID& vId,
                               RowReader* reader,
                               std::shared_ptr<nebula::meta::cpp2::IndexItem> index);

    void encodeEdgeValue(PartitionID partId,
                         RowReader* reader,
                         const meta::NebulaSchemaProvider* schema,
                         std::string& newkey,
                         VertexID& svId,
                         EdgeType type,
                         EdgeRanking rank,
                         VertexID& dstId,
                         std::vector<kvstore::KV>& data);

   std::string indexEdgeKey(PartitionID partId,
                            RowReader* reader,
                            VertexID& svId,
                            EdgeRanking rank,
                            VertexID& dstId,
                            std::shared_ptr<nebula::meta::cpp2::IndexItem> index);

    WriteResult convertValue(const meta::NebulaSchemaProvider* newSchema,
                             const meta::SchemaProviderIf* oldSchema,
                             std::string& name,
                             Value& val);
    void runPartV1();

    void runPartV2();

public:
    // Souce data path
    std::string                                                    srcPath_;
    // Destination data path
    std::string                                                    dstPath_;
    std::string                                                    entry_;

private:
    meta::MetaClient*                                              metaClient_;
    meta::ServerBasedSchemaManager*                                schemaMan_;
    meta::IndexManager*                                            indexMan_;

    // The following variables are space level
    GraphSpaceID                                                   spaceId_;
    int32_t                                                        spaceVidLen_;
    std::string                                                    spaceName_;
    std::vector<PartitionID>                                       parts_;
    std::unique_ptr<kvstore::RocksEngine>                          readEngine_;
    std::unique_ptr<kvstore::RocksEngine>                          writeEngine_;

    // Get all tag newest schema in space
    std::unordered_map<TagID,
        std::vector<std::shared_ptr<const meta::NebulaSchemaProvider>>>       tagSchemas_;

    // tagId ->  All field names in newest schema
    std::unordered_map<TagID, std::vector<std::string>>                       tagFieldName_;

    // tagId -> all indexes of this tag
    std::unordered_map<TagID,
        std::unordered_set<std::shared_ptr<nebula::meta::cpp2::IndexItem>>>   tagIndexes_;

    // Get all edge newest schema in space
    std::unordered_map<EdgeType,
        std::vector<std::shared_ptr<const meta::NebulaSchemaProvider>>>       edgeSchemas_;

    // edgetype -> all field name in newest schema
    std::unordered_map<EdgeType, std::vector<std::string>>                     edgeFieldName_;

    // edgetype -> all indexes of this edgetype
    std::unordered_map<EdgeType,
        std::unordered_set<std::shared_ptr<nebula::meta::cpp2::IndexItem>>>   edgeIndexes_;

    // for parallel parts
    std::unique_ptr<folly::IOThreadPoolExecutor>                              pool_{nullptr};

    folly::UnboundedBlockingQueue<PartitionID>                                partQueue_;

    std::atomic<size_t>                                                       unFinishedPart_;
};

// Upgrade one data path in storage conf
class DbUpgrader {
public:
    DbUpgrader() = default;

    ~DbUpgrader() {
        if (pool_) {
            pool_->join();
        }
    }

    Status init(meta::MetaClient* mclient,
                meta::ServerBasedSchemaManager*sMan,
                meta::IndexManager* iMan,
                const std::string& srcPath,
                const std::string& dstPath);

    void run();

private:
    // Get all string spaceId
    Status listSpace();

    void doProcessAllTagsAndEdges();

    void doSpace();

private:
    meta::MetaClient*                                               metaClient_;
    meta::ServerBasedSchemaManager*                                 schemaMan_;
    meta::IndexManager*                                             indexMan_;
    // Souce data path
    std::string                                                     srcPath_;

    // Destination data path
    std::string                                                     dstPath_;
    std::vector<std::string>                                        subDirs_;

    // for parallel spaces
    std::unique_ptr<folly::IOThreadPoolExecutor>                    pool_{nullptr};

    folly::UnboundedBlockingQueue<nebula::storage::UpgraderSpace*>  spaceQueue_;

    std::atomic<size_t>                                             unFinishedSpace_;
};

}  // namespace storage
}  // namespace nebula

#endif  // TOOLS_DBUPGRADE_DBUPGRADER_H_
