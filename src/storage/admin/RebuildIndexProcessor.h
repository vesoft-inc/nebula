/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef STORAGE_ADMIN_REBUILDINDEXPROCESSOR_H_
#define STORAGE_ADMIN_REBUILDINDEXPROCESSOR_H_

#include "kvstore/LogEncoder.h"
#include "meta/SchemaManager.h"
#include "storage/BaseProcessor.h"

namespace nebula {
namespace storage {

class RebuildIndexProcessor : public BaseProcessor<cpp2::AdminExecResp> {
public:
    explicit RebuildIndexProcessor(kvstore::KVStore* kvstore,
                                   meta::SchemaManager* schemaMan,
                                   meta::IndexManager* indexMan,
                                   stats::Stats* stats,
                                   StorageEnvironment* env)
            : BaseProcessor<cpp2::AdminExecResp>(kvstore, schemaMan, stats, env)
            , indexMan_(indexMan) {}

protected:
    void processInternal(const cpp2::RebuildIndexRequest& req);

    virtual StatusOr<std::shared_ptr<nebula::cpp2::IndexItem>>
    getIndex(GraphSpaceID space, IndexID indexID) = 0;

    virtual void buildIndex(GraphSpaceID space,
                            PartitionID part,
                            nebula::cpp2::SchemaID SchemaID,
                            IndexID indexID,
                            kvstore::KVIterator* iter,
                            const std::vector<nebula::cpp2::ColumnDef>& cols) = 0;

private:
    void processModifyOperation(GraphSpaceID space,
                                PartitionID part,
                                const folly::StringPiece& opKey);

    std::string modifyOpLog(const folly::StringPiece& opKey);

    void processDeleteOperation(GraphSpaceID space,
                                PartitionID part,
                                const folly::StringPiece& opKey);

    std::string deleteOpLog(const folly::StringPiece& opKey);

protected:
    meta::IndexManager* indexMan_{nullptr};
};

}  // namespace storage
}  // namespace nebula
#endif  // STORAGE_ADMIN_REBUILDINDEXPROCESSOR_H_
