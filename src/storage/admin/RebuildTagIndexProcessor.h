/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef STORAGE_ADMIN_REBUILDTAGINDEXPROCESSOR_H_
#define STORAGE_ADMIN_REBUILDTAGINDEXPROCESSOR_H_

#include "kvstore/KVStore.h"
#include "kvstore/KVIterator.h"

#include "storage/admin/RebuildIndexProcessor.h"

namespace nebula {
namespace storage {

class RebuildTagIndexProcessor : public RebuildIndexProcessor {
public:
    static RebuildTagIndexProcessor* instance(kvstore::KVStore* kvstore,
                                              meta::SchemaManager* schemaMan,
                                              meta::IndexManager* indexMan,
                                              stats::Stats* stats,
                                              StorageEnvironment* env) {
        return new RebuildTagIndexProcessor(kvstore, schemaMan, indexMan, stats, env);
    }

    void process(const cpp2::RebuildIndexRequest& req);

private:
    explicit RebuildTagIndexProcessor(kvstore::KVStore* kvstore,
                                      meta::SchemaManager* schemaMan,
                                      meta::IndexManager* indexMan,
                                      stats::Stats* stats,
                                      StorageEnvironment* env)
            : RebuildIndexProcessor(kvstore, schemaMan, indexMan, stats, env) {}

    StatusOr<std::shared_ptr<nebula::cpp2::IndexItem>>
    getIndex(GraphSpaceID space, IndexID indexID) override;

    void buildIndex(GraphSpaceID space,
                    PartitionID part,
                    nebula::cpp2::SchemaID SchemaID,
                    IndexID indexID,
                    kvstore::KVIterator* iter,
                    const std::vector<nebula::cpp2::ColumnDef>& cols) override;
};

}  // namespace storage
}  // namespace nebula
#endif  // STORAGE_ADMIN_REBUILDTAGINDEXPROCESSOR_H_
