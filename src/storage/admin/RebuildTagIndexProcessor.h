/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef STORAGE_ADMIN_REBUILDTAGINDEXPROCESSOR_H_
#define STORAGE_ADMIN_REBUILDTAGINDEXPROCESSOR_H_

#include "kvstore/KVStore.h"
#include "kvstore/KVIterator.h"
#include "meta/SchemaManager.h"
#include "storage/BaseProcessor.h"

namespace nebula {
namespace storage {

class RebuildTagIndexProcessor : public BaseProcessor<cpp2::AdminExecResp> {
public:
    static RebuildTagIndexProcessor* instance(kvstore::KVStore* kvstore,
                                              meta::SchemaManager* schemaMan,
                                              meta::IndexManager* indexMan) {
        return new RebuildTagIndexProcessor(kvstore, schemaMan, indexMan);
    }

    void process(const cpp2::RebuildIndexRequest& req);

private:
    explicit RebuildTagIndexProcessor(kvstore::KVStore* kvstore,
                                      meta::SchemaManager* schemaMan,
                                      meta::IndexManager* indexMan)
            : BaseProcessor<cpp2::AdminExecResp>(kvstore, schemaMan, nullptr)
            , indexMan_(indexMan) {}

    std::string partitionRebuildIndex(GraphSpaceID space,
                                      PartitionID part,
                                      TagID tag,
                                      std::shared_ptr<nebula::cpp2::IndexItem> item);

private:
    meta::IndexManager* indexMan_{nullptr};
};

}  // namespace storage
}  // namespace nebula
#endif  // STORAGE_ADMIN_REBUILDTAGINDEXPROCESSOR_H_
