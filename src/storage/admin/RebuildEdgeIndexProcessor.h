/* Copyright (c) 2019 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef STORAGE_ADMIN_REBUILDEDGEINDEXPROCESSOR_H_
#define STORAGE_ADMIN_REBUILDEDGEINDEXPROCESSOR_H_

#include "kvstore/KVStore.h"
#include "kvstore/KVIterator.h"
#include "meta/SchemaManager.h"
#include "storage/StorageFlags.h"
#include "storage/BaseProcessor.h"

namespace nebula {
namespace storage {

class RebuildEdgeIndexProcessor : public BaseProcessor<cpp2::AdminExecResp> {
public:
    static RebuildEdgeIndexProcessor* instance(kvstore::KVStore* kvstore,
                                               meta::SchemaManager* schemaMan) {
        return new RebuildEdgeIndexProcessor(kvstore, schemaMan);
    }

    void process(const cpp2::RebuildEdgeIndexRequest& req);

private:
    explicit RebuildEdgeIndexProcessor(kvstore::KVStore* kvstore,
                                       meta::SchemaManager* schemaMan)
            : BaseProcessor<cpp2::AdminExecResp>(kvstore, schemaMan, nullptr) {}
};

}  // namespace storage
}  // namespace nebula
#endif  // STORAGE_ADMIN_REBUILDEDGEINDEXPROCESSOR_H_
