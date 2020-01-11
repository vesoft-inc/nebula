/* Copyright (c) 2019 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef STORAGE_LOOKUPEDGEINDEXPROCESSOR_H
#define STORAGE_LOOKUPEDGEINDEXPROCESSOR_H

#include "base/Base.h"
#include "kvstore/NebulaStore.h"
#include "storage/index/IndexExecutor.h"

namespace nebula {
namespace storage {
class LookUpEdgeIndexProcessor
    : public IndexExecutor<cpp2::LookUpEdgeIndexResp> {
public:
    static LookUpEdgeIndexProcessor* instance(kvstore::KVStore* kvstore,
                                            meta::SchemaManager* schemaMan,
                                            stats::Stats* stats) {
        return new LookUpEdgeIndexProcessor(kvstore, schemaMan, stats);
    }

    void process(const cpp2::LookUpIndexRequest& req);

private:
    explicit LookUpEdgeIndexProcessor(kvstore::KVStore* kvstore,
                                    meta::SchemaManager* schemaMan,
                                    stats::Stats* stats)
        : IndexExecutor<cpp2::LookUpEdgeIndexResp>
            (kvstore, schemaMan, stats, nullptr, true) {}
};
}  // namespace storage
}  // namespace nebula

#endif  // STORAGE_LOOKUPEDGEINDEXPROCESSOR_H

