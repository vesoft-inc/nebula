/* Copyright (c) 2019 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef STORAGE_ADMIN_BUILDEDGEINDEXPROCESSOR_H_
#define STORAGE_ADMIN_BUILDEDGEINDEXPROCESSOR_H_

#include "kvstore/KVStore.h"
#include "kvstore/KVIterator.h"
#include "meta/SchemaManager.h"
#include "storage/StorageFlags.h"
#include "storage/BaseProcessor.h"

namespace nebula {
namespace storage {

class BuildEdgeIndexProcessor : public BaseProcessor<cpp2::AdminExecResp> {
public:
    static BuildEdgeIndexProcessor* instance(kvstore::KVStore* kvstore,
                                             meta::SchemaManager* schemaMan) {
        return new BuildEdgeIndexProcessor(kvstore, schemaMan);
    }

    void process(const cpp2::BuildEdgeIndexRequest& req);

private:
    explicit BuildEdgeIndexProcessor(kvstore::KVStore* kvstore,
                                     meta::SchemaManager* schemaMan)
            : BaseProcessor<cpp2::AdminExecResp>(kvstore, schemaMan, nullptr) {}
};

}  // namespace storage
}  // namespace nebula
#endif  // STORAGE_ADMIN_BUILDEDGEINDEXPROCESSOR_H_
