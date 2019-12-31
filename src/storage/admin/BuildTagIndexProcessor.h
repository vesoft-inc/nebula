/* Copyright (c) 2019 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef STORAGE_ADMIN_BUILDTAGINDEXPROCESSOR_H_
#define STORAGE_ADMIN_BUILDTAGINDEXPROCESSOR_H_

#include "kvstore/KVStore.h"
#include "kvstore/KVIterator.h"
#include "meta/SchemaManager.h"
#include "storage/BaseProcessor.h"
#include "storage/StorageFlags.h"

namespace nebula {
namespace storage {

class BuildTagIndexProcessor : public BaseProcessor<cpp2::AdminExecResp> {
public:
    static BuildTagIndexProcessor* instance(kvstore::KVStore* kvstore,
                                            meta::SchemaManager* schemaMan) {
        return new BuildTagIndexProcessor(kvstore, schemaMan);
    }

    void process(const cpp2::BuildTagIndexRequest& req);

private:
    explicit BuildTagIndexProcessor(kvstore::KVStore* kvstore,
                                    meta::SchemaManager* schemaMan)
            : BaseProcessor<cpp2::AdminExecResp>(kvstore, schemaMan, nullptr) {}
};

}  // namespace storage
}  // namespace nebula
#endif  // STORAGE_ADMIN_BUILDTAGINDEXPROCESSOR_H_
