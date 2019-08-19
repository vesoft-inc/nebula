/* Copyright (c) 2019 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef STORAGE_BUILDINDEXPROCESSOR_H_
#define STORAGE_BUILDINDEXPROCESSOR_H_

#include "base/Base.h"
#include "storage/IndexBaseProcessor.h"

namespace nebula {
namespace storage {

class CreateIndexProcessor : public IndexBaseProcessor<cpp2::ExecResponse> {
public:
    static CreateIndexProcessor* instance(kvstore::KVStore* kvstore,
                                         meta::SchemaManager* schemaMan) {
        return new CreateIndexProcessor(kvstore, schemaMan);
    }

    void process(const cpp2::BuildIndexReq& req);

private:
    explicit CreateIndexProcessor(kvstore::KVStore* kvstore, meta::SchemaManager* schemaMan)
            : IndexBaseProcessor<cpp2::ExecResponse>(kvstore, schemaMan) {}
};

}  // namespace storage
}  // namespace nebula

#endif  // STORAGE_BUILDINDEXPROCESSOR_H_
