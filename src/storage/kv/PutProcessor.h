/* Copyright (c) 2019 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef STORAGE_KV_PUTPROCESSOR_H_
#define STORAGE_KV_PUTPROCESSOR_H_

#include "base/Base.h"
#include "storage/BaseProcessor.h"

namespace nebula {
namespace storage {

class PutProcessor : public BaseProcessor<cpp2::ExecResponse> {
public:
    static PutProcessor* instance(kvstore::KVStore* kvstore,
                                  meta::SchemaManager* schemaMan,
                                  stats::Stats* stats) {
        return new PutProcessor(kvstore, schemaMan, stats);
    }

    void process(const cpp2::PutRequest& req);

private:
    explicit PutProcessor(kvstore::KVStore* kvstore,
                          meta::SchemaManager* schemaMan,
                          stats::Stats* stats)
            : BaseProcessor<cpp2::ExecResponse>(kvstore, schemaMan, stats) {}
};

}  // namespace storage
}  // namespace nebula

#endif  // STORAGE_KV_PUTPROCESSOR_H_
