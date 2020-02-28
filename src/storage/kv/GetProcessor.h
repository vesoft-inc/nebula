/* Copyright (c) 2019 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef STORAGE_KV_GETPROCESSOR_H_
#define STORAGE_KV_GETPROCESSOR_H_

#include "base/Base.h"
#include "storage/BaseProcessor.h"

namespace nebula {
namespace storage {

class GetProcessor : public BaseProcessor<cpp2::GeneralResponse> {
public:
    static GetProcessor* instance(kvstore::KVStore* kvstore,
                                  meta::SchemaManager* schemaMan,
                                  stats::Stats* stats) {
        return new GetProcessor(kvstore, schemaMan, stats);
    }

    void process(const cpp2::GetRequest& req);

protected:
    explicit GetProcessor(kvstore::KVStore* kvstore,
                          meta::SchemaManager* schemaMan,
                          stats::Stats* stats):
        BaseProcessor<cpp2::GeneralResponse>(kvstore, schemaMan, stats) {}
};

}  // namespace storage
}  // namespace nebula

#endif  // STORAGE_KV_GETPROCESSOR_H_
