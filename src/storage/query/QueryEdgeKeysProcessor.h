/* Copyright (c) 2019 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef STORAGE_QUERY_QUERYEDGEKEYSPROCESSOR_H_
#define STORAGE_QUERY_QUERYEDGEKEYSPROCESSOR_H_

#include "base/Base.h"
#include "storage/BaseProcessor.h"

namespace nebula {
namespace storage {

class QueryEdgeKeysProcessor : public BaseProcessor<cpp2::EdgeKeyResponse> {
public:
    static QueryEdgeKeysProcessor* instance(kvstore::KVStore* kvstore,
                                            meta::SchemaManager* schemaMan) {
        return new QueryEdgeKeysProcessor(kvstore, schemaMan);
    }

     void process(const cpp2::EdgeKeyRequest& req);

private:
    explicit QueryEdgeKeysProcessor(kvstore::KVStore* kvstore, meta::SchemaManager* schemaMan)
            : BaseProcessor<cpp2::EdgeKeyResponse>(kvstore, schemaMan) {}
};

}  // namespace storage
}  // namespace nebula
#endif  // STORAGE_QUERY_QUERYEDGEKEYSPROCESSOR_H_
