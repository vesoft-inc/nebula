/* Copyright (c) 2019 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef STORAGE_DELETEVERTICESPROCESSOR_H_
#define STORAGE_DELETEVERTICESPROCESSOR_H_

#include "base/Base.h"
#include "storage/BaseProcessor.h"

namespace nebula {
namespace storage {

class DeleteVerticesProcessor : public BaseProcessor<cpp2::ExecResponse> {
public:
    static DeleteVerticesProcessor* instance(kvstore::KVStore* kvstore,
                                             meta::SchemaManager* schemaMan,
                                             StorageStats* stats) {
        return new DeleteVerticesProcessor(kvstore, schemaMan, stats);
    }

    void process(const cpp2::DeleteVerticesRequest& req);

private:
    explicit DeleteVerticesProcessor(kvstore::KVStore* kvstore,
                                     meta::SchemaManager* schemaMan,
                                     StorageStats* stats)
            : BaseProcessor<cpp2::ExecResponse>(kvstore, schemaMan, stats) {}
};


}  // namespace storage
}  // namespace nebula
#endif  // STORAGE_DELETEVERTICESPROCESSOR_H_
