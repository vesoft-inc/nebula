/* Copyright (c) 2019 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef STORAGE_CLEANINDEXLOGPROCESSOR_H_
#define STORAGE_CLEANINDEXLOGPROCESSOR_H_

#include "base/Base.h"
#include "storage/IndexBaseProcessor.h"

namespace nebula {
namespace storage {

class CleanIndexLogProcessor : public IndexBaseProcessor<cpp2::ExecResponse> {
public:
    static CleanIndexLogProcessor* instance(kvstore::KVStore* kvstore,
                                          meta::SchemaManager* schemaMan) {
        return new CleanIndexLogProcessor(kvstore, schemaMan);
    }

    void process(const cpp2::CleanIndexLogReq& req);

private:
    explicit CleanIndexLogProcessor(kvstore::KVStore* kvstore, meta::SchemaManager* schemaMan)
            : IndexBaseProcessor<cpp2::ExecResponse>(kvstore, schemaMan) {}
};

}  // namespace storage
}  // namespace nebula

#endif  // STORAGE_CLEANINDEXLOGPROCESSOR_H_

