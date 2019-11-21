/* Copyright (c) 2019 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef STORAGE_MUTATE_DELETEEDGESPROCESSOR_H_
#define STORAGE_MUTATE_DELETEEDGESPROCESSOR_H_

#include "base/Base.h"
#include "storage/BaseProcessor.h"
#include "kvstore/LogEncoder.h"

namespace nebula {
namespace storage {

class DeleteEdgesProcessor : public BaseProcessor<cpp2::ExecResponse> {
public:
    static DeleteEdgesProcessor* instance(kvstore::KVStore* kvstore,
                                          meta::SchemaManager* schemaMan) {
        return new DeleteEdgesProcessor(kvstore, schemaMan);
    }

     void process(const cpp2::DeleteEdgesRequest& req);

private:
    explicit DeleteEdgesProcessor(kvstore::KVStore* kvstore,
                                  meta::SchemaManager* schemaMan)
            : BaseProcessor<cpp2::ExecResponse>(kvstore, schemaMan) {}

    std::string deleteEdges(GraphSpaceID spaceId,
                            PartitionID partId,
                            const std::vector<cpp2::EdgeKey>& edges);

private:
    const std::vector<cpp2::IndexItem>* indexes_{nullptr};
};

}  // namespace storage
}  // namespace nebula
#endif  // STORAGE_MUTATE_DELETEEDGESPROCESSOR_H_
