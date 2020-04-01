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
                                          meta::SchemaManager* schemaMan,
                                          meta::IndexManager* indexMan) {
        return new DeleteEdgesProcessor(kvstore, schemaMan, indexMan);
    }

     void process(const cpp2::DeleteEdgesRequest& req);

private:
    explicit DeleteEdgesProcessor(kvstore::KVStore* kvstore,
                                  meta::SchemaManager* schemaMan,
                                  meta::IndexManager* indexMan)
            : BaseProcessor<cpp2::ExecResponse>(kvstore, schemaMan)
            , indexMan_(indexMan) {}


    folly::Optional<std::string> deleteEdges(GraphSpaceID spaceId,
                                             PartitionID partId,
                                             const std::vector<cpp2::EdgeKey>& edges);

private:
    meta::IndexManager*                                   indexMan_{nullptr};
    std::vector<std::shared_ptr<nebula::cpp2::IndexItem>> indexes_;
};

}  // namespace storage
}  // namespace nebula
#endif  // STORAGE_MUTATE_DELETEEDGESPROCESSOR_H_
