/* Copyright (c) 2019 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef META_LISTPARTSPROCESSOR_H_
#define META_LISTPARTSPROCESSOR_H_

#include "meta/processors/BaseProcessor.h"
#include "meta/processors/admin/AdminClient.h"

namespace nebula {
namespace meta {

class ListPartsProcessor : public BaseProcessor<cpp2::ListPartsResp> {
public:
    static ListPartsProcessor* instance(kvstore::KVStore* kvstore,
                                        AdminClient* adminClient = nullptr) {
        return new ListPartsProcessor(kvstore, adminClient);
    }

    void process(const cpp2::ListPartsReq& req);

private:
    explicit ListPartsProcessor(kvstore::KVStore* kvstore, AdminClient* adminClient)
            : BaseProcessor<cpp2::ListPartsResp>(kvstore)
            , adminClient_(adminClient) {}


    // Get parts alloc information
    StatusOr<std::unordered_map<PartitionID, std::vector<nebula::cpp2::HostAddr>>> getAllParts();

    // Get all parts with storage leader distribution
    void getLeaderDist(std::vector<cpp2::PartItem>& partItems);

private:
    AdminClient*                                        adminClient_;
    GraphSpaceID                                        spaceId_;
};

}  // namespace meta
}  // namespace nebula

#endif  // META_LISTPARTSPROCESSOR_H_
