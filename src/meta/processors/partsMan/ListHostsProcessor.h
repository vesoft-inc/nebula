/* Copyright (c) 2018 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef META_LISTHOSTSPROCESSOR_H_
#define META_LISTHOSTSPROCESSOR_H_

#include "meta/processors/BaseProcessor.h"
#include "meta/processors/admin/AdminClient.h"

namespace nebula {
namespace meta {

class ListHostsProcessor : public BaseProcessor<cpp2::ListHostsResp> {
public:
    static ListHostsProcessor* instance(kvstore::KVStore* kvstore,
                                        AdminClient* adminClient = nullptr) {
        return new ListHostsProcessor(kvstore, adminClient);
    }

    void process(const cpp2::ListHostsReq& req);

private:
    explicit ListHostsProcessor(kvstore::KVStore* kvstore, AdminClient* adminClient)
            : BaseProcessor<cpp2::ListHostsResp>(kvstore)
            , adminClient_(adminClient) {}

    /**
     * Get all hosts with online/offline status and partition distribution.
     * */
    StatusOr<std::vector<cpp2::HostItem>>
    allHostsWithStatus(std::unordered_map<GraphSpaceID, std::string>& spaceIdNameMap);

    /**
     * Get all hosts with storage leader distribution.
     * */
    void getLeaderDist(std::vector<cpp2::HostItem>& hostItems,
                       std::unordered_map<GraphSpaceID, std::string>& spaceIdNameMap);

    AdminClient* adminClient_;
};

}  // namespace meta
}  // namespace nebula

#endif  // META_LISTHOSTSPROCESSOR_H_
