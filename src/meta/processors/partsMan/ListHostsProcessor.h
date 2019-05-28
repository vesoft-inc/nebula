/* Copyright (c) 2018 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef META_LISTHOSTSPROCESSOR_H_
#define META_LISTHOSTSPROCESSOR_H_

#include "meta/processors/BaseProcessor.h"

namespace nebula {
namespace meta {

class ListHostsProcessor : public BaseProcessor<cpp2::ListHostsResp> {
public:
    static ListHostsProcessor* instance(kvstore::KVStore* kvstore) {
        return new ListHostsProcessor(kvstore);
    }

    void process(const cpp2::ListHostsReq& req);

private:
    explicit ListHostsProcessor(kvstore::KVStore* kvstore)
            : BaseProcessor<cpp2::ListHostsResp>(kvstore) {}

    /**
     * Get all hosts with online/offline status.
     * */
    StatusOr<std::vector<cpp2::HostItem>> allHostsWithStatus();
};

}  // namespace meta
}  // namespace nebula

#endif  // META_LISTHOSTSPROCESSOR_H_
