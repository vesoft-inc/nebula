/* Copyright (c) 2021 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef META_LISTCLUSTERINFOSPROCESSOR_H_
#define META_LISTCLUSTERINFOSPROCESSOR_H_

#include "meta/processors/BaseProcessor.h"
#include "meta/processors/admin/AdminClient.h"

namespace nebula {
namespace meta {

class ListClusterInfoProcessor : public BaseProcessor<cpp2::ListClusterInfoResp> {
public:
    static ListClusterInfoProcessor* instance(kvstore::KVStore* kvstore, AdminClient* client) {
        return new ListClusterInfoProcessor(kvstore, client);
    }
    void process(const cpp2::ListClusterInfoReq& req);

private:
    explicit ListClusterInfoProcessor(kvstore::KVStore* kvstore, AdminClient* client)
        : BaseProcessor<cpp2::ListClusterInfoResp>(kvstore), client_(client) {}
    AdminClient* client_;
};
}   // namespace meta
}   // namespace nebula

#endif   // META_LISTCLUSTERINFOSPROCESSOR_H_
