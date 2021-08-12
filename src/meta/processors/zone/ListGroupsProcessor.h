/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef META_LISTGROUPSPROCESSOR_H
#define META_LISTGROUPSPROCESSOR_H

#include "meta/processors/BaseProcessor.h"

namespace nebula {
namespace meta {

class ListGroupsProcessor : public BaseProcessor<cpp2::ListGroupsResp> {
public:
    static ListGroupsProcessor* instance(kvstore::KVStore* kvstore) {
        return new ListGroupsProcessor(kvstore);
    }

    void process(const cpp2::ListGroupsReq& req);

private:
    explicit ListGroupsProcessor(kvstore::KVStore* kvstore)
        : BaseProcessor<cpp2::ListGroupsResp>(kvstore) {}
};

}  // namespace meta
}  // namespace nebula
#endif  // META_LISTGROUPSPROCESSOR_H
