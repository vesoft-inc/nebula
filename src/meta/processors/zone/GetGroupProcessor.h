/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef META_GETGROUPPROCESSOR_H
#define META_GETGROUPPROCESSOR_H

#include "meta/processors/BaseProcessor.h"

namespace nebula {
namespace meta {

class GetGroupProcessor : public BaseProcessor<cpp2::GetGroupResp> {
public:
    static GetGroupProcessor* instance(kvstore::KVStore* kvstore) {
        return new GetGroupProcessor(kvstore);
    }

    void process(const cpp2::GetGroupReq& req);

private:
    explicit GetGroupProcessor(kvstore::KVStore* kvstore)
        : BaseProcessor<cpp2::GetGroupResp>(kvstore) {}
};

}  // namespace meta
}  // namespace nebula
#endif  // META_GETGROUPPROCESSOR_H
