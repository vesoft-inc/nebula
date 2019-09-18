/* Copyright (c) 2018 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef META_GETPARTSALLOCPROCESSOR_H_
#define META_GETPARTSALLOCPROCESSOR_H_

#include "meta/processors/BaseProcessor.h"

namespace nebula {
namespace meta {

class GetPartsAllocProcessor : public BaseProcessor<cpp2::GetPartsAllocResp> {
public:
    static GetPartsAllocProcessor* instance(kvstore::KVStore* kvstore) {
        return new GetPartsAllocProcessor(kvstore);
    }

    void process(const cpp2::GetPartsAllocReq& req);

private:
    explicit GetPartsAllocProcessor(kvstore::KVStore* kvstore)
            : BaseProcessor<cpp2::GetPartsAllocResp>(kvstore) {}
};

}  // namespace meta
}  // namespace nebula

#endif  // META_GETPARTSALLOCPROCESSOR_H_
