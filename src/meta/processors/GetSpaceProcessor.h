/* Copyright (c) 2019 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef META_GETSPACEPROCESSOR_H_
#define META_GETSPACEPROCESSOR_H_

#include "meta/processors/BaseProcessor.h"

namespace nebula {
namespace meta {

class GetSpaceProcessor : public BaseProcessor<cpp2::GetSpaceResp> {
public:
    static GetSpaceProcessor* instance(kvstore::KVStore* kvstore) {
        return new GetSpaceProcessor(kvstore);
    }

    void process(const cpp2::GetSpaceReq& req);

private:
    explicit GetSpaceProcessor(kvstore::KVStore* kvstore)
            : BaseProcessor<cpp2::GetSpaceResp>(kvstore) {}
};

}  // namespace meta
}  // namespace nebula

#endif  // META_GETSPACEPROCESSOR_H_

