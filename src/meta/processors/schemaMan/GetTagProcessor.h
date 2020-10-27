/* Copyright (c) 2018 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef META_GETTAGPROCESSOR_H_
#define META_GETTAGPROCESSOR_H_

#include "meta/processors/BaseProcessor.h"

namespace nebula {
namespace meta {

class GetTagProcessor : public BaseProcessor<cpp2::GetTagResp> {
public:
    static GetTagProcessor* instance(kvstore::KVStore* kvstore) {
        return new GetTagProcessor(kvstore);
    }

    void process(const cpp2::GetTagReq& req);

private:
    explicit GetTagProcessor(kvstore::KVStore* kvstore)
        : BaseProcessor<cpp2::GetTagResp>(kvstore) {}
};

}  // namespace meta
}  // namespace nebula
#endif  // META_GETTAGPROCESSOR_H_
