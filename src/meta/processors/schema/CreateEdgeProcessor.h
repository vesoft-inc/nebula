/* Copyright (c) 2018 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef META_CREATEEDGEPROCESSOR_H_
#define META_CREATEEDGEPROCESSOR_H_

#include "meta/processors/BaseProcessor.h"

namespace nebula {
namespace meta {

class CreateEdgeProcessor : public BaseProcessor<cpp2::ExecResp> {
public:
    static CreateEdgeProcessor* instance(kvstore::KVStore* kvstore) {
        return new CreateEdgeProcessor(kvstore);
    }

    void process(const cpp2::CreateEdgeReq& req);

private:
    explicit CreateEdgeProcessor(kvstore::KVStore* kvstore)
        : BaseProcessor<cpp2::ExecResp>(kvstore) {}
};

}  // namespace meta
}  // namespace nebula

#endif  // META_CREATEEDGEPROCESSOR_H_
