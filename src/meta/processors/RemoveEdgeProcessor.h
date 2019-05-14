/* Copyright (c) 2018 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef META_REMOVEEDGEPROCESSOR_H_
#define META_REMOVEEDGEPROCESSOR_H_

#include "meta/processors/BaseProcessor.h"

namespace nebula {
namespace meta {

class RemoveEdgeProcessor : public BaseProcessor<cpp2::ExecResp> {
public:
    static RemoveEdgeProcessor* instance(kvstore::KVStore* kvstore) {
        return new RemoveEdgeProcessor(kvstore);
    }

    void process(const cpp2::RemoveEdgeReq& req);

private:
    explicit RemoveEdgeProcessor(kvstore::KVStore* kvstore)
            : BaseProcessor<cpp2::ExecResp>(kvstore) {}

    StatusOr<std::vector<std::string>> getEdgeKeys(GraphSpaceID id, const std::string& edgeName);
};

}  // namespace meta
}  // namespace nebula

#endif  // META_REMOVEEDGEPROCESSOR_H_
