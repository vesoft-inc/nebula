/* Copyright (c) 2018 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef META_DROPEDGEPROCESSOR_H_
#define META_DROPEDGEPROCESSOR_H_

#include "meta/processors/BaseProcessor.h"

namespace nebula {
namespace meta {

class DropEdgeProcessor : public BaseProcessor<cpp2::ExecResp> {
public:
    static DropEdgeProcessor* instance(kvstore::KVStore* kvstore) {
        return new DropEdgeProcessor(kvstore);
    }

    void process(const cpp2::DropEdgeReq& req);

private:
    explicit DropEdgeProcessor(kvstore::KVStore* kvstore)
        : BaseProcessor<cpp2::ExecResp>(kvstore) {}

    ErrorOr<nebula::cpp2::ErrorCode, std::vector<std::string>>
    getEdgeKeys(GraphSpaceID id, EdgeType edgeType);
};

}  // namespace meta
}  // namespace nebula

#endif  // META_DROPEDGEPROCESSOR_H_

