/* Copyright (c) 2019 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef META_BUILDEDGEINDEXPROCESSOR_H
#define META_BUILDEDGEINDEXPROCESSOR_H

#include "meta/processors/BaseProcessor.h"

namespace nebula {
namespace meta {

class BuildEdgeIndexProcessor : public BaseProcessor<cpp2::ExecResp> {
public:
    static BuildEdgeIndexProcessor* instance(kvstore::KVStore* kvstore) {
        return new BuildEdgeIndexProcessor(kvstore);
    }

    void process(const cpp2::BuildEdgeIndexReq& req);

private:
    explicit BuildEdgeIndexProcessor(kvstore::KVStore* kvstore)
            : BaseProcessor<cpp2::ExecResp>(kvstore) {}
};

}  // namespace meta
}  // namespace nebula

#endif  // META_BUILDEDGEINDEXPROCESSOR_H
