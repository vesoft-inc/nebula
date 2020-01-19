/* Copyright (c) 2019 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef META_REBUILDEDGEINDEXPROCESSOR_H
#define META_REBUILDEDGEINDEXPROCESSOR_H

#include "meta/processors/BaseProcessor.h"

namespace nebula {
namespace meta {

class RebuildEdgeIndexProcessor : public BaseProcessor<cpp2::ExecResp> {
public:
    static RebuildEdgeIndexProcessor* instance(kvstore::KVStore* kvstore) {
        return new RebuildEdgeIndexProcessor(kvstore);
    }

    void process(const cpp2::RebuildIndexReq& req);

private:
    explicit RebuildEdgeIndexProcessor(kvstore::KVStore* kvstore)
            : BaseProcessor<cpp2::ExecResp>(kvstore) {}
};

}  // namespace meta
}  // namespace nebula

#endif  // META_REBUILDEDGEINDEXPROCESSOR_H
