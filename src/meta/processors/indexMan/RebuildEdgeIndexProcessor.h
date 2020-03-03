/* Copyright (c) 2020 vesoft inc. All rights reserved.
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
    static RebuildEdgeIndexProcessor* instance(kvstore::KVStore* kvstore,
                                               AdminClient* adminClient) {
        return new RebuildEdgeIndexProcessor(kvstore, adminClient);
    }

    void process(const cpp2::RebuildIndexReq& req);

private:
    explicit RebuildEdgeIndexProcessor(kvstore::KVStore* kvstore,
                                       AdminClient* adminClient)
            : BaseProcessor<cpp2::ExecResp>(kvstore), adminClient_(adminClient) {}

private:
    AdminClient* adminClient_;
};

}  // namespace meta
}  // namespace nebula

#endif  // META_REBUILDEDGEINDEXPROCESSOR_H
