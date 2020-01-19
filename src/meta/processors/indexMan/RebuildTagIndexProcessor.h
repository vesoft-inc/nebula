/* Copyright (c) 2019 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef META_REBUILDTAGINDEXPROCESSOR_H
#define META_REBUILDTAGINDEXPROCESSOR_H

#include "meta/processors/BaseProcessor.h"

namespace nebula {
namespace meta {

class RebuildTagIndexProcessor : public BaseProcessor<cpp2::ExecResp> {
public:
    static RebuildTagIndexProcessor* instance(kvstore::KVStore* kvstore) {
        return new RebuildTagIndexProcessor(kvstore);
    }

    void process(const cpp2::RebuildIndexReq& req);

private:
    explicit RebuildTagIndexProcessor(kvstore::KVStore* kvstore)
            : BaseProcessor<cpp2::ExecResp>(kvstore) {}
};

}  // namespace meta
}  // namespace nebula

#endif  // META_REBUILDTAGINDEXPROCESSOR_H
