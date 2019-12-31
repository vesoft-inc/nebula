/* Copyright (c) 2019 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef META_BUILDTAGINDEXPROCESSOR_H
#define META_BUILDTAGINDEXPROCESSOR_H

#include "meta/processors/BaseProcessor.h"

namespace nebula {
namespace meta {

class BuildTagIndexProcessor : public BaseProcessor<cpp2::ExecResp> {
public:
    static BuildTagIndexProcessor* instance(kvstore::KVStore* kvstore) {
        return new BuildTagIndexProcessor(kvstore);
    }

    void process(const cpp2::BuildTagIndexReq& req);

private:
    explicit BuildTagIndexProcessor(kvstore::KVStore* kvstore)
            : BaseProcessor<cpp2::ExecResp>(kvstore) {}
};

}  // namespace meta
}  // namespace nebula

#endif  // META_BUILDTAGINDEXPROCESSOR_H
