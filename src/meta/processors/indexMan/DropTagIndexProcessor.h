/* Copyright (c) 2019 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef META_DROPTAGINDEXPROCESSOR_H
#define META_DROPTAGINDEXPROCESSOR_H

#include "meta/processors/BaseProcessor.h"

namespace nebula {
namespace meta {

class DropTagIndexProcessor : public BaseProcessor<cpp2::ExecResp> {
public:
    static DropTagIndexProcessor* instance(kvstore::KVStore* kvstore) {
        return new DropTagIndexProcessor(kvstore);
    }

    void process(const cpp2::DropTagIndexReq& req);

private:
    explicit DropTagIndexProcessor(kvstore::KVStore* kvstore)
            : BaseProcessor<cpp2::ExecResp>(kvstore) {}
};

}  // namespace meta
}  // namespace nebula

#endif  // META_DROPTAGINDEXPROCESSOR_H

