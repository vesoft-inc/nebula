/* Copyright (c) 2019 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef META_LEADERBALANCEPROCESSOR_H_
#define META_LEADERBALANCEPROCESSOR_H_

#include <gtest/gtest_prod.h>
#include "meta/processors/BaseProcessor.h"

namespace nebula {
namespace meta {

class LeaderDistProcessor : public BaseProcessor<cpp2::LeaderDistResp> {
public:
    static LeaderDistProcessor* instance(kvstore::KVStore* kvstore) {
        return new LeaderDistProcessor(kvstore);
    }

    void process(const cpp2::LeaderDistReq& req);

private:
    explicit LeaderDistProcessor(kvstore::KVStore* kvstore)
            : BaseProcessor<cpp2::LeaderDistResp>(kvstore) {}
};

}  // namespace meta
}  // namespace nebula

#endif  // META_LEADERBALANCEPROCESSOR_H_
