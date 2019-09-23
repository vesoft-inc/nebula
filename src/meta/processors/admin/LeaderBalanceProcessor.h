/* Copyright (c) 2019 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef META_LEADERCOUNTPROCESSOR_H_
#define META_LEADERCOUNTPROCESSOR_H_

#include <gtest/gtest_prod.h>
#include "meta/processors/BaseProcessor.h"

namespace nebula {
namespace meta {

class LeaderBalanceProcessor : public BaseProcessor<cpp2::ExecResp> {
public:
    static LeaderBalanceProcessor* instance(kvstore::KVStore* kvstore) {
        return new LeaderBalanceProcessor(kvstore);
    }

    void process(const cpp2::LeaderBalanceReq& req);

private:
    explicit LeaderBalanceProcessor(kvstore::KVStore* kvstore)
            : BaseProcessor<cpp2::ExecResp>(kvstore) {}
};

}  // namespace meta
}  // namespace nebula

#endif  // META_LEADERCOUNTPROCESSOR_H_
