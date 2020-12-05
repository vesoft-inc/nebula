/* Copyright (c) 2019 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef META_BALANCEPROCESSOR_H_
#define META_BALANCEPROCESSOR_H_

#include <gtest/gtest_prod.h>
#include "meta/processors/BaseProcessor.h"
#include "meta/ActiveHostsMan.h"

namespace nebula {
namespace meta {

class BalanceProcessor : public BaseProcessor<cpp2::BalanceResp> {
public:
    static BalanceProcessor* instance(kvstore::KVStore* kvstore) {
        return new BalanceProcessor(kvstore);
    }

    void process(const cpp2::BalanceReq& req);

private:
    explicit BalanceProcessor(kvstore::KVStore* kvstore)
        : BaseProcessor<cpp2::BalanceResp>(kvstore) {}
};

}  // namespace meta
}  // namespace nebula

#endif  // META_BALANCEPROCESSOR_H_
