/* Copyright (c) 2019 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */


#include "meta/processors/admin/LeaderBalanceProcessor.h"
#include "meta/processors/admin/Balancer.h"

namespace nebula {
namespace meta {

void LeaderBalanceProcessor::process(const cpp2::LeaderBalanceReq&) {
    auto ret = Balancer::instance(kvstore_)->leaderBalance();
    handleErrorCode(ret);
    onFinished();
}


}  // namespace meta
}  // namespace nebula

