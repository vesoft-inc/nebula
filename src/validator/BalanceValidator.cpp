/* Copyright (c) 2020 vesoft inc. All rights reserved.
*
* This source code is licensed under Apache 2.0 License,
* attached with Common Clause Condition 1.0, found in the LICENSES directory.
*/

#include "common/base/Base.h"

#include "planner/plan/Admin.h"
#include "validator/BalanceValidator.h"

namespace nebula {
namespace graph {

Status BalanceValidator::toPlan() {
    PlanNode *current = nullptr;
    BalanceSentence *sentence = static_cast<BalanceSentence*>(sentence_);
    switch (sentence->subType()) {
    case BalanceSentence::SubType::kLeader:
        current = BalanceLeaders::make(qctx_, nullptr);
        break;
    case BalanceSentence::SubType::kData: {
        auto hosts = sentence->hostDel() == nullptr ? std::vector<HostAddr>()
                                                    : sentence->hostDel()->hosts();
        if (!hosts.empty()) {
            auto it = std::unique(hosts.begin(), hosts.end());
            if (it != hosts.end()) {
                return Status::SemanticError("Host have duplicated");
            }
        }
        current = Balance::make(qctx_,
                                nullptr,
                                std::move(hosts));
        break;
    }
    case BalanceSentence::SubType::kDataStop:
        current = StopBalance::make(qctx_, nullptr);
        break;
    case BalanceSentence::SubType::kDataReset:
        current = ResetBalance::make(qctx_, nullptr);
        break;
    case BalanceSentence::SubType::kShowBalancePlan:
        current = ShowBalance::make(qctx_, nullptr, sentence->balanceId());
        break;
    case BalanceSentence::SubType::kUnknown:
        // fallthrough
    default:
        DLOG(FATAL) << "Unknown balance kind " << sentence->kind();
        return Status::NotSupported("Unknown balance kind %d", static_cast<int>(sentence->kind()));
    }
    root_ = current;
    tail_ = root_;
    return Status::OK();
}

}  // namespace graph
}  // namespace nebula
