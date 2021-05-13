/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "storage/transaction/TransactionProcessor.h"
#include "storage/transaction/TransactionManager.h"
#include "storage/transaction/TransactionUtils.h"

namespace nebula {
namespace storage {

ProcessorCounters kForwardTranxCounters;

void InterTxnProcessor::process(const cpp2::InternalTxnRequest& req) {
    int64_t txnId = req.get_txn_id();
    auto spaceId = req.get_space_id();
    auto partId = req.get_part_id();

    LOG_IF(INFO, FLAGS_trace_toss) << "process req, txnId=" << txnId
                                   << ", spaceId=" << spaceId << ", partId=" << partId;
    auto data = req.get_data()[req.get_position()].back();

    env_->txnMan_->commitBatch(spaceId, partId, std::move(data))
        .via(env_->txnMan_->getExecutor())
        .thenValue([=](nebula::cpp2::ErrorCode rc) {
            LOG_IF(INFO, FLAGS_trace_toss) << "txnId="
                << txnId << " commitBatch ret rc=" << static_cast<int32_t>(rc);
            auto code = rc;
            if (code == nebula::cpp2::ErrorCode::E_LEADER_CHANGED) {
                handleLeaderChanged(spaceId, partId);
            } else if (code != nebula::cpp2::ErrorCode::SUCCEEDED) {
                pushResultCode(code, partId);
            }
            for (auto& p : codes_) {
                if (p.leader_ref().has_value()) {
                    LOG(INFO) << "txnId=" << txnId << ", part=" << *p.part_id_ref()
                              << ", code=" << static_cast<int32_t>(*p.code_ref())
                              << ", leader=" << *p.leader_ref();
                } else {
                    LOG(INFO) << "txnId=" << txnId << ", part=" << *p.part_id_ref()
                              << ", code=" << static_cast<int32_t>(*p.code_ref());
                }
            }
            onFinished();
        })
        .thenError([&](auto&& ex) {
            LOG(ERROR) << "txnId=" << txnId << ", " << ex.what();
            pushResultCode(nebula::cpp2::ErrorCode::E_UNKNOWN, partId);
            onFinished();
        });
}

}   // namespace storage
}   // namespace nebula
