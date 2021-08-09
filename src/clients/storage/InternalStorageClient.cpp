/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "common/clients/storage/InternalStorageClient.h"
#include "common/base/Base.h"

namespace nebula {
namespace storage {

template <typename T>
nebula::cpp2::ErrorCode extractErrorCode(T& tryResp) {
    if (!tryResp.hasValue()) {
        LOG(ERROR) << tryResp.exception().what();
        return nebula::cpp2::ErrorCode::E_RPC_FAILURE;
    }

    auto& stResp = tryResp.value();
    if (!stResp.ok()) {
        switch (stResp.status().code()) {
            case Status::Code::kLeaderChanged:
                return nebula::cpp2::ErrorCode::E_LEADER_CHANGED;
            default:
                LOG(ERROR) << "not impl error transform: code="
                           << static_cast<int32_t>(stResp.status().code());
        }
        return nebula::cpp2::ErrorCode::E_UNKNOWN;
    }

    auto& failedPart = stResp.value().get_result().get_failed_parts();
    for (auto& p : failedPart) {
        return p.code;
    }
    return nebula::cpp2::ErrorCode::SUCCEEDED;
}

StatusOr<HostAddr> InternalStorageClient::getFuzzyLeader(GraphSpaceID spaceId,
                                                         PartitionID partId) const {
    return getLeader(spaceId, partId);
}

folly::SemiFuture<nebula::cpp2::ErrorCode>
InternalStorageClient::forwardTransaction(
        int64_t txnId,
        GraphSpaceID spaceId,
        PartitionID partId,
        std::string&& data,
        folly::EventBase* evb) {
    auto c = folly::makePromiseContract<nebula::cpp2::ErrorCode>();
    forwardTransactionImpl(txnId,
        spaceId, partId, std::move(data), std::move(c.first), evb);
    return std::move(c.second);
}

void InternalStorageClient::forwardTransactionImpl(int64_t txnId,
                                                   GraphSpaceID spaceId,
                                                   PartitionID partId,
                                                   std::string&& data,
                                                   folly::Promise<nebula::cpp2::ErrorCode> p,
                                                   folly::EventBase* evb) {
    VLOG(1) << "forwardTransactionImpl txnId=" << txnId;
    auto statusOrLeader = getFuzzyLeader(spaceId, partId);
    if (!statusOrLeader.ok()) {
        p.setValue(nebula::cpp2::ErrorCode::E_SPACE_NOT_FOUND);
        return;
    }
    HostAddr& dest = statusOrLeader.value();
    dest.port += kInternalPortOffset;

    cpp2::InternalTxnRequest interReq;
    interReq.set_txn_id(txnId);
    interReq.set_space_id(spaceId);
    interReq.set_part_id(partId);
    interReq.set_position(1);
    (*interReq.data_ref()).resize(2);
    (*interReq.data_ref()).back().emplace_back(data);
    getResponse(
        evb,
        std::make_pair(dest, interReq),
        [](cpp2::InternalStorageServiceAsyncClient* client, const cpp2::InternalTxnRequest& r) {
            return client->future_forwardTransaction(r);
        })
        .thenTry([=, p = std::move(p)](auto&& t) mutable {
            auto code = extractErrorCode(t);
            if (code == nebula::cpp2::ErrorCode::E_LEADER_CHANGED) {
                std::this_thread::sleep_for(std::chrono::milliseconds(500));
                return forwardTransactionImpl(
                    txnId, spaceId, partId, std::move(data), std::move(p), evb);
            } else {
                p.setValue(code);
            }
        });
}

folly::SemiFuture<ErrOrVal> InternalStorageClient::getValue(size_t vIdLen,
                                                            GraphSpaceID spaceId,
                                                            folly::StringPiece key,
                                                            folly::EventBase* evb) {
    auto srcVid = key.subpiece(sizeof(PartitionID), vIdLen);
    auto partId = metaClient_->partId(spaceId, srcVid.str());

    auto c = folly::makePromiseContract<ErrOrVal>();
    getValueImpl(spaceId, partId, key.str(), std::move(c.first), evb);
    return std::move(c.second);
}

void InternalStorageClient::getValueImpl(GraphSpaceID spaceId,
                                         PartitionID partId,
                                         std::string&& key,
                                         folly::Promise<ErrOrVal> p,
                                         folly::EventBase* evb) {
    std::pair<HostAddr, cpp2::GetValueRequest> req;
    auto stLeaderHost = getFuzzyLeader(spaceId, partId);
    if (!stLeaderHost.ok()) {
        if (stLeaderHost.status().toString().find("partid")) {
            p.setValue(nebula::cpp2::ErrorCode::E_PART_NOT_FOUND);
        } else {
            p.setValue(nebula::cpp2::ErrorCode::E_SPACE_NOT_FOUND);
        }
        return;
    }
    req.first = stLeaderHost.value();
    req.first.port += kInternalPortOffset;

    req.second.set_space_id(spaceId);
    req.second.set_part_id(partId);
    req.second.set_key(key);

    auto remote = [](cpp2::InternalStorageServiceAsyncClient* client,
                     const cpp2::GetValueRequest& r) { return client->future_getValue(r); };

    auto cb = [=, p = std::move(p)](auto&& t) mutable {
        auto code = extractErrorCode(t);
        if (code == nebula::cpp2::ErrorCode::SUCCEEDED) {
            p.setValue(t.value().value().get_value());
        } else if (code == nebula::cpp2::ErrorCode::E_LEADER_CHANGED) {
            // retry directly may easily get same error
            std::this_thread::sleep_for(std::chrono::milliseconds(500));
            return getValueImpl(spaceId, partId, std::move(key), std::move(p), evb);
        } else {
            p.setValue(code);
        }
    };

    getResponse(evb, std::move(req), remote).thenTry(std::move(cb));
}

}   // namespace storage
}   // namespace nebula
