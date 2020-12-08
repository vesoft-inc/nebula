/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */
#include <folly/container/Enumerate.h>

#include "codec/RowWriterV2.h"
#include "common/clients/storage/InternalStorageClient.h"
#include "storage/CommonUtils.h"
#include "storage/StorageFlags.h"
#include "storage/mutate/AddEdgesProcessor.h"
#include "storage/transaction/TransactionManager.h"
#include "storage/transaction/TransactionUtils.h"
#include "utils/NebulaKeyUtils.h"

#define TOSS_INTRUSIVE_TEST 0
#if TOSS_INTRUSIVE_TEST
constexpr int kCommitInEdgeFail = 19;
constexpr int kCommitOutEdgeFail = 29;
constexpr int kRemoveLockFail = 37;
#endif

namespace nebula {
namespace storage {

/*
 * edgeKey : thrift data structure
 * rawKey  : NebulaKeyUtils::edgeKey
 * lockKey : rawKey + lock suffix
 * */
TransactionManager::TransactionManager(StorageEnv* env) : env_(env) {
    exec_ = std::make_shared<folly::IOThreadPoolExecutor>(10);
    interClient_ = std::make_unique<storage::InternalStorageClient>(
                            exec_,
                            env_->metaClient_);
}

/*
 * multi edges have same local partition and same remote partition
 * will process as a batch
 * *Important**Important**Important*
 * normally, ver will be 0
 * */
folly::Future<cpp2::ErrorCode> TransactionManager::addSamePartEdges(
    size_t vIdLen,
    GraphSpaceID spaceId,
    PartitionID localPart,
    PartitionID remotePart,
    std::vector<KV>& localEdges,
    AddEdgesProcessor* processor,
    folly::Optional<GetBatchFunc> optBatchGetter) {
    int64_t txnId = TransactionUtils::getSnowFlakeUUID();
    if (FLAGS_trace_toss) {
        for (auto& kv : localEdges) {
            auto srcId = NebulaKeyUtils::getSrcId(vIdLen, kv.first);
            auto dstId = NebulaKeyUtils::getDstId(vIdLen, kv.first);
            LOG(INFO) << "begin txn hexSrcDst=" << folly::hexlify(srcId)
                << folly::hexlify(dstId) << ", txnId=" << txnId;
        }
    }
    // steps 1: lock edges in memory
    bool setMemoryLock = true;
    std::for_each(localEdges.begin(), localEdges.end(), [&](auto& kv){
        auto keyWoVer = NebulaKeyUtils::keyWithNoVersion(kv.first).str();
        if (!memLock_.insert(std::make_pair(keyWoVer, txnId)).second) {
            setMemoryLock = false;
        }
    });

    auto cleanup = [=]{
        for (auto& kv : localEdges) {
            auto keyWoVer = NebulaKeyUtils::keyWithNoVersion(kv.first).str();
            auto cit = memLock_.find(keyWoVer);
            if (cit != memLock_.end() && cit->second == txnId) {
                memLock_.erase(keyWoVer);
            }
        }
    };

    if (!setMemoryLock) {
        LOG(ERROR) << "set memory lock failed, txnId=" << txnId;
        cleanup();
        return folly::makeFuture(cpp2::ErrorCode::E_MUTATE_EDGE_CONFLICT);
    } else {
        LOG_IF(INFO, FLAGS_trace_toss) << "set memory lock succeeded, txnId=" << txnId;
    }

    // steps 2: batch commit persist locks
    std::string batch;
    std::vector<KV> lockData = localEdges;
    // insert don't have BatchGetter
    if (!optBatchGetter) {
        // insert don't have batch Getter
        auto addEdgeErrorCode = cpp2::ErrorCode::SUCCEEDED;
        std::transform(lockData.begin(), lockData.end(), lockData.begin(), [&](auto& kv) {
            if (processor) {
                std::vector<KV> data{std::make_pair(kv.first, kv.second)};
                auto optVal = processor->addEdges(localPart, data);
                if (optVal) {
                    return std::make_pair(NebulaKeyUtils::toLockKey(kv.first), *optVal);
                } else {
                    addEdgeErrorCode = cpp2::ErrorCode::E_ATOMIC_OP_FAILED;
                    return std::make_pair(NebulaKeyUtils::toLockKey(kv.first), std::string(""));
                }
            } else {
                std::vector<KV> data{std::make_pair(kv.first, kv.second)};
                return std::make_pair(NebulaKeyUtils::toLockKey(kv.first),
                                      encodeBatch(std::move(data)));
            }
        });
        if (addEdgeErrorCode != cpp2::ErrorCode::SUCCEEDED) {
            cleanup();
            return addEdgeErrorCode;
        }
        auto lockDataSink = lockData;
        batch = encodeBatch(std::move(lockDataSink));
    } else {   // only update should enter here
        lockData.back().first = NebulaKeyUtils::toLockKey(localEdges.back().first);
        auto optBatch = (*optBatchGetter)();
        if (!optBatch) {
            cleanup();
            return cpp2::ErrorCode::E_ATOMIC_OP_FAILED;
        }
        batch = *optBatch;
        auto decodeKV = kvstore::decodeBatchValue(batch);
        localEdges.back().second = decodeKV.back().second.second.str();
        lockData.back().second = batch;
    }

    auto c = folly::makePromiseContract<cpp2::ErrorCode>();
    commitBatch(spaceId, localPart, batch)
        .via(exec_.get())
        .thenTry([=, p = std::move(c.first)](auto&& t) mutable {
            auto code = cpp2::ErrorCode::SUCCEEDED;
            if (!t.hasValue()) {
                LOG(INFO) << "commitBatch throw ex=" << t.exception()
                    << ", txnId=" << txnId;
                code = cpp2::ErrorCode::E_UNKNOWN;
            } else if (t.value() != kvstore::ResultCode::SUCCEEDED) {
                code = CommonUtils::to(t.value());
            }
            if (code != cpp2::ErrorCode::SUCCEEDED) {
                LOG(INFO) << folly::sformat("commitBatch for ({},{},{}) failed, code={}, txnId={}",
                                            spaceId,
                                            localPart,
                                            remotePart,
                                            static_cast<int32_t>(code),
                                            txnId);
                p.setValue(code);
                return;
            }

            std::vector<KV> remoteEdges = localEdges;
            std::transform(
                remoteEdges.begin(), remoteEdges.end(), remoteEdges.begin(), [=](auto& kv) {
                    auto key = TransactionUtils::reverseRawKey(vIdLen, remotePart, kv.first);
                    return std::make_pair(std::move(key), kv.second);
                });
            auto remoteBatch = encodeBatch(std::move(remoteEdges));

            // steps 3: multi put remote edges
            LOG_IF(INFO, FLAGS_trace_toss) << "begin forwardTransaction, txnId=" << txnId;
            interClient_->forwardTransaction(txnId, spaceId, remotePart, std::move(remoteBatch))
                .via(exec_.get())
                .thenTry([=, p = std::move(p)](auto&& _t) mutable {
                    auto _code = extractErrorCode(_t);
                    LOG_IF(INFO, FLAGS_trace_toss) << folly::sformat(
                        "end forwardTransaction: txnId={}, spaceId={}, partId={}, code={}",
                        txnId,
                        spaceId,
                        remotePart,
                        static_cast<int32_t>(_code));
                    if (_code != cpp2::ErrorCode::SUCCEEDED) {
                        p.setValue(_code);
                        return;
                    }

                    if (FLAGS_trace_toss) {
                        for (auto& kv : localEdges) {
                            LOG(INFO) << "key=" << folly::hexlify(kv.first) << ", txnId=" << txnId;
                        }
                    }

                    // steps 4 & 5: multi put local edges & multi remove persist locks
                    kvstore::BatchHolder bat;
                    for (auto& lock : lockData) {
                        bat.remove(NebulaKeyUtils::toLockKey(lock.first));
                        LOG_IF(INFO, FLAGS_trace_toss)
                            << "before bat put key=" << folly::hexlify(lock.first)
                            << ", txnId=" << txnId;
                        auto operations = kvstore::decodeBatchValue(lock.second);
                        for (auto& op : operations) {
                            auto opType = op.first;
                            auto& kv = op.second;
                            switch (opType) {
                                case kvstore::BatchLogType::OP_BATCH_PUT:
                                    bat.put(kv.first.str(), kv.second.str());
                                    break;
                                case kvstore::BatchLogType::OP_BATCH_REMOVE:
                                    bat.remove(kv.first.str());
                                    break;
                                default:
                                    LOG(ERROR) << "unexpected opType: " << static_cast<int>(opType);
                            }
                        }
                    }
                    auto _batch = kvstore::encodeBatchValue(bat.getBatch());
                    commitBatch(spaceId, localPart, _batch)
                        .via(exec_.get())
                        .thenValue([=, p = std::move(p)](auto&& rc) mutable {
                            auto commitBatchCode = CommonUtils::to(rc);
                            LOG_IF(INFO, FLAGS_trace_toss) << "txnId=" << txnId
                                << " finished, code=" << static_cast<int32_t>(commitBatchCode);
                            p.setValue(commitBatchCode);
                        });
                });
        })
        .ensure([=]() { cleanup(); });
    return std::move(c.second).via(exec_.get());
}

folly::Future<cpp2::ErrorCode> TransactionManager::updateEdgeAtomic(size_t vIdLen,
                                                                    GraphSpaceID spaceId,
                                                                    const std::string& rawKey,
                                                                    GetBatchFunc batchGetter) {
    auto localPart = NebulaKeyUtils::getPart(rawKey);
    auto dstId = NebulaKeyUtils::getDstId(vIdLen, rawKey).str();
    auto stRemotePart = env_->metaClient_->partId(spaceId, dstId);
    if (!stRemotePart.ok()) {
        return folly::makeFuture(CommonUtils::to(stRemotePart.status()));
    }
    auto remotePart = stRemotePart.value();
    auto remoteKey = TransactionUtils::reverseRawKey(spaceId, remotePart, rawKey);

    // auto decodeKV = kvstore::decodeBatchValue(localBatch);
    // auto remoteVal = decodeKV.back().second.second.str();

    std::vector<KV> data{std::make_pair(remoteKey, "")};
    // folly::Optional<std::string> optBatch = localBatch;
    return addSamePartEdges(vIdLen, spaceId, localPart, remotePart, data, nullptr, batchGetter);
}

folly::Future<cpp2::ErrorCode> TransactionManager::resumeTransaction(size_t vIdLen,
                                                                     GraphSpaceID spaceId,
                                                                     std::string lockKey,
                                                                     std::string lockVal,
                                                                     ResumedResult result) {
    // 1st, set memory lock
    auto localKey = NebulaKeyUtils::toEdgeKey(lockKey);
    CHECK(NebulaKeyUtils::isEdge(vIdLen, localKey));
    int64_t ver = NebulaKeyUtils::getVersion(vIdLen, localKey);
    auto keyWoVer = NebulaKeyUtils::keyWithNoVersion(localKey).str();
    if (!memLock_.insert(std::make_pair(keyWoVer, ver)).second) {
        return folly::makeFuture(cpp2::ErrorCode::E_MUTATE_EDGE_CONFLICT);
    }

    // 2nd, get values from remote in-edge
    auto spPromiseVal = std::make_shared<cpp2::ErrorCode>(cpp2::ErrorCode::SUCCEEDED);
    auto c = folly::makePromiseContract<cpp2::ErrorCode>();
    auto remoteKey = reverseKey(vIdLen, spaceId, localKey, ver);
    interClient_->getValue(vIdLen, spaceId, remoteKey)
        .via(exec_.get())
        .thenValue([=, p = std::move(c.first)](auto&& errOrVal) mutable {
            if (!nebula::ok(errOrVal)) {
                *spPromiseVal = nebula::error(errOrVal);
                return;
            }
            auto lockedPtr = result->wlock();
            if (!lockedPtr) {
                *spPromiseVal = cpp2::ErrorCode::E_MUTATE_EDGE_CONFLICT;
                return;
            }
            auto& kv = *lockedPtr;
            if (FLAGS_enable_multi_versions) {
                auto verPrev =
                    NebulaKeyUtils::isEdge(vIdLen, kv.first)
                        ? folly::Endian::big(NebulaKeyUtils::getVersion(vIdLen, kv.first))
                        :
                                    std::numeric_limits<int64_t>::max();
                auto verCurr = folly::Endian::big(ver);
                LOG_IF(INFO, FLAGS_trace_toss)
                    << "compare ver=" << ver << ", verCurr=" << verCurr << ", verPrev=" << verPrev;
                if (verCurr > verPrev) {
                    *spPromiseVal = cpp2::ErrorCode::E_OUTDATED_LOCK;
                    return;
                }
            }
            kv.first = localKey;
            kv.second = nebula::value(errOrVal);
            // 3rd, commit local key(indexes)
            auto localPart = NebulaKeyUtils::getPart(localKey);
            /*
             * if mvcc enabled, get value will get exactly the same ver in-edge against lock
             *                  which means we can trust the val of lock as the out-edge
             *              else, don't trust lock.
             * */
            if (FLAGS_enable_multi_versions) {
                commitBatch(spaceId, localPart, lockVal)
                    .via(exec_.get())
                    .thenValue([=](auto&& rc) { *spPromiseVal = CommonUtils::to(rc); })
                    .thenError([=](auto&&) { *spPromiseVal = cpp2::ErrorCode::E_UNKNOWN; });
            } else {
                auto key = kv.first;
                auto val = kv.second;
                commitEdgeOut(vIdLen, spaceId, localPart, std::move(kv.first), std::move(kv.second))
                    .via(exec_.get())
                    .thenValue([=](auto&& rc) { *spPromiseVal = CommonUtils::to(rc); })
                    .thenError([=](auto&&) { *spPromiseVal = cpp2::ErrorCode::E_UNKNOWN; });
            }
        })
        .thenValue([=](auto&&) {
            // 4th, remove persist lock
            if (*spPromiseVal == cpp2::ErrorCode::SUCCEEDED ||
                *spPromiseVal == cpp2::ErrorCode::E_OUTDATED_LOCK) {
                erasePersistLock(vIdLen, spaceId, localKey)
                    .via(exec_.get())
                    .thenValue([=](auto&& code) { *spPromiseVal = code; })
                    .thenError([=](auto&&) { *spPromiseVal = cpp2::ErrorCode::E_UNKNOWN; });
            }
        })
        .thenError([=](auto&& ex) {
            LOG(ERROR) << ex.what();
            *spPromiseVal = cpp2::ErrorCode::E_UNKNOWN;
        })
        .ensure([=, p = std::move(c.first)]() mutable {
            eraseMemoryLock(localKey, ver);
            p.setValue(*spPromiseVal);
        });
    return std::move(c.second).via(exec_.get());
}

// combine multi put and remove in a batch
// this may sometimes reduce some raft operation
folly::SemiFuture<kvstore::ResultCode> TransactionManager::commitBatch(GraphSpaceID spaceId,
                                                                       PartitionID partId,
                                                                       std::string& batch) {
    auto c = folly::makePromiseContract<kvstore::ResultCode>();
    env_->kvstore_->asyncAppendBatch(
        spaceId, partId, batch, [pro = std::move(c.first)](kvstore::ResultCode rc) mutable {
            pro.setValue(rc);
        });
    return std::move(c.second);
}

folly::SemiFuture<kvstore::ResultCode> TransactionManager::writeLock(size_t vIdLen,
                                                                     GraphSpaceID spaceId,
                                                                     PartitionID partId,
                                                                     folly::StringPiece rawKey,
                                                                     folly::StringPiece val) {
    UNUSED(vIdLen);
    std::string lockKey = NebulaKeyUtils::toLockKey(rawKey);
    std::vector<kvstore::KV> data{{std::move(lockKey), val.str()}};
    auto c = folly::makePromiseContract<kvstore::ResultCode>();
    env_->kvstore_->asyncMultiPut(
        spaceId,
        partId,
        std::move(data),
        [pro = std::move(c.first)](kvstore::ResultCode code) mutable { pro.setValue(code); });
    return std::move(c.second);
}

/*
 * 1. use rawKey without version as in-memory lock key
 * */
folly::SemiFuture<kvstore::ResultCode> TransactionManager::commitEdgeOut(size_t vIdLen,
                                                                         GraphSpaceID spaceId,
                                                                         PartitionID partId,
                                                                         std::string&& key,
                                                                         std::string&& props) {
#if TOSS_INTRUSIVE_TEST
    static bool throwNextRequest = false;
    auto rank = NebulaKeyUtils::getRank(vIdLen, key);
    int64_t ver = NebulaKeyUtils::getVersion(vIdLen, key);
    LOG(INFO) << folly::sformat(
        "rank={}, partId={}, throwNextRequest={}, ver={}", rank, partId, throwNextRequest, ver);
    if (rank == kCommitOutEdgeFail) {
        throwNextRequest = true;
        throw std::runtime_error("set throwNextRequest = true");
    } else if (throwNextRequest) {
        throwNextRequest = false;
        throw std::runtime_error(folly::sformat("TOSS_INTRUSIVE_TEST: err={}", kCommitOutEdgeFail));
    }
#endif
    UNUSED(vIdLen);
    std::vector<std::shared_ptr<nebula::meta::cpp2::IndexItem>> indexes;
    auto idxRet = env_->indexMan_->getEdgeIndexes(spaceId);
    if (idxRet.ok()) {
        indexes = std::move(idxRet).value();
    }
    if (!indexes.empty()) {
        std::vector<kvstore::KV> data;
        data.reserve(32);
        data.emplace_back(std::move(key), std::move(props));

        auto c = folly::makePromiseContract<kvstore::ResultCode>();

        auto atomic = [partId, edges = std::move(data), this]() -> folly::Optional<std::string> {
            auto* processor = AddEdgesProcessor::instance(env_, nullptr);
            return processor->addEdges(partId, edges);
        };

        auto cb = [pro = std::move(c.first)](kvstore::ResultCode rc) mutable {
            pro.setValue(rc);
        };

        env_->kvstore_->asyncAtomicOp(spaceId, partId, atomic, std::move(cb));
        return std::move(c.second);
    }
    return commitEdge(spaceId, partId, std::move(key), std::move(props));
}

folly::SemiFuture<kvstore::ResultCode> TransactionManager::multiPut(
    GraphSpaceID spaceId,
    PartitionID partId,
    std::vector<kvstore::KV>&& data) {
    auto c = folly::makePromiseContract<kvstore::ResultCode>();
    env_->kvstore_->asyncMultiPut(
        spaceId, partId, std::move(data), [p = std::move(c.first)](kvstore::ResultCode rc) mutable {
            p.setValue(rc);
        });
    return std::move(c.second);
}

folly::SemiFuture<kvstore::ResultCode> TransactionManager::commitEdge(GraphSpaceID spaceId,
                                                                      PartitionID partId,
                                                                      std::string&& key,
                                                                      std::string&& props) {
    std::vector<kvstore::KV> data;
    data.emplace_back(std::move(key), std::move(props));

    auto c = folly::makePromiseContract<kvstore::ResultCode>();
    env_->kvstore_->asyncMultiPut(
        spaceId,
        partId,
        std::move(data),
        [pro = std::move(c.first)](kvstore::ResultCode rc) mutable {
            pro.setValue(rc);
        });
    return std::move(c.second);
}

folly::SemiFuture<cpp2::ErrorCode> TransactionManager::erasePersistLock(size_t vIdLen,
                                                                        GraphSpaceID spaceId,
                                                                        std::string rawKey) {
    LOG_IF(INFO, FLAGS_trace_toss)
        << "going to erasePersistLock " << TransactionUtils::dumpEdge(vIdLen, rawKey, "");
    PartitionID partId = NebulaKeyUtils::getPart(rawKey);
    auto lockKey = NebulaKeyUtils::toLockKey(rawKey);
#if TOSS_INTRUSIVE_TEST
    static bool throwNextRequest = false;
    auto rank = NebulaKeyUtils::getRank(vIdLen, rawKey);
    if (throwNextRequest) {
        throwNextRequest = false;
        throw std::runtime_error(folly::sformat("TOSS_INTRUSIVE_TEST: err={}", kRemoveLockFail));
    } else if (rank == kRemoveLockFail) {
        throwNextRequest = true;
        throw std::runtime_error("erasePersistLock() set throwNextRequest = true");
    }
#endif
    auto contract = folly::makePromiseContract<cpp2::ErrorCode>();
    env_->kvstore_->asyncRemove(
        spaceId,
        partId,
        lockKey,
        [pro = std::move(contract.first)](kvstore::ResultCode code) mutable {
            LOG_IF(INFO, FLAGS_trace_toss) << "asyncRemove code=" << static_cast<int>(code);
            pro.setValue(CommonUtils::to(code));
        });
    return std::move(contract.second);
}

void TransactionManager::eraseMemoryLock(const std::string& rawKey, int64_t ver) {
    // auto lockKey = TransactionUtils::lockKeyWithoutVer(rawKey);
    auto lockKey = NebulaKeyUtils::keyWithNoVersion(rawKey).str();
    auto cit = memLock_.find(lockKey);
    if (cit != memLock_.end() && cit->second == ver) {
        memLock_.erase(lockKey);
    }
}

meta::cpp2::IsolationLevel TransactionManager::getSpaceIsolationLvel(GraphSpaceID spaceId) {
    auto ret = env_->metaClient_->getIsolationLevel(spaceId);
    if (!ret.ok()) {
        return meta::cpp2::IsolationLevel::DEFAULT;
    }
    return ret.value();
}

std::string TransactionManager::reverseKey(size_t vIdLen,
                                           GraphSpaceID spaceId,
                                           std::string& rawKey,
                                           int64_t ver) {
    VertexIDSlice src = NebulaKeyUtils::getSrcId(vIdLen, rawKey);
    VertexIDSlice dst = NebulaKeyUtils::getDstId(vIdLen, rawKey);
    EdgeType type = NebulaKeyUtils::getEdgeType(vIdLen, rawKey);
    EdgeRanking rank = NebulaKeyUtils::getRank(vIdLen, rawKey);

    auto inPartIdStatus = env_->metaClient_->partId(spaceId, src.str());
    CHECK(inPartIdStatus.ok());
    auto inPartId = inPartIdStatus.value();

    return NebulaKeyUtils::edgeKey(vIdLen, inPartId, dst.str(), 0 - type, rank, src.str(), ver);
}

cpp2::ErrorCode TransactionManager::extractErrorCode(folly::Try<StatusOr<cpp2::ExecResponse>>& t) {
    if (!t.hasValue()) {
        LOG_IF(INFO, FLAGS_trace_toss) << "extractErrorCode() !t.hasValue()";
        return cpp2::ErrorCode::E_UNKNOWN;
    }

    if (!t.value().ok()) {
        LOG_IF(INFO, FLAGS_trace_toss) << "extractErrorCode() !t.value().ok()";
        return CommonUtils::to(t.value().status());
    }

    auto& execResp = t.value().value();
    auto& respCmn = execResp.get_result();
    for (auto& partResult : respCmn.get_failed_parts()) {
        return partResult.get_code();
    }
    return cpp2::ErrorCode::SUCCEEDED;
}

std::string TransactionManager::encodeBatch(std::vector<KV>&& data) {
    kvstore::BatchHolder bat;
    for (auto& kv : data) {
        bat.put(std::move(kv.first), std::move(kv.second));
    }
    return encodeBatchValue(bat.getBatch());
}

}   // namespace storage
}   // namespace nebula
