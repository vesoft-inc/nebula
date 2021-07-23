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
folly::Future<nebula::cpp2::ErrorCode>
TransactionManager::addSamePartEdges(size_t vIdLen,
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
        return folly::makeFuture(nebula::cpp2::ErrorCode::E_MUTATE_EDGE_CONFLICT);
    } else {
        LOG_IF(INFO, FLAGS_trace_toss) << "set memory lock succeeded, txnId=" << txnId;
    }

    // steps 2: batch commit persist locks
    std::string batch;
    std::vector<KV> lockData = localEdges;
    // insert don't have BatchGetter
    if (!optBatchGetter) {
        // insert don't have batch Getter
        auto addEdgeErrorCode = nebula::cpp2::ErrorCode::SUCCEEDED;
        std::transform(lockData.begin(), lockData.end(), lockData.begin(), [&](auto& kv) {
            if (processor) {
                processor->spaceId_ = spaceId;
                processor->spaceVidLen_ = vIdLen;
                std::vector<KV> data{std::make_pair(kv.first, kv.second)};
                auto optVal = processor->addEdges(localPart, data);
                if (nebula::ok(optVal)) {
                    return std::make_pair(NebulaKeyUtils::toLockKey(kv.first),
                                          nebula::value(optVal));
                } else {
                    addEdgeErrorCode = nebula::cpp2::ErrorCode::E_ATOMIC_OP_FAILED;
                    return std::make_pair(NebulaKeyUtils::toLockKey(kv.first), std::string(""));
                }
            } else {
                std::vector<KV> data{std::make_pair(kv.first, kv.second)};
                return std::make_pair(NebulaKeyUtils::toLockKey(kv.first),
                                      encodeBatch(std::move(data)));
            }
        });
        if (addEdgeErrorCode != nebula::cpp2::ErrorCode::SUCCEEDED) {
            cleanup();
            return addEdgeErrorCode;
        }
        auto lockDataSink = lockData;
        batch = encodeBatch(std::move(lockDataSink));
    } else {   // only update should enter here
        auto optBatch = (*optBatchGetter)();
        if (!optBatch) {
            cleanup();
            return nebula::cpp2::ErrorCode::E_ATOMIC_OP_FAILED;
        }
        lockData.back().first = NebulaKeyUtils::toLockKey(localEdges.back().first);
        batch = *optBatch;
        auto decodeKV = kvstore::decodeBatchValue(batch);
        localEdges.back().first = decodeKV.back().second.first.str();
        localEdges.back().second = decodeKV.back().second.second.str();

        lockData.back().first = localEdges.back().first;
        lockData.back().second = batch;
    }

    auto c = folly::makePromiseContract<nebula::cpp2::ErrorCode>();
    commitBatch(spaceId, localPart, std::move(batch))
        .via(exec_.get())
        .thenTry([=, p = std::move(c.first)](auto&& t) mutable {
            auto code = nebula::cpp2::ErrorCode::SUCCEEDED;
            if (!t.hasValue()) {
                LOG(INFO) << "commitBatch throw ex=" << t.exception()
                    << ", txnId=" << txnId;
                code = nebula::cpp2::ErrorCode::E_UNKNOWN;
            } else if (t.value() != nebula::cpp2::ErrorCode::SUCCEEDED) {
                code = t.value();
            }
            if (code != nebula::cpp2::ErrorCode::SUCCEEDED) {
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
                    auto _code = _t.hasValue() ? _t.value()
                        : nebula::cpp2::ErrorCode::E_UNKNOWN;
                    LOG_IF(INFO, FLAGS_trace_toss) << folly::sformat(
                        "end forwardTransaction: txnId={}, spaceId={}, partId={}, code={}",
                        txnId,
                        spaceId,
                        remotePart,
                        static_cast<int32_t>(_code));
                    if (_code != nebula::cpp2::ErrorCode::SUCCEEDED) {
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
                        LOG_IF(INFO, FLAGS_trace_toss)
                            << "remove lock, hex=" << folly::hexlify(lock.first)
                            << ", txnId=" << txnId;
                        bat.remove(std::move(lock.first));
                        auto operations = kvstore::decodeBatchValue(lock.second);
                        for (auto& op : operations) {
                            auto opType = op.first;
                            auto& kv = op.second;
                            LOG_IF(INFO, FLAGS_trace_toss)
                                        << "bat op=" << static_cast<int32_t>(opType)
                                        << ", hex=" << folly::hexlify(kv.first)
                                        << ", txnId=" << txnId;
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
                    commitBatch(spaceId, localPart, std::move(_batch))
                        .via(exec_.get())
                        .thenValue([=, p = std::move(p)](auto&& rc) mutable {
                            auto commitBatchCode = rc;
                            LOG_IF(INFO, FLAGS_trace_toss) << "txnId=" << txnId
                                << " finished, code=" << static_cast<int32_t>(commitBatchCode);
                            p.setValue(commitBatchCode);
                        });
                });
        })
        .ensure([=]() { cleanup(); });
    return std::move(c.second).via(exec_.get());
}

folly::Future<nebula::cpp2::ErrorCode>
TransactionManager::updateEdgeAtomic(size_t vIdLen,
                                     GraphSpaceID spaceId,
                                     PartitionID partId,
                                     const cpp2::EdgeKey& edgeKey,
                                     GetBatchFunc batchGetter) {
    auto remotePart = env_->metaClient_->partId(spaceId, (*edgeKey.dst_ref()).getStr());
    auto localKey = TransactionUtils::edgeKey(vIdLen, partId, edgeKey);

    std::vector<KV> data{std::make_pair(localKey, "")};
    return addSamePartEdges(vIdLen, spaceId, partId, remotePart, data, nullptr, batchGetter);
}

folly::Future<nebula::cpp2::ErrorCode>
TransactionManager::resumeTransaction(size_t vIdLen,
                                      GraphSpaceID spaceId,
                                      std::string lockKey,
                                      ResumedResult result) {
    LOG_IF(INFO, FLAGS_trace_toss) << "begin resume txn from lock=" << folly::hexlify(lockKey);
    // 1st, set memory lock
    auto localKey = NebulaKeyUtils::toEdgeKey(lockKey);
    CHECK(NebulaKeyUtils::isEdge(vIdLen, localKey));
    // int64_t ver = NebulaKeyUtils::getVersion(vIdLen, localKey);
    auto ver = NebulaKeyUtils::getLockVersion(lockKey);
    auto keyWoVer = NebulaKeyUtils::keyWithNoVersion(localKey).str();
    if (!memLock_.insert(std::make_pair(keyWoVer, ver)).second) {
        return folly::makeFuture(nebula::cpp2::ErrorCode::E_MUTATE_EDGE_CONFLICT);
    }

    auto localPart = NebulaKeyUtils::getPart(localKey);
    // 2nd, get values from remote in-edge
    auto spPromiseVal =
        std::make_shared<nebula::cpp2::ErrorCode>(nebula::cpp2::ErrorCode::SUCCEEDED);
    auto c = folly::makePromiseContract<nebula::cpp2::ErrorCode>();

    auto dst = NebulaKeyUtils::getDstId(vIdLen, localKey);
    auto remotePartId = env_->metaClient_->partId(spaceId, dst.str());
    auto remoteKey = TransactionUtils::reverseRawKey(vIdLen, remotePartId, localKey);

    LOG_IF(INFO, FLAGS_trace_toss) << "try to get remote key=" << folly::hexlify(remoteKey)
        << ", according to lock=" << folly::hexlify(lockKey);
    interClient_->getValue(vIdLen, spaceId, remoteKey)
        .via(exec_.get())
        .thenValue([=](auto&& errOrVal) mutable {
            if (!nebula::ok(errOrVal)) {
                LOG_IF(INFO, FLAGS_trace_toss)
                    << "get remote key failed, lock=" << folly::hexlify(lockKey);
                *spPromiseVal = nebula::error(errOrVal);
                return;
            }
            auto lockedPtr = result->wlock();
            if (!lockedPtr) {
                *spPromiseVal = nebula::cpp2::ErrorCode::E_MUTATE_EDGE_CONFLICT;
                return;
            }
            auto& kv = *lockedPtr;
            kv.first = localKey;
            kv.second = nebula::value(errOrVal);
            LOG_IF(INFO, FLAGS_trace_toss)
                << "got value=[" << kv.second << "] from remote key=" << folly::hexlify(remoteKey)
                << ", according to lock=" << folly::hexlify(lockKey);
            // 3rd, commit local key(indexes)
            /*
             * if mvcc enabled, get value will get exactly the same ver in-edge against lock
             *                  which means we can trust the val of lock as the out-edge
             *              else, don't trust lock.
             * */
            commitEdgeOut(
                    spaceId, localPart, std::string(kv.first), std::string(kv.second))
                    .via(exec_.get())
                    .thenValue([=](auto&& rc) { *spPromiseVal = rc; })
                    .thenError([=](auto&&) {
                        *spPromiseVal = nebula::cpp2::ErrorCode::E_UNKNOWN;
                    });
        })
        .thenValue([=](auto&&) {
            // 4th, remove persist lock
            LOG_IF(INFO, FLAGS_trace_toss) << "erase lock " << folly::hexlify(lockKey)
                << ", *spPromiseVal=" << static_cast<int32_t>(*spPromiseVal);
            if (*spPromiseVal == nebula::cpp2::ErrorCode::SUCCEEDED ||
                *spPromiseVal == nebula::cpp2::ErrorCode::E_KEY_NOT_FOUND ||
                *spPromiseVal == nebula::cpp2::ErrorCode::E_OUTDATED_LOCK) {
                auto eraseRet = eraseKey(spaceId, localPart, lockKey);
                eraseRet.wait();
                auto code = eraseRet.hasValue() ? eraseRet.value() :
                    nebula::cpp2::ErrorCode::E_UNKNOWN;
                *spPromiseVal = code;
            }
        })
        .thenError([=](auto&& ex) {
            LOG(ERROR) << ex.what();
            *spPromiseVal = nebula::cpp2::ErrorCode::E_UNKNOWN;
        })
        .ensure([=, p = std::move(c.first)]() mutable {
            eraseMemoryLock(localKey, ver);
            LOG_IF(INFO, FLAGS_trace_toss)
                << "end resume *spPromiseVal=" << static_cast<int32_t>(*spPromiseVal);
            p.setValue(*spPromiseVal);
        });
    return std::move(c.second).via(exec_.get());
}

// combine multi put and remove in a batch
// this may sometimes reduce some raft operation
folly::SemiFuture<nebula::cpp2::ErrorCode>
TransactionManager::commitBatch(GraphSpaceID spaceId,
                                PartitionID partId,
                                std::string&& batch) {
    auto c = folly::makePromiseContract<nebula::cpp2::ErrorCode>();
    env_->kvstore_->asyncAppendBatch(
        spaceId, partId, std::move(batch),
        [pro = std::move(c.first)](nebula::cpp2::ErrorCode rc) mutable {
            pro.setValue(rc);
        });
    return std::move(c.second);
}

/*
 * 1. use rawKey without version as in-memory lock key
 * */
folly::SemiFuture<nebula::cpp2::ErrorCode>
TransactionManager::commitEdgeOut(GraphSpaceID spaceId,
                                  PartitionID partId,
                                  std::string&& key,
                                  std::string&& props) {
    std::vector<std::shared_ptr<nebula::meta::cpp2::IndexItem>> indexes;
    auto idxRet = env_->indexMan_->getEdgeIndexes(spaceId);
    if (idxRet.ok()) {
        indexes = std::move(idxRet).value();
    }
    if (!indexes.empty()) {
        std::vector<kvstore::KV> data{{std::move(key), std::move(props)}};

        auto c = folly::makePromiseContract<nebula::cpp2::ErrorCode>();

        auto atomic = [partId, edges = std::move(data), this]() -> folly::Optional<std::string> {
            auto* processor = AddEdgesProcessor::instance(env_);
            auto ret = processor->addEdges(partId, edges);
            if (nebula::ok(ret)) {
                return nebula::value(ret);
            } else {
                return folly::Optional<std::string>();
            }
        };

        auto cb = [pro = std::move(c.first)](nebula::cpp2::ErrorCode rc) mutable {
            pro.setValue(rc);
        };

        env_->kvstore_->asyncAtomicOp(spaceId, partId, atomic, std::move(cb));
        return std::move(c.second);
    }
    return commitEdge(spaceId, partId, key, props);
}

folly::SemiFuture<nebula::cpp2::ErrorCode>
TransactionManager::commitEdge(GraphSpaceID spaceId,
                               PartitionID partId,
                               std::string& key,
                               std::string& props) {
    std::vector<kvstore::KV> data;
    data.emplace_back(std::move(key), std::move(props));

    auto c = folly::makePromiseContract<nebula::cpp2::ErrorCode>();
    env_->kvstore_->asyncMultiPut(
        spaceId,
        partId,
        std::move(data),
        [pro = std::move(c.first)](nebula::cpp2::ErrorCode rc) mutable {
            pro.setValue(rc);
        });
    return std::move(c.second);
}

folly::SemiFuture<nebula::cpp2::ErrorCode>
TransactionManager::eraseKey(GraphSpaceID spaceId,
                             PartitionID partId,
                             const std::string& key) {
    LOG_IF(INFO, FLAGS_trace_toss) << "eraseKey: " << folly::hexlify(key);
    auto c = folly::makePromiseContract<nebula::cpp2::ErrorCode>();
    env_->kvstore_->asyncRemove(
        spaceId,
        partId,
        key,
        [pro = std::move(c.first)](nebula::cpp2::ErrorCode code) mutable {
            LOG_IF(INFO, FLAGS_trace_toss) << "asyncRemove code=" << static_cast<int>(code);
            pro.setValue(code);
        });
    return std::move(c.second);
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


std::string TransactionManager::encodeBatch(std::vector<KV>&& data) {
    kvstore::BatchHolder bat;
    for (auto& kv : data) {
        bat.put(std::move(kv.first), std::move(kv.second));
    }
    return encodeBatchValue(bat.getBatch());
}

}   // namespace storage
}   // namespace nebula
