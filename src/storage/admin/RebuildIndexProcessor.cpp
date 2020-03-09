/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "storage/StorageFlags.h"
#include "storage/admin/RebuildIndexProcessor.h"

namespace nebula {
namespace storage {

void RebuildIndexProcessor::processInternal(const cpp2::RebuildIndexRequest& req) {
    CHECK_NOTNULL(kvstore_);
    auto space = req.get_space_id();
    auto parts = req.get_parts();
    auto indexID = req.get_index_id();
    auto isOffline = req.get_is_offline();

    auto itemRet = getIndex(space, indexID);
    if (!itemRet.ok()) {
        cpp2::ResultCode thriftRet;
        thriftRet.set_code(cpp2::ErrorCode::E_INDEX_NOT_FOUND);
        codes_.emplace_back(thriftRet);
        onFinished();
        return;
    }

    if (env_->getIndexID() != -1) {
        cpp2::ResultCode thriftRet;
        thriftRet.set_code(cpp2::ErrorCode::E_INDEX_NOT_FOUND);
        codes_.emplace_back(thriftRet);
        onFinished();
        return;
    }

    env_->setIndexID(indexID);
    auto item = itemRet.value();
    auto schemaID = item->get_schema_id();
    if (schemaID.getType() == nebula::cpp2::SchemaID::Type::tag_id) {
        env_->setTagID(schemaID.get_tag_id());
    } else {
        env_->setEdgeType(schemaID.get_edge_type());
    }

    if (isOffline) {
        LOG(INFO) << "Rebuild Index Offline Space: " << space
                  << " Index: " << indexID;
    } else {
        LOG(INFO) << "Rebuild Index Space: " << space
                  << " Index: " << indexID;
    }

    // Firstly, we should cleanup the operation records.
    if (!isOffline) {
        LOG(INFO) << "Remove Operation Logs";
    }

    for (PartitionID part : parts) {
        env_->setPartID(part);
        std::unique_ptr<kvstore::KVIterator> iter;
        auto prefix = NebulaKeyUtils::prefix(part);
        auto ret = kvstore_->prefix(space, part, prefix, &iter);
        if (ret != kvstore::ResultCode::SUCCEEDED) {
            LOG(ERROR) << "Processing Part " << part << " Failed";
            this->pushResultCode(to(ret), part);
            onFinished();
            return;
        }

        buildIndex(space, part, schemaID, indexID, iter.get(), item->get_fields());

        if (!isOffline) {
            int32_t counter = -1;
            while (true) {
                std::unique_ptr<kvstore::KVIterator> operationIter;
                auto operationPrefix = NebulaKeyUtils::operationPrefix(part);
                auto operationRet = kvstore_->prefix(space, part, operationPrefix, &operationIter);
                if (operationRet != kvstore::ResultCode::SUCCEEDED) {
                    LOG(ERROR) << "Processing Part " << part << " Failed";
                    this->pushResultCode(to(operationRet), part);
                    onFinished();
                    return;
                }

                bool writeLocked = false;
                if (counter != -1 && counter < 1024) {
                    writeLocked = true;
                    if (!writeBlocking(space, true)) {
                       LOG(ERROR) << "Blocking Write Failed Space: " << space;
                       this->pushResultCode(cpp2::ErrorCode::E_BLOCKING_WRITE, part);
                       onFinished();
                       return;
                    }
                }

                // int32_t cleanBatchNumber = 0;
                // int32_t cleanBatchSize = 1024;
                // std::vector<std::string> keys;
                // keys.reserve(cleanBatchSize);
                while (operationIter && operationIter->valid()) {
                    counter += 1;
                    auto opKey = iter->key();
                    // auto opVal = iter->val();
                    // replay operation record
                    if (NebulaKeyUtils::isModifyOperation(opKey)) {
                        processModifyOperation(space, part, opKey);
                    } else if (NebulaKeyUtils::isDeleteOperation(opKey)) {
                        processDeleteOperation(space, part, opKey);
                    } else {
                        LOG(ERROR) << "Unknow Operation Type";
                        this->pushResultCode(cpp2::ErrorCode::E_INVALID_OPERATION, part);
                        onFinished();
                        return;
                    }

                    // remove operation record
                    // if (cleanBatchNumber < cleanBatchSize) {
                    //     cleanBatchNumber += 1;
                    //     keys.emplace_back(opKey);
                    // } else {
                    //     auto cleanRet = doSyncRemove(space, part, std::move(keys));
                    //     if (cleanRet != kvstore::ResultCode::SUCCEEDED) {
                    //         LOG(ERROR) << "Clean Operation Record " << part << " Failed";
                    //     }
                    //     keys.reserve(cleanBatchSize);
                    // }
                }

                if (writeLocked) {
                    if (!writeBlocking(space, false)) {
                       LOG(ERROR) << "Unlock Write Failed Space: " << space;
                       this->pushResultCode(cpp2::ErrorCode::E_BLOCKING_WRITE, part);
                       onFinished();
                       return;
                    } else {
                        break;
                    }
                }
            }
        }
    }

    env_->cleanup();
    onFinished();
}

void RebuildIndexProcessor::processModifyOperation(GraphSpaceID space,
                                                   PartitionID part,
                                                   const folly::StringPiece& opKey) {
    auto atomic = [opKey, this]() -> std::string {
        return modifyOpLog(opKey);
    };

    auto callback = [space, part, this](kvstore::ResultCode code) {
        handleAsync(space, part, code);
    };

    kvstore_->asyncAtomicOp(space, part, atomic, callback);
}

std::string RebuildIndexProcessor::modifyOpLog(const folly::StringPiece& opKey) {
    std::unique_ptr<kvstore::BatchHolder> holder = std::make_unique<kvstore::BatchHolder>();
    auto key = NebulaKeyUtils::getOperationKey(opKey);
    holder->put(key.data(), "");
    holder->remove(opKey.data());
    return encodeBatchValue(holder->getBatch());
}

void RebuildIndexProcessor::processDeleteOperation(GraphSpaceID space,
                                                   PartitionID part,
                                                   const folly::StringPiece& opKey) {
    auto atomic = [opKey, this]() -> std::string {
        return deleteOpLog(opKey);
    };

    auto callback = [space, part, this](kvstore::ResultCode code) {
        handleAsync(space, part, code);
    };

    kvstore_->asyncAtomicOp(space, part, atomic, callback);
}

std::string RebuildIndexProcessor::deleteOpLog(const folly::StringPiece& opKey) {
    std::unique_ptr<kvstore::BatchHolder> holder = std::make_unique<kvstore::BatchHolder>();
    auto key = NebulaKeyUtils::getOperationKey(opKey);
    holder->remove(key.data());
    holder->remove(opKey.data());
    return encodeBatchValue(holder->getBatch());
}

}  // namespace storage
}  // namespace nebula
