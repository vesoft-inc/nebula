/* Copyright (c) 2018 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */
#include "kvstore/PartManager.h"

namespace nebula {
namespace kvstore {

PartsMap MemPartManager::parts(const HostAddr& hostAddr) {
    UNUSED(hostAddr);
    return partsMap_;
}

PartMeta MemPartManager::partMeta(GraphSpaceID spaceId, PartitionID partId) {
    auto it = partsMap_.find(spaceId);
    CHECK(it != partsMap_.end());
    auto partIt = it->second.find(partId);
    CHECK(partIt != it->second.end());
    return partIt->second;
}

bool MemPartManager::partExist(const HostAddr& host, GraphSpaceID spaceId, PartitionID partId) {
    UNUSED(host);
    auto it = partsMap_.find(spaceId);
    if (it != partsMap_.end()) {
        auto partIt = it->second.find(partId);
        if (partIt != it->second.end()) {
            return true;
        }
    }
    return false;
}

MetaServerBasedPartManager::MetaServerBasedPartManager(HostAddr host, meta::MetaClient *client)
    : localHost_(std::move(host)) {
    client_ = client;
    CHECK_NOTNULL(client_);
    client_->registerListener(this);
}

MetaServerBasedPartManager::~MetaServerBasedPartManager() {
    VLOG(3) << "~MetaServerBasedPartManager";
    if (nullptr != client_) {
        client_->unRegisterListener();
        client_ = nullptr;
    }
}

PartsMap MetaServerBasedPartManager::parts(const HostAddr& hostAddr) {
    return client_->getPartsMapFromCache(hostAddr);
}

PartMeta MetaServerBasedPartManager::partMeta(GraphSpaceID spaceId, PartitionID partId) {
    return client_->getPartMetaFromCache(spaceId, partId);
}

bool MetaServerBasedPartManager::partExist(const HostAddr& host,
                                           GraphSpaceID spaceId,
                                           PartitionID partId) {
    return client_->checkPartExistInCache(host, spaceId, partId);
}

bool MetaServerBasedPartManager::spaceExist(const HostAddr& host,
                                            GraphSpaceID spaceId) {
    return client_->checkSpaceExistInCache(host, spaceId);
}

void MetaServerBasedPartManager::onSpaceAdded(GraphSpaceID spaceId) {
    if (handler_ != nullptr) {
        handler_->addSpace(spaceId);
    } else {
        VLOG(1) << "handler_ is nullptr!";
    }
}

void MetaServerBasedPartManager::onSpaceRemoved(GraphSpaceID spaceId) {
    if (handler_ != nullptr) {
        handler_->removeSpace(spaceId);
    } else {
        VLOG(1) << "handler_ is nullptr!";
    }
}

void MetaServerBasedPartManager::onPartAdded(const PartMeta& partMeta) {
    if (handler_ != nullptr) {
        handler_->addPart(partMeta.spaceId_, partMeta.partId_, false);
    } else {
        VLOG(1) << "handler_ is nullptr!";
    }
}

void MetaServerBasedPartManager::onPartRemoved(GraphSpaceID spaceId, PartitionID partId) {
    if (handler_ != nullptr) {
        handler_->removePart(spaceId, partId);
    } else {
        VLOG(1) << "handler_ is nullptr!";
    }
}

void MetaServerBasedPartManager::onPartUpdated(const PartMeta& partMeta) {
    UNUSED(partMeta);
}

}  // namespace kvstore
}  // namespace nebula
