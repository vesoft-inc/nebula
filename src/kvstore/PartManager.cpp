/* Copyright (c) 2018 - present, VE Software Inc. All rights reserved
 *
 * This source code is licensed under Apache 2.0 License
 *  (found in the LICENSE.Apache file in the root directory)
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
    if (nullptr == client) {
        LOG(INFO) << "MetaClient is nullptr, create new one";
        // multi instances use one metaclient
        static auto clientPtr = std::make_unique<meta::MetaClient>();
        static std::once_flag flag;
        std::call_once(flag, std::bind(&meta::MetaClient::init, clientPtr.get()));
        client_ = clientPtr.get();
    } else {
        client_ = client;
    }

    client_->registerListener(this);
}

MetaServerBasedPartManager::~MetaServerBasedPartManager() {
    VLOG(3) << "~MetaServerBasedPartManager";
    if (nullptr != client_) {
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
    CHECK_NOTNULL(handler_);
    handler_->addSpace(spaceId);
}

void MetaServerBasedPartManager::onSpaceRemoved(GraphSpaceID spaceId) {
    CHECK_NOTNULL(handler_);
    handler_->removeSpace(spaceId);
}

void MetaServerBasedPartManager::onPartAdded(const PartMeta& partMeta) {
    CHECK_NOTNULL(handler_);
    handler_->addPart(partMeta.spaceId_, partMeta.partId_);
}

void MetaServerBasedPartManager::onPartRemoved(GraphSpaceID spaceId, PartitionID partId) {
    CHECK_NOTNULL(handler_);
    handler_->removePart(spaceId, partId);
}

void MetaServerBasedPartManager::onPartUpdated(const PartMeta& partMeta) {
    UNUSED(partMeta);
}

}  // namespace kvstore
}  // namespace nebula

