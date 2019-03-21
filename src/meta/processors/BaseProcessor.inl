/* Copyright (c) 2018 - present, VE Software Inc. All rights reserved
 *
 * This source code is licensed under Apache 2.0 License
 *  (found in the LICENSE.Apache file in the root directory)
 */

#include "meta/MetaServiceUtils.h"
#include "meta/processors/BaseProcessor.h"

namespace nebula {
namespace meta {

template<typename RESP>
void BaseProcessor<RESP>::doPut(std::vector<kvstore::KV> data) {
    kvstore_->asyncMultiPut(kDefaultSpaceId_, kDefaultPartId_, std::move(data),
                            [this] (kvstore::ResultCode code) {
        this->resp_.set_code(to(code));
        this->onFinished();
    });
}


template<typename RESP>
StatusOr<std::string> BaseProcessor<RESP>::doGet(const std::string& key) {
    auto res = kvstore_->get(kDefaultSpaceId_, kDefaultPartId_, key);
    if (ok(res)) {
        return value(std::move(res));
    }
    switch (error(res)) {
        case kvstore::ResultCode::ERR_KEY_NOT_FOUND:
            return Status::Error("Key Not Found");
        default:
            return Status::Error("Get Failed");
    }
}


template<typename RESP>
StatusOr<std::vector<std::string>>
BaseProcessor<RESP>::doMultiGet(const std::vector<std::string>& keys) {
    auto res = kvstore_->multiGet(kDefaultSpaceId_, kDefaultPartId_, keys);
    if (!ok(res)) {
        return Status::Error("MultiGet Failed");
    }
    return value(std::move(res));
}


template<typename RESP>
void BaseProcessor<RESP>::doRemove(const std::string& key) {
    kvstore_->asyncRemove(kDefaultSpaceId_,
                          kDefaultPartId_,
                          key,
                          [this] (kvstore::ResultCode code) {
        this->resp_.set_code(to(code));
        this->onFinished();
    });
}


template<typename RESP>
void BaseProcessor<RESP>::doMultiRemove(std::vector<std::string> keys) {
    kvstore_->asyncMultiRemove(kDefaultSpaceId_,
                               kDefaultPartId_,
                               std::move(keys),
                               [this] (kvstore::ResultCode code) {
        this->resp_.set_code(to(code));
        this->onFinished();
    });
}


template<typename RESP>
void BaseProcessor<RESP>::doRemoveRange(const std::string& start,
                                        const std::string& end) {
    kvstore_->asyncRemoveRange(kDefaultSpaceId_,
                               kDefaultPartId_,
                               start,
                               end,
                               [this] (kvstore::ResultCode code) {
        this->resp_.set_code(to(code));
        this->onFinished();
    });
}


template<typename RESP>
StatusOr<std::vector<std::string>> BaseProcessor<RESP>::doScan(
        const std::string& start,
        const std::string& end) {
    auto res = kvstore_->range(kDefaultSpaceId_, kDefaultPartId_, start, end);
    if (!ok(res)) {
        return Status::Error("Scan Failed");
    }

    std::vector<std::string> values;
    std::unique_ptr<kvstore::KVIterator> iter = value(std::move(res));
    while (iter->valid()) {
        values.emplace_back(iter->val());
        iter->next();
    }
    return values;
}


template<typename RESP>
StatusOr<std::vector<nebula::cpp2::HostAddr>> BaseProcessor<RESP>::allHosts() {
    std::vector<nebula::cpp2::HostAddr> hosts;
    const auto& prefix = MetaServiceUtils::hostPrefix();
    auto res = kvstore_->prefix(kDefaultSpaceId_, kDefaultPartId_, prefix);
    if (!ok(res)) {
        return Status::Error("Can't find any hosts");
    }

    std::unique_ptr<kvstore::KVIterator> iter = value(std::move(res));
    while (iter->valid()) {
        nebula::cpp2::HostAddr h;
        auto hostAddrPiece = iter->key().subpiece(prefix.size());
        memcpy(&h, hostAddrPiece.data(), hostAddrPiece.size());
        hosts.emplace_back(std::move(h));
        iter->next();
    }
    return hosts;
}


template<typename RESP>
int32_t BaseProcessor<RESP>::autoIncrementId() {
    folly::SharedMutex::WriteHolder holder(LockUtils::idLock());
    static const std::string kIdKey = "__id__";
    int32_t id;
    std::string val;
    auto res = kvstore_->get(kDefaultSpaceId_, kDefaultPartId_, kIdKey);
    if (!ok(res)) {
        CHECK_EQ(error(res), kvstore::ResultCode::ERR_KEY_NOT_FOUND);
        id = 1;
    } else {
        id = *reinterpret_cast<const int32_t*>(value(std::move(res)).c_str()) + 1;
    }

    std::vector<kvstore::KV> data;
    data.emplace_back(kIdKey,
                      std::string(reinterpret_cast<const char*>(&id), sizeof(id)));
    kvstore_->asyncMultiPut(kDefaultSpaceId_,
                            kDefaultPartId_,
                            std::move(data),
                            [this] (kvstore::ResultCode code) {
        CHECK_EQ(code, kvstore::ResultCode::SUCCEEDED);
    });

    return id;
}


template<typename RESP>
Status BaseProcessor<RESP>::spaceExist(GraphSpaceID spaceId) {
    folly::SharedMutex::ReadHolder rHolder(LockUtils::spaceLock());
    auto spaceKey = MetaServiceUtils::spaceKey(spaceId);
    std::string val;
    auto res = kvstore_->get(kDefaultSpaceId_, kDefaultPartId_, spaceKey);
    if (ok(res)) {
        return Status::OK();
    }

    return Status::SpaceNotFound();
}


template<typename RESP>
Status BaseProcessor<RESP>::hostsExist(const std::vector<std::string> &hostsKey) {
    for (const auto& hostKey : hostsKey) {
        auto res = kvstore_->get(kDefaultSpaceId_, kDefaultPartId_, hostKey);
        if (!ok(res)) {
            if (error(res) == kvstore::ResultCode::ERR_KEY_NOT_FOUND) {
                nebula::cpp2::HostAddr host = MetaServiceUtils::parseHostKey(hostKey);
                std::string ip = NetworkUtils::intToIPv4(host.get_ip());
                int32_t port = host.get_port();
                VLOG(3) << "Error, host IP " << ip << " port " << port
                        << " not exist";
                return Status::HostNotFound();
            } else {
                VLOG(3) << "Unknown Error , result = "
                        << static_cast<int32_t>(error(res));
                return Status::Error("Unknown error!");
            }
        }
    }
    return Status::OK();
}


template<typename RESP>
StatusOr<GraphSpaceID> BaseProcessor<RESP>::getSpaceId(const std::string& name) {
    auto indexKey = MetaServiceUtils::indexSpaceKey(name);
    auto res = kvstore_->get(kDefaultSpaceId_, kDefaultPartId_, indexKey);
    if (ok(res)) {
        return *reinterpret_cast<const GraphSpaceID*>(value(std::move(res)).c_str());
    }
    return Status::SpaceNotFound(folly::stringPrintf("Space %s not found", name.c_str()));
}


template<typename RESP>
StatusOr<TagID> BaseProcessor<RESP>::getTagId(GraphSpaceID spaceId,
                                              const std::string& name) {
    auto indexKey = MetaServiceUtils::indexTagKey(spaceId, name);
    auto res = kvstore_->get(kDefaultSpaceId_, kDefaultPartId_, indexKey);
    if (ok(res)) {
        return *reinterpret_cast<const TagID*>(value(std::move(res)).c_str());
    }
    return Status::TagNotFound(folly::stringPrintf("Tag %s not found", name.c_str()));
}


template<typename RESP>
StatusOr<EdgeType> BaseProcessor<RESP>::getEdgeType(GraphSpaceID spaceId,
                                                    const std::string& name) {
    auto indexKey = MetaServiceUtils::indexEdgeKey(spaceId, name);
    auto res = kvstore_->get(kDefaultSpaceId_, kDefaultPartId_, indexKey);
    if (ok(res)) {
        return *reinterpret_cast<const TagID*>(value(std::move(res)).c_str());
    }
    return Status::EdgeNotFound(folly::stringPrintf("Edge %s not found", name.c_str()));
}

}  // namespace meta
}  // namespace nebula
