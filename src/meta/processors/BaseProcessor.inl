/* Copyright (c) 2018 - present, VE Software Inc. All rights reserved
 *
 * This source code is licensed under Apache 2.0 License
 *  (found in the LICENSE.Apache file in the root directory)
 */


#include "meta/processors/BaseProcessor.h"

namespace nebula {
namespace meta {

template<typename RESP>
void BaseProcessor<RESP>::doPut(std::vector<kvstore::KV> data) {
    kvstore_->asyncMultiPut(kDefaultSpaceId_, kDefaultPartId_, std::move(data),
                            [this] (kvstore::ResultCode code, HostAddr leader) {
        UNUSED(leader);
        this->resp_.set_code(to(code));
        this->onFinished();
    });
}

template<typename RESP>
void BaseProcessor<RESP>::doRemove(std::vector<std::string> keys) {
    kvstore_->asyncMultiRemove(kDefaultSpaceId_, kDefaultPartId_, std::move(keys),
                            [this] (kvstore::ResultCode code, HostAddr leader) {
        UNUSED(leader);
        this->resp_.set_code(to(code));
        this->onFinished();
    });
}

template<typename RESP>
StatusOr<std::vector<nebula::cpp2::HostAddr>> BaseProcessor<RESP>::allHosts() {
    std::vector<nebula::cpp2::HostAddr> hosts;
    const auto& prefix = MetaUtils::hostPrefix();
    std::unique_ptr<kvstore::KVIterator> iter;
    auto ret = kvstore_->prefix(kDefaultSpaceId_, kDefaultPartId_, prefix, &iter);
    if (ret != kvstore::ResultCode::SUCCEEDED) {
        return Status::Error("Can't find any hosts");
    }
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
    auto ret = kvstore_->get(kDefaultSpaceId_, kDefaultPartId_, kIdKey, &val);
    if (ret != kvstore::ResultCode::SUCCEEDED) {
        CHECK_EQ(ret, kvstore::ResultCode::ERR_KEY_NOT_FOUND);
        id = 1;
    } else {
        id = *reinterpret_cast<const int32_t*>(val.c_str()) + 1;
    }
    std::vector<kvstore::KV> data;
    data.emplace_back(kIdKey,
                      std::string(reinterpret_cast<const char*>(&id), sizeof(id)));
    kvstore_->asyncMultiPut(kDefaultSpaceId_, kDefaultPartId_, std::move(data),
                            [this] (kvstore::ResultCode code, HostAddr leader) {
        UNUSED(leader);
        CHECK_EQ(code, kvstore::ResultCode::SUCCEEDED);
    });
    return id;
}

template<typename RESP>
Status BaseProcessor<RESP>::spaceExist(GraphSpaceID spaceId) {
    folly::SharedMutex::ReadHolder rHolder(LockUtils::spaceLock());
    auto spaceKey = MetaUtils::spaceKey(spaceId);
    std::string val;
    auto ret = kvstore_->get(kDefaultSpaceId_, kDefaultPartId_, spaceKey, &val);
    if (ret == kvstore::ResultCode::SUCCEEDED) {
        return Status::OK();
    }
    return Status::SpaceNotFound();
}

template<typename RESP>
Status BaseProcessor<RESP>::hostsExist(const std::vector<std::string> &hostsKey) {
    for (const auto& hostKey : hostsKey) {
        std::string val;
        auto ret = kvstore_->get(kDefaultSpaceId_, kDefaultPartId_, hostKey , &val);
        if (ret != kvstore::ResultCode::SUCCEEDED) {
            if (ret == kvstore::ResultCode::ERR_KEY_NOT_FOUND) {
                nebula::cpp2::HostAddr host = MetaUtils::parseHostKey(hostKey);
                std::string ip = NetworkUtils::intToIPv4(host.get_ip());
                int32_t port = host.get_port();
                VLOG(3) << "Error, host IP " << ip << " port " << port
                        << " not exist";
                return Status::HostNotFound();
            } else {
                VLOG(3) << "Unknown Error ,, ret = " << static_cast<int32_t>(ret);
                return Status::Error("Unknown error!");
            }
        }
    }
    return Status::OK();
}

template<typename RESP>
StatusOr<GraphSpaceID> BaseProcessor<RESP>::getSpaceId(const std::string& name) {
    auto indexKey = MetaUtils::indexKey(EntryType::SPACE, name);
    std::string val;
    auto ret = kvstore_->get(kDefaultSpaceId_, kDefaultPartId_, indexKey, &val);
    if (ret == kvstore::ResultCode::SUCCEEDED) {
        return *reinterpret_cast<const GraphSpaceID*>(val.c_str());
    }
    return Status::SpaceNotFound();
}

}  // namespace meta
}  // namespace nebula

