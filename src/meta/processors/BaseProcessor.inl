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
    folly::RWSpinLock::WriteHolder holder(LockUtils::idLock());
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
bool BaseProcessor<RESP>::spaceExist(GraphSpaceID spaceId) {
    folly::RWSpinLock::ReadHolder rHolder(LockUtils::spaceLock());
    auto spaceKey = MetaUtils::spaceKey(spaceId);
    std::string val;
    auto ret = kvstore_->get(kDefaultSpaceId_, kDefaultPartId_, spaceKey, &val);
    if (ret == kvstore::ResultCode::SUCCEEDED) {
        return true;
    }
    return false;
}

}  // namespace meta
}  // namespace nebula

