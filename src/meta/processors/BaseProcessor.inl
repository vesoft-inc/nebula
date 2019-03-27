/* Copyright (c) 2018 - present, VE Software Inc. All rights reserved
 *
 * This source code is licensed under Apache 2.0 License
 *  (found in the LICENSE.Apache file in the root directory)
 */

#include "meta/MetaUtils.h"
#include "meta/processors/BaseProcessor.h"

namespace nebula {
namespace meta {

template<typename RESP>
void BaseProcessor<RESP>::doPut(std::vector<kvstore::KV> data) {
    CHECK(!lock_.try_lock());
    kvstore_->asyncMultiPut(kDefaultSpaceId_, kDefaultPartId_, std::move(data),
                            [this] (kvstore::ResultCode code, HostAddr leader) {
        UNUSED(leader);
        this->resp_.set_code(to(code));
        this->onFinished();
    });
}

template<typename RESP>
StatusOr<std::string> BaseProcessor<RESP>::doGet(const std::string& key) {
    CHECK(!lock_.try_lock());
    std::string value;
    auto code = kvstore_->get(kDefaultSpaceId_, kDefaultPartId_,
                              std::move(key), &value);
    if (code != kvstore::ResultCode::SUCCEEDED) {
        return Status::Error("Get Failed");
    }
    return value;
}

template<typename RESP>
StatusOr<std::vector<std::string>>
BaseProcessor<RESP>::doMultiGet(const std::vector<std::string> keys) {
    CHECK(!lock_.try_lock());
    std::vector<std::string> values;
    auto code = kvstore_->multiGet(kDefaultSpaceId_, kDefaultPartId_,
                                   std::move(keys), &values);
    if (code != kvstore::ResultCode::SUCCEEDED) {
        return Status::Error("MultiGet Failed");
    }
    return values;
}

template<typename RESP>
void BaseProcessor<RESP>::doRemove(const std::string& key) {
    CHECK(!lock_.try_lock());
    kvstore_->asyncRemove(kDefaultSpaceId_, kDefaultPartId_, std::move(key),
                          [this] (kvstore::ResultCode code, HostAddr leader) {
        UNUSED(leader);
        this->resp_.set_code(to(code));
        this->onFinished();
    });
}

template<typename RESP>
void BaseProcessor<RESP>::doRemoveRange(const std::string& start,
                                        const std::string& end) {
    CHECK(!lock_.try_lock());
    kvstore_->asyncRemoveRange(kDefaultSpaceId_, kDefaultPartId_,
                               std::move(start), std::move(end),
                               [this] (kvstore::ResultCode code, HostAddr leader) {
        UNUSED(leader);
        this->resp_.set_code(to(code));
        this->onFinished();
    });
}

template<typename RESP>
StatusOr<std::vector<std::string>> BaseProcessor<RESP>::doScan(const std::string& start,
                                 const std::string& end) {
    CHECK(!lock_.try_lock());
    std::unique_ptr<kvstore::KVIterator> iter;
    auto code = kvstore_->range(kDefaultSpaceId_, kDefaultPartId_,
                                start, end, &iter);
    if (code != kvstore::ResultCode::SUCCEEDED) {
        return Status::Error("Scan Failed");
    }

    std::vector<std::string> values;
    while (iter->valid()) {
        values.emplace_back(std::move(iter->val()));
        iter->next();
    }
    return values;
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
    CHECK(!lock_.try_lock());
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

// TODO(dangleptr) Maybe we could use index to improve the efficient
template<typename RESP>
StatusOr<GraphSpaceID> BaseProcessor<RESP>::spaceExist(const std::string& name) {
    auto prefix = MetaUtils::spacePrefix();
    std::unique_ptr<kvstore::KVIterator> iter;
    auto ret = kvstore_->prefix(kDefaultSpaceId_, kDefaultPartId_, prefix, &iter);
    if (ret != kvstore::ResultCode::SUCCEEDED) {
        VLOG(3) << "Error, prefix " << prefix << ", ret = " << static_cast<int32_t>(ret);
        return Status::Error("Unknown error!");
    }
    while (iter->valid()) {
        auto spaceName = MetaUtils::spaceName(iter->val());
        if (spaceName == name) {
            return MetaUtils::spaceId(iter->key());
        }
        iter->next();
    }
    return Status::SpaceNotFound();
}

template<typename RESP>
bool BaseProcessor<RESP>::checkRetainedPrefix(const std::string& key) {
    if ((key == MetaUtils::spacePrefix()) ||
        (key == MetaUtils::partPrefix()) ||
        (key == MetaUtils::hostPrefix())) {
        return false;
    } else {
        return true;
    }
}

}  // namespace meta
}  // namespace nebula

