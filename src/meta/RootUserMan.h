/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef META_ROOTUSERMAN_H_
#define META_ROOTUSERMAN_H_

#include "common/base/Base.h"
#include "common/encryption/MD5Utils.h"
#include "kvstore/KVStore.h"
#include "meta/MetaServiceUtils.h"

namespace nebula {
namespace meta {

/**
 * This class manages root user for meta server.
 * */
class RootUserMan {
public:
    static bool isUserExists(kvstore::KVStore* kv) {
        auto userKey = MetaServiceUtils::userKey("root");
        std::string val;
        auto result = kv->get(kDefaultSpaceId, kDefaultPartId, userKey, &val);
        if (result == nebula::cpp2::ErrorCode::SUCCEEDED) {
            LOG(INFO) << "Root user exists";
            return true;
        } else {
            LOG(INFO) << "Root user is not exists";
            return false;
        }
    }

    static bool initRootUser(kvstore::KVStore* kv) {
        LOG(INFO) << "Init root user";
        auto encodedPwd = encryption::MD5Utils::md5Encode("nebula");
        auto userKey = MetaServiceUtils::userKey("root");
        auto userVal = MetaServiceUtils::userVal(std::move(encodedPwd));
        auto roleKey = MetaServiceUtils::roleKey(kDefaultSpaceId, "root");
        auto roleVal = MetaServiceUtils::roleVal(cpp2::RoleType::GOD);

        std::vector<kvstore::KV> data;
        data.emplace_back(std::move(userKey), std::move(userVal));
        data.emplace_back(std::move(roleKey), std::move(roleVal));
        bool ret = true;
        folly::Baton<true, std::atomic> baton;
        kv->asyncMultiPut(kDefaultSpaceId, kDefaultPartId,
                          std::move(data), [&](nebula::cpp2::ErrorCode code) {
            if (code != nebula::cpp2::ErrorCode::SUCCEEDED) {
                LOG(ERROR) << "Put failed, error " << static_cast<int32_t>(code);
                ret = false;
            }
            baton.post();
        });
        baton.wait();
        return ret;
    }
};

}  // namespace meta
}  // namespace nebula

#endif   // META_ROOTUSERMAN_H_
