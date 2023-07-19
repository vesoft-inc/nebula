/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#ifndef META_ROOTUSERMAN_H_
#define META_ROOTUSERMAN_H_

#include <proxygen/lib/utils/CryptUtil.h>

#include "common/base/Base.h"
#include "common/utils/MetaKeyUtils.h"
#include "interface/gen-cpp2/common_types.h"
#include "kvstore/KVStore.h"

namespace nebula {
namespace meta {

/**
 * This class manages root user for meta server.
 * */
class RootUserMan {
 public:
  static ErrorOr<nebula::cpp2::ErrorCode, bool> isGodExists(kvstore::KVStore* kv, bool fromLeader) {
    auto rolePrefix = MetaKeyUtils::roleSpacePrefix(kDefaultSpaceId);
    std::unique_ptr<kvstore::KVIterator> iter;
    auto code = kv->prefix(kDefaultSpaceId, kDefaultPartId, rolePrefix, &iter, !fromLeader);
    if (code != nebula::cpp2::ErrorCode::SUCCEEDED) {
      return code;
    }
    while (iter->valid()) {
      auto val = iter->val();
      auto type = *reinterpret_cast<const meta::cpp2::RoleType*>(val.begin());
      if (type == meta::cpp2::RoleType::GOD) {
        LOG(INFO) << "God user exists";
        return true;
      }
      iter->next();
    }
    LOG(INFO) << "God user is not exists";
    return false;
  }

  static bool isUserExists(kvstore::KVStore* kv) {
    auto userKey = MetaKeyUtils::userKey("root");
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

  static nebula::cpp2::ErrorCode initRootUser(kvstore::KVStore* kv) {
    LOG(INFO) << "Init root user";
    auto encodedPwd = proxygen::md5Encode(folly::StringPiece("nebula"));
    auto userKey = MetaKeyUtils::userKey("root");
    auto userVal = MetaKeyUtils::userVal(std::move(encodedPwd));
    auto roleKey = MetaKeyUtils::roleKey(kDefaultSpaceId, "root");
    auto roleVal = MetaKeyUtils::roleVal(cpp2::RoleType::GOD);

    std::vector<kvstore::KV> data;
    data.emplace_back(std::move(userKey), std::move(userVal));
    data.emplace_back(std::move(roleKey), std::move(roleVal));
    nebula::cpp2::ErrorCode ec;
    folly::Baton<true, std::atomic> baton;
    kv->asyncMultiPut(
        kDefaultSpaceId, kDefaultPartId, std::move(data), [&](nebula::cpp2::ErrorCode code) {
          ec = code;
          baton.post();
        });
    baton.wait();
    return ec;
  }
};

}  // namespace meta
}  // namespace nebula

#endif  // META_ROOTUSERMAN_H_
