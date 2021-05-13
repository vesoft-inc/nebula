/* Copyright (c) 2018 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef META_COMMON_H_
#define META_COMMON_H_

#include "common/base/Base.h"
#include "common/base/Status.h"
#include "common/interface/gen-cpp2/meta_types.h"
#include "kvstore/KVStore.h"
#include "meta/processors/Common.h"

namespace nebula {
namespace meta {

class MetaCommon final {
public:
    MetaCommon() = delete;

    static bool checkSegment(const std::string& segment) {
        static const std::regex pattern("^[0-9a-zA-Z]+$");
        if (!segment.empty() && std::regex_match(segment, pattern)) {
            return true;
        }
        return false;
    }

    static bool saveRebuildStatus(kvstore::KVStore* kvstore,
                                  std::string statusKey,
                                  std::string&& statusValue) {
        std::vector<kvstore::KV> status{std::make_pair(std::move(statusKey),
                                                       std::forward<std::string>(statusValue))};
        folly::Baton<true, std::atomic> baton;
        auto ret = nebula::cpp2::ErrorCode::SUCCEEDED;
        kvstore->asyncMultiPut(kDefaultSpaceId,
                               kDefaultPartId,
                               std::move(status),
                               [&ret, &baton] (nebula::cpp2::ErrorCode code) {
                                   if (nebula::cpp2::ErrorCode::SUCCEEDED != code) {
                                       ret = code;
                                       LOG(INFO) << "Put data error on meta server";
                                   }
                                   baton.post();
                               });
        baton.wait();
        if (ret != nebula::cpp2::ErrorCode::SUCCEEDED) {
            LOG(ERROR) << "Save Status Failed";
            return false;
        }
        return true;
    }
};

}  // namespace meta
}  // namespace nebula

#endif  // META_COMMON_H_
