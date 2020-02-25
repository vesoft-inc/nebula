/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef META_CFHELPER_H_
#define META_CFHELPER_H_

#include "base/Base.h"
#include "meta/MetaServiceUtils.h"
#include "meta/processors/Common.h"
#include "kvstore/NebulaStore.h"

namespace nebula {
namespace meta {

class MetaCFHelper {
public:
    MetaCFHelper() = default;

    void registerKv(kvstore::KVStore* kv) {
        kv_ = kv;
    }

    void init() {
        LOG(INFO) << __PRETTY_FUNCTION__;
        // scan the spaces whenever we need to compact
        spaces_.clear();
        if (kv_ != nullptr) {
            folly::SharedMutex::ReadHolder rHolder(LockUtils::spaceLock());
            auto prefix = MetaServiceUtils::spacePrefix();
            std::unique_ptr<kvstore::KVIterator> iter;
            auto ret = kv_->prefix(kDefaultSpaceId, kDefaultPartId, prefix, &iter);
            if (ret != kvstore::ResultCode::SUCCEEDED) {
                return;
            }
            while (iter->valid()) {
                auto spaceId = MetaServiceUtils::spaceId(iter->key());
                spaces_.insert(spaceId);
                iter->next();
            }
        }
    }

    bool spaceValid(GraphSpaceID spaceId) const {
        // If spaceId is not in the spaces_ when we create compaction filter,
        // valid spaceId must be greater than the max spaceId in spaces_;
        return spaces_.count(spaceId) || (spaces_.upper_bound(spaceId) == spaces_.end());
    }

private:
    kvstore::KVStore* kv_;
    std::set<GraphSpaceID> spaces_;
};

}   // namespace meta
}   // namespace nebula
#endif   // META_CFHELPER_H_
