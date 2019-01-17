/* Copyright (c) 2018 - present, VE Software Inc. All rights reserved
 *
 * This source code is licensed under Apache 2.0 License
 *  (found in the LICENSE.Apache file in the root directory)
 */

#ifndef META_LISTCHILDRENPROCESSOR_H_
#define META_LISTCHILDRENPROCESSOR_H_

#include "base/Base.h"
#include "meta/BaseProcessor.h"
#include "fs/FileUtils.h"

namespace nebula {
namespace meta {

class ListChildrenProcessor : public BaseProcessor<cpp2::ListChildrenResponse> {
public:
    static ListChildrenProcessor* instance(kvstore::KVStore* kvstore, folly::RWSpinLock* lock) {
        return new ListChildrenProcessor(kvstore, lock);
    }

    void process(const cpp2::ListChildrenRequest& req) {
        auto path = MetaUtils::normalize(req.get_path());
        auto layer = MetaUtils::layer(path);
        auto key = MetaUtils::metaKey(layer, path);
        // Acquire the read lock.
        rh_ = std::make_unique<folly::RWSpinLock::ReadHolder>(*lock_);
        // Check the node exists or not. If NOT existed, return failure.
        std::string value;
        auto ret = kvstore_->get(kDefaultSpaceId, kDefaultPartId, key, &value);
        if (ret != kvstore::ResultCode::SUCCESSED) {
            resp_.set_code(to(ret));
            VLOG(3) << "Get node failed! key " << key;
            onFinished();
            return;
        }
        auto prefix = MetaUtils::metaKey(layer + 1, path);
        std::unique_ptr<kvstore::StorageIter> iter;
        ret = kvstore_->prefix(kDefaultSpaceId, kDefaultPartId, prefix, &iter);
        if (ret != kvstore::ResultCode::SUCCESSED || !iter) {
            resp_.set_code(to(ret));
            onFinished();
            return;
        }
        while (iter->valid()) {
            auto child = MetaUtils::getPath(iter->key());
            resp_.children.emplace_back(child.data(), child.size());
            iter->next();
        }
        onFinished();
    }

private:
    ListChildrenProcessor(kvstore::KVStore* kvstore, folly::RWSpinLock* lock)
            : BaseProcessor<cpp2::ListChildrenResponse>(kvstore, lock) {}

private:
    std::unique_ptr<folly::RWSpinLock::ReadHolder> rh_;
};


}  // namespace meta
}  // namespace nebula
#endif  // META_LISTCHILDRENPROCESSOR_H_
