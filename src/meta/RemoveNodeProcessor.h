/* Copyright (c) 2018 - present, VE Software Inc. All rights reserved
 *
 * This source code is licensed under Apache 2.0 License
 *  (found in the LICENSE.Apache file in the root directory)
 */

#ifndef META_REMOVENODEPROCESSOR_H_
#define META_REMOVENODEPROCESSOR_H_

#include "base/Base.h"
#include "meta/BaseProcessor.h"
#include "fs/FileUtils.h"

namespace nebula {
namespace meta {

class RemoveNodeProcessor : public BaseProcessor<cpp2::ExecResponse> {
public:
    static RemoveNodeProcessor* instance(kvstore::KVStore* kvstore, folly::RWSpinLock* lock) {
        return new RemoveNodeProcessor(kvstore, lock);
    }

    void process(const cpp2::RemoveNodeRequest& req) {
        auto path = MetaUtils::normalize(req.get_path());
        auto layer = MetaUtils::layer(path);
        // Acquire the write lock.
        wh_ = std::make_unique<folly::RWSpinLock::WriteHolder>(*lock_);
        // Check the node exists or not. If NOT existed, return failure.
        auto key = MetaUtils::metaKey(layer, path);
        std::string value;
        auto ret = kvstore_->get(kDefaultSpaceId, kDefaultPartId, key, &value);
        resp_.set_code(to(ret));
        if (ret != kvstore::ResultCode::SUCCESSED) {
            VLOG(3) << "Error: ret = " << static_cast<int32_t>(ret);
            onFinished();
            return;
        }
        // Check the node has children or not. If children exited, return failure.
        auto prefix = MetaUtils::metaKey(layer + 1, path);
        std::unique_ptr<kvstore::StorageIter> iter;
        ret = kvstore_->prefix(kDefaultSpaceId, kDefaultPartId, prefix, &iter);
        if (ret == kvstore::ResultCode::SUCCESSED && iter->valid()) {
            VLOG(3) << "Error: ret = " << static_cast<int32_t>(ret);
            resp_.set_code(cpp2::ErrorCode::E_CHILD_EXISTED);
            onFinished();
            return;
        }
        // All check passed, remove current node.
        kvstore_->asyncRemove(kDefaultSpaceId, kDefaultPartId, key,
                             [this] (kvstore::ResultCode code, HostAddr addr) {
            UNUSED(addr);
            this->resp_.set_code(to(code));
            onFinished();
        });
    }

private:
    RemoveNodeProcessor(kvstore::KVStore* kvstore, folly::RWSpinLock* lock)
            : BaseProcessor<cpp2::ExecResponse>(kvstore, lock) {}

private:
    std::unique_ptr<folly::RWSpinLock::WriteHolder> wh_;
};


}  // namespace meta
}  // namespace nebula
#endif  // META_REMOVENODEPROCESSOR_H_
