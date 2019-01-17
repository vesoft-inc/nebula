/* Copyright (c) 2018 - present, VE Software Inc. All rights reserved
 *
 * This source code is licensed under Apache 2.0 License
 *  (found in the LICENSE.Apache file in the root directory)
 */

#ifndef META_GETNODEPROCESSOR_H_
#define META_GETNODEPROCESSOR_H_

#include "base/Base.h"
#include "meta/BaseProcessor.h"
#include "fs/FileUtils.h"

namespace nebula {
namespace meta {

class GetNodeProcessor : public BaseProcessor<cpp2::GetNodeResponse> {
public:
    static GetNodeProcessor* instance(kvstore::KVStore* kvstore, folly::RWSpinLock* lock) {
        return new GetNodeProcessor(kvstore, lock);
    }

    void process(const cpp2::GetNodeRequest& req) {
        auto path = MetaUtils::normalize(req.get_path());
        auto layer = MetaUtils::layer(path);
        auto key = MetaUtils::metaKey(layer, path);
        // Acquire the read lock.
        rh_ = std::make_unique<folly::RWSpinLock::ReadHolder>(*lock_);
        // Check the node exists or not. If NOT existed, return failure.
        std::string value;
        auto ret = kvstore_->get(kDefaultSpaceId, kDefaultPartId, key, &value);
        resp_.set_code(to(ret));
        if (ret != kvstore::ResultCode::SUCCESSED) {
            onFinished();
            return;
        }
        resp_.set_value(std::move(value));
        onFinished();
    }

private:
    GetNodeProcessor(kvstore::KVStore* kvstore, folly::RWSpinLock* lock)
            : BaseProcessor<cpp2::GetNodeResponse>(kvstore, lock) {}

private:
    std::unique_ptr<folly::RWSpinLock::ReadHolder> rh_;
};


}  // namespace meta
}  // namespace nebula
#endif  // META_GETNODEPROCESSOR_H_
