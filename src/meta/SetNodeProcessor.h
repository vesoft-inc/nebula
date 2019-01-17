/* Copyright (c) 2018 - present, VE Software Inc. All rights reserved
 *
 * This source code is licensed under Apache 2.0 License
 *  (found in the LICENSE.Apache file in the root directory)
 */

#ifndef META_SETNODEPROCESSOR_H_
#define META_SETNODEPROCESSOR_H_

#include "base/Base.h"
#include "meta/BaseProcessor.h"
#include "fs/FileUtils.h"

namespace nebula {
namespace meta {

class SetNodeProcessor : public BaseProcessor<cpp2::ExecResponse> {
public:
    static SetNodeProcessor* instance(kvstore::KVStore* kvstore, folly::RWSpinLock* lock) {
        return new SetNodeProcessor(kvstore, lock);
    }

    void process(const cpp2::SetNodeRequest& req) {
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
        std::vector<kvstore::KV> data;
        data.emplace_back(std::move(key), std::move(req.get_value()));
        kvstore_->asyncMultiPut(kDefaultSpaceId, kDefaultPartId, std::move(data),
            [this] (kvstore::ResultCode code, HostAddr addr) {
            UNUSED(addr);
            resp_.set_code(to(code));
            this->onFinished();
        });
    }

private:
    SetNodeProcessor(kvstore::KVStore* kvstore, folly::RWSpinLock* lock)
            : BaseProcessor<cpp2::ExecResponse>(kvstore, lock) {}

private:
    std::unique_ptr<folly::RWSpinLock::ReadHolder> rh_;
};


}  // namespace meta
}  // namespace nebula
#endif  // META_SETNODEPROCESSOR_H_
