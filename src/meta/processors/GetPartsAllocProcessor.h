/* Copyright (c) 2018 - present, VE Software Inc. All rights reserved
 *
 * This source code is licensed under Apache 2.0 License
 *  (found in the LICENSE.Apache file in the root directory)
 */

#ifndef META_GETPARTSALLOCPROCESSOR_H_
#define META_GETPARTSALLOCPROCESSOR_H_

#include "meta/processors/BaseProcessor.h"

namespace nebula {
namespace meta {

class GetPartsAllocProcessor : public BaseProcessor<cpp2::GetPartsAllocResp> {
public:
    static GetPartsAllocProcessor* instance(kvstore::KVStore* kvstore, folly::RWSpinLock* lock) {
        return new GetPartsAllocProcessor(kvstore, lock);
    }

    void process(const cpp2::GetPartsAllocReq& req) {
        auto spaceId = req.get_space_id();
        rh_ = std::make_unique<folly::RWSpinLock::ReadHolder>(*lock_);
        auto prefix = MetaUtils::partPrefix(spaceId);
        std::unique_ptr<kvstore::KVIterator> iter;
        auto ret = kvstore_->prefix(kDefaultSpaceId_, kDefaultPartId_, prefix, &iter);
        resp_.set_code(to(ret));
        if (ret != kvstore::ResultCode::SUCCEEDED) {
            onFinished();
            return;
        }
        decltype(resp_.parts) parts;
        while (iter->valid()) {
            auto key = iter->key();
            PartitionID partId;
            memcpy(&partId, key.data() + prefix.size(), sizeof(PartitionID));
            std::vector<nebula::cpp2::HostAddr> partHosts = MetaUtils::parsePartVal(iter->val());
            parts.emplace(partId, std::move(partHosts));
            iter->next();
        }
        resp_.set_parts(std::move(parts));
        onFinished();
    }

private:
    explicit GetPartsAllocProcessor(kvstore::KVStore* kvstore, folly::RWSpinLock* lock)
            : BaseProcessor<cpp2::GetPartsAllocResp>(kvstore, lock) {}

private:
    std::unique_ptr<folly::RWSpinLock::ReadHolder> rh_;
};

}  // namespace meta
}  // namespace nebula

#endif  // META_GETPARTSALLOCPROCESSOR_H_
