/* Copyright (c) 2018 - present, VE Software Inc. All rights reserved
 *
 * This source code is licensed under Apache 2.0 License
 *  (found in the LICENSE.Apache file in the root directory)
 */

#ifndef META_CREATENODEPROCESSOR_H_
#define META_CREATENODEPROCESSOR_H_

#include "base/Base.h"
#include "meta/BaseProcessor.h"
#include "fs/FileUtils.h"

namespace nebula {
namespace meta {

class CreateNodeProcessor : public BaseProcessor<cpp2::ExecResponse> {
public:
    static CreateNodeProcessor* instance(kvstore::KVStore* kvstore, folly::RWSpinLock* lock) {
        return new CreateNodeProcessor(kvstore, lock);
    }

    void process(const cpp2::CreateNodeRequest& req) {
        if (!MetaUtils::pathValid(req.get_path())) {
            resp_.set_code(cpp2::ErrorCode::E_INVALID_PATH);
            VLOG(3) << "Invalid path:" << req.get_path();
            onFinished();
            return;
        }
        auto path = MetaUtils::normalize(req.get_path());
        auto layer = MetaUtils::layer(path);
        folly::StringPiece parent, child;
        fs::FileUtils::dividePath(path, parent, child);
        VLOG(3) << "DividePath, parent " << parent << ", child " << child
                << ", path " << path << ", layer " << std::to_string(layer);
        // Acquire the write lock.
        wh_ = std::make_unique<folly::RWSpinLock::WriteHolder>(*lock_);
        // Check the parent exists or not. If not existed, return failure.
        if (!parent.empty()) {
            auto parentKey = MetaUtils::metaKey(layer - 1,  parent);
            std::string value;
            auto ret = kvstore_->get(kDefaultSpaceId, kDefaultPartId, parentKey, &value);
            if (ret != kvstore::ResultCode::SUCCESSED) {
                resp_.set_code(to(ret));
                if (resp_.code == cpp2::ErrorCode::E_LEADER_CHANGED) {
                    // TODO: get current leader.
                }
                VLOG(3) << "Get parent failed! key: " << parent
                        << ", ret = " << static_cast<int32_t>(resp_.code);
                onFinished();
                return;
            }
        }
        // Check the node exists or not. If existed, return failure.
        auto key = MetaUtils::metaKey(layer, path);
        {
            std::string value;
            auto ret = kvstore_->get(kDefaultSpaceId, kDefaultPartId, key, &value);
            if (ret == kvstore::ResultCode::SUCCESSED) {
                resp_.set_code(cpp2::ErrorCode::E_NODE_EXISTED);
                VLOG(3) << "Node existed yet! key " << key;
                onFinished();
                return;
            }
            CHECK_EQ(kvstore::ResultCode::ERR_KEY_NOT_FOUND, ret);
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
    CreateNodeProcessor(kvstore::KVStore* kvstore, folly::RWSpinLock* lock)
            : BaseProcessor<cpp2::ExecResponse>(kvstore, lock) {}

private:
    std::unique_ptr<folly::RWSpinLock::WriteHolder> wh_;
};


}  // namespace meta
}  // namespace nebula
#endif  // META_CREATENODEPROCESSOR_H_
