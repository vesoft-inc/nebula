/* Copyright (c) 2018 - present, VE Software Inc. All rights reserved
 *
 * This source code is licensed under Apache 2.0 License
 *  (found in the LICENSE.Apache file in the root directory)
 */

#include "meta/processors/RemoveTagProcessor.h"

namespace nebula {
namespace meta {

void RemoveTagProcessor::process(const cpp2::RemoveTagReq& req) {
    if (spaceExist(req.get_space_id()) == Status::SpaceNotFound()) {
        LOG(ERROR) << "Remove Tag Failed : Space " << req.get_space_id() << " not found";
        resp_.set_code(cpp2::ErrorCode::E_NOT_FOUND);
        onFinished();
        return;
    }
    folly::SharedMutex::WriteHolder wHolder(LockUtils::tagLock());
    auto ret = getTagKeys(req.get_space_id(), req.get_tag_name());
    if (!ret.ok()) {
        LOG(ERROR) << "Remove Tag Failed : " << req.get_tag_name() << " not found";
        resp_.set_code(cpp2::ErrorCode::E_NOT_FOUND);
        onFinished();
        return;
    }
    resp_.set_code(cpp2::ErrorCode::SUCCEEDED);
    LOG(INFO) << "Remove Tag " << req.get_tag_name();
    doMultiRemove(std::move(ret.value()));
}

StatusOr<std::vector<std::string>> RemoveTagProcessor::getTagKeys(GraphSpaceID id,
                                                                  const std::string& tagName) {
    auto indexKey = MetaServiceUtils::indexKey(EntryType::TAG, tagName);
    std::vector<std::string> keys;
    std::string tagVal;
    TagID tagId;
    auto ret = kvstore_->get(kDefaultSpaceId_, kDefaultPartId_, indexKey, &tagVal);
    if (ret == kvstore::ResultCode::SUCCEEDED) {
        tagId = *reinterpret_cast<const TagID *>(tagVal.data());
        resp_.set_id(to(tagId, EntryType::TAG));
        keys.emplace_back(indexKey);
    } else {
        return Status::Error("No Tag!");
    }

    std::unique_ptr<kvstore::KVIterator> iter;
    ret = kvstore_->range(kDefaultSpaceId_, kDefaultPartId_,
                          MetaServiceUtils::schemaTagKey(id, tagId, MIN_VERSION_HEX),
                          MetaServiceUtils::schemaTagKey(id, tagId, MAX_VERSION_HEX),
                          &iter);
    if (ret != kvstore::ResultCode::SUCCEEDED) {
        return Status::Error("Tag get error by id : %d !", tagId);
    }

    while (iter->valid()) {
        keys.emplace_back(iter->key());
        iter->next();
    }
    return keys;
}

}  // namespace meta
}  // namespace nebula

