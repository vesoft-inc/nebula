/* Copyright (c) 2018 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "meta/processors/RemoveTagProcessor.h"

namespace nebula {
namespace meta {

void RemoveTagProcessor::process(const cpp2::RemoveTagReq& req) {
    CHECK_SPACE_ID_AND_RETURN(req.get_space_id());
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
    auto indexKey = MetaServiceUtils::indexTagKey(id, tagName);
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
    auto key = MetaServiceUtils::schemaTagPrefix(id, tagId);
    ret = kvstore_->prefix(kDefaultSpaceId_, kDefaultPartId_, key, &iter);
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

