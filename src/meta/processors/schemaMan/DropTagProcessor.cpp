/* Copyright (c) 2018 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "meta/processors/schemaMan/DropTagProcessor.h"

namespace nebula {
namespace meta {

void DropTagProcessor::process(const cpp2::DropTagReq& req) {
    CHECK_SPACE_ID_AND_RETURN(req.get_space_id());
    folly::SharedMutex::WriteHolder wHolder(LockUtils::tagLock());
    auto ret = getTagKeys(req.get_space_id(), req.get_tag_name());
    if (!ret.ok()) {
        LOG(ERROR) << "Drop Tag Failed : " << req.get_tag_name() << " not found";
        resp_.set_code(cpp2::ErrorCode::E_NOT_FOUND);
        onFinished();
        return;
    }
    resp_.set_code(cpp2::ErrorCode::SUCCEEDED);
    LOG(INFO) << "Drop Tag " << req.get_tag_name();
    doMultiRemove(std::move(ret.value()));
}

StatusOr<std::vector<std::string>> DropTagProcessor::getTagKeys(GraphSpaceID id,
                                                                  const std::string& tagName) {
    auto indexKey = MetaServiceUtils::indexTagKey(id, tagName);
    std::vector<std::string> keys;
    std::string tagVal;
    TagID tagId;
    auto ret = kvstore_->get(kDefaultSpaceId, kDefaultPartId, indexKey, &tagVal);
    if (ret == kvstore::ResultCode::SUCCEEDED) {
        tagId = *reinterpret_cast<const TagID *>(tagVal.data());
        resp_.set_id(to(tagId, EntryType::TAG));
        keys.emplace_back(indexKey);
    } else {
        return Status::Error("No Tag!");
    }

    std::unique_ptr<kvstore::KVIterator> iter;
    auto key = MetaServiceUtils::schemaTagPrefix(id, tagId);
    ret = kvstore_->prefix(kDefaultSpaceId, kDefaultPartId, key, &iter);
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


