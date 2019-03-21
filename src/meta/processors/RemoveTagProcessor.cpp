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
        resp_.set_code(cpp2::ErrorCode::E_NOT_FOUND);
        onFinished();
        return;
    }
    folly::SharedMutex::WriteHolder wHolder(LockUtils::tagLock());
    auto ret = getTagKeys(req.get_space_id(), req.get_tag_name());
    if (!ret.ok()) {
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
    TagID tagId;
    auto getRes = kvstore_->get(kDefaultSpaceId_, kDefaultPartId_, indexKey);
    if (ok(getRes)) {
        std::string tagVal = value(std::move(getRes));
        tagId = *reinterpret_cast<const TagID *>(tagVal.data());
        resp_.set_id(to(tagId, EntryType::TAG));
        keys.emplace_back(indexKey);
    } else {
        return Status::Error("No Tag!");
    }

    auto key = MetaServiceUtils::schemaTagPrefix(id, tagId);
    auto prefixRes = kvstore_->prefix(kDefaultSpaceId_, kDefaultPartId_, key);
    if (!ok(prefixRes)) {
        return Status::Error("Tag get error by id : %d !", tagId);
    }

    std::unique_ptr<kvstore::KVIterator> iter = value(std::move(prefixRes));
    while (iter->valid()) {
        keys.emplace_back(iter->key());
        iter->next();
    }
    return keys;
}

}  // namespace meta
}  // namespace nebula

