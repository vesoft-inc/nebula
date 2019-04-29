/* Copyright (c) 2018 - present, VE Software Inc. All rights reserved
 *
 * This source code is licensed under Apache 2.0 License
 *  (found in the LICENSE.Apache file in the root directory)
 */

#include "meta/processors/RemoveTagProcessor.h"

namespace nebula {
namespace meta {

void RemoveTagProcessor::process(const cpp2::RemoveTagReq& req) {
    if (spaceExist(req.get_space_id()) == Status::NotFound()) {
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
    LOG(INFO) << "Remove Tag " << req.get_tag_name();
    doMultiRemove(std::move(ret.value()));
}

StatusOr<std::vector<std::string>> RemoveTagProcessor::getTagKeys(GraphSpaceID id,
                                                                  const std::string& tagName) {
    auto indexKey = MetaServiceUtils::tagIndexKey(id, tagName);
    TagID tagId;
    auto indexValue = doGet(indexKey);
    if (indexValue.ok()) {
        tagId = *reinterpret_cast<const TagID *>(indexValue.value().data());
        resp_.set_id(to(tagId, EntryType::TAG));
    } else {
        return Status::Error("No Tag!");
    }

    std::unique_ptr<kvstore::KVIterator> iter;
    auto ret = doScanKey(MetaServiceUtils::schemaTagKey(id, tagId, MIN_VERSION_HEX),
                         MetaServiceUtils::schemaTagKey(id, tagId, MAX_VERSION_HEX));
    if (!ret.ok()) {
        return Status::Error("Tag get error by id : %d !", tagId);
    }
    auto keys = ret.value();
    keys.emplace_back(indexKey);
    return keys;
}

}  // namespace meta
}  // namespace nebula

