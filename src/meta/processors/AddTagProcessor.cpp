/* Copyright (c) 2018 - present, VE Software Inc. All rights reserved
 *
 * This source code is licensed under Apache 2.0 License
 *  (found in the LICENSE.Apache file in the root directory)
 */

#include "meta/processors/AddTagProcessor.h"
#include "time/TimeUtils.h"

namespace nebula {
namespace meta {

void AddTagProcessor::process(const cpp2::AddTagReq& req) {
    if (spaceExist(req.get_space_id()) == Status::SpaceNotFound()) {
        resp_.set_code(cpp2::ErrorCode::E_NOT_FOUND);
        onFinished();
        return;
    }
    folly::SharedMutex::WriteHolder wHolder(LockUtils::tagLock());
    auto ret = getTag(req.get_tag_name());
    std::vector<kvstore::KV> data;
    if (ret.ok()) {
        resp_.set_id(to(ret.value(), EntryType::TAG));
        resp_.set_code(cpp2::ErrorCode::E_EXISTED);
        onFinished();
        return;
    }
    auto version = time::TimeUtils::nowInMSeconds();
    TagID tagId = autoIncrementId();
    data.emplace_back(MetaServiceUtils::indexKey(EntryType::TAG, req.get_tag_name()),
                      std::string(reinterpret_cast<const char*>(&tagId), sizeof(tagId)));
    LOG(INFO) << "Add Tag " << req.get_tag_name() << ", tagId " << tagId;
    data.emplace_back(MetaServiceUtils::schemaTagKey(req.get_space_id(), tagId, version),
                      MetaServiceUtils::schemaTagVal(req.get_tag_name(), req.get_schema()));
    resp_.set_code(cpp2::ErrorCode::SUCCEEDED);
    resp_.set_id(to(tagId, EntryType::TAG));
    doPut(std::move(data));
}

StatusOr<TagID> AddTagProcessor::getTag(const std::string& tagName) {
    auto indexKey = MetaServiceUtils::indexKey(EntryType::TAG, tagName);
    std::string val;
    auto ret = kvstore_->get(kDefaultSpaceId_, kDefaultPartId_, indexKey, &val);
    if (ret == kvstore::ResultCode::SUCCEEDED) {
        return *reinterpret_cast<const TagID*>(val.c_str());
    }
    return Status::Error("No Tag!");
}

}  // namespace meta
}  // namespace nebula
