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
    if (!spaceExist(req.get_space_id())) {
        resp_.set_code(cpp2::ErrorCode::E_NOT_FOUND);
        onFinished();
        return;
    }
    folly::RWSpinLock::WriteHolder wHolder(LockUtils::tagLock());
    TagID tagId = -1;
    auto ret = findTag(req.get_tag_name());
    auto version = time::TimeUtils::nowInMSeconds();
    std::vector<kvstore::KV> data;
    if (ret.ok()) {
        LOG(INFO) << "Tag " << req.get_tag_name() << " has been existed!";
        tagId = ret.value();
    } else {
        tagId = autoIncrementId();
        LOG(INFO) << "This is a new Tag:" << req.get_tag_name();
        data.emplace_back(MetaUtils::indexKey(EntryType::TAG, req.get_tag_name()),
                          std::string(reinterpret_cast<const char*>(&tagId), sizeof(tagId)));
    }
    LOG(INFO) << "Add Tag " << req.get_tag_name() << ", tagId " << tagId;
    data.emplace_back(MetaUtils::schemaTagKey(req.get_space_id(), tagId, version),
                      MetaUtils::schemaTagVal(req.get_schema()));
    resp_.set_code(cpp2::ErrorCode::SUCCEEDED);
    resp_.set_id(to(tagId, EntryType::TAG));
    doPut(std::move(data));
}

StatusOr<TagID> AddTagProcessor::findTag(const std::string& tagName) {
    auto indexKey = MetaUtils::indexKey(EntryType::TAG, tagName);
    std::string val;
    auto ret = kvstore_->get(kDefaultSpaceId_, kDefaultPartId_, indexKey, &val);
    if (ret == kvstore::ResultCode::SUCCEEDED) {
        try {
            return folly::to<TagID>(val);
        } catch (std::exception& e) {
            LOG(ERROR) << "Convert failed for " << val;
        }
    }
    return Status::Error("No Tag!");
}

}  // namespace meta
}  // namespace nebula
