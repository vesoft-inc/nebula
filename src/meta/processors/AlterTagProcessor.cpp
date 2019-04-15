/* Copyright (c) 2018 - present, VE Software Inc. All rights reserved
 *
 * This source code is licensed under Apache 2.0 License
 *  (found in the LICENSE.Apache file in the root directory)
 */

#include "meta/processors/AlterTagProcessor.h"
#include "time/TimeUtils.h"

namespace nebula {
namespace meta {

void AlterTagProcessor::process(const cpp2::WriteTagReq& req) {
    if (spaceExist(req.get_space_id()) == Status::SpaceNotFound()) {
        resp_.set_code(cpp2::ErrorCode::E_NOT_FOUND);
        onFinished();
        return;
    }
    folly::SharedMutex::WriteHolder wHolder(LockUtils::tagLock());
    auto ret = getElementId(EntryType::TAG, req.get_tag_name());
    if (!ret.ok()) {
        resp_.set_code(cpp2::ErrorCode::E_NOT_FOUND);
        onFinished();
        return;
    }
    auto tagId = ret.value();

    // Check the tag belongs to the space
    std::unique_ptr<kvstore::KVIterator> iter;
    auto code = kvstore_->prefix(kDefaultSpaceId_, kDefaultPartId_,
                                MetaUtils::schemaTagPrefix(req.get_space_id(), tagId),
                                &iter);
    if (code != kvstore::ResultCode::SUCCEEDED || !iter->valid()) {
        LOG(WARNING) << "Tag could not be found " << req.get_tag_name()
        << ", spaceId " << req.get_space_id() << ", tagId " << tagId;
        resp_.set_code(cpp2::ErrorCode::E_NOT_FOUND);
        onFinished();
        return;
    }

    std::vector<kvstore::KV> data;
    auto version = time::TimeUtils::nowInMSeconds();
    LOG(INFO) << "Alter Tag " << req.get_tag_name() << ", tagId " << tagId;
    data.emplace_back(MetaUtils::schemaTagKey(req.get_space_id(), tagId, version),
                      MetaUtils::schemaTagVal(req.get_tag_name(), req.get_schema()));
    resp_.set_code(cpp2::ErrorCode::SUCCEEDED);
    resp_.set_id(to(tagId, EntryType::TAG));
    doPut(std::move(data));
}

}  // namespace meta
}  // namespace nebula

