/* Copyright (c) 2018 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "meta/processors/schemaMan/AlterTagProcessor.h"
#include "time/TimeUtils.h"

namespace nebula {
namespace meta {

void AlterTagProcessor::process(const cpp2::AlterTagReq& req) {
    auto spaceId = req.get_space_id();
    CHECK_SPACE_ID_AND_RETURN(spaceId);
    folly::SharedMutex::WriteHolder wHolder(LockUtils::tagLock());
    auto ret = getTagId(spaceId, req.get_tag_name());
    if (!ret.ok()) {
        resp_.set_code(to(ret.status()));
        onFinished();
        return;
    }
    auto tagId = ret.value();

    // Check the tag belongs to the space
    std::unique_ptr<kvstore::KVIterator> iter;
    auto tagPrefix = MetaServiceUtils::schemaTagPrefix(spaceId, tagId);
    auto code = kvstore_->prefix(kDefaultSpaceId, kDefaultPartId, tagPrefix, &iter);
    if (code != kvstore::ResultCode::SUCCEEDED || !iter->valid()) {
        LOG(WARNING) << "Tag could not be found " << req.get_tag_name()
                     << ", spaceId " << spaceId
                     << ", tagId " << tagId;
        resp_.set_code(cpp2::ErrorCode::E_NOT_FOUND);
        onFinished();
        return;
    }

    // Get the last version of the tag
    auto version = MetaServiceUtils::parseTagVersion(iter->key()) + 1;
    auto schema = MetaServiceUtils::parseSchema(iter->val());
    auto columns = schema.get_columns();
    auto& tagItems = req.get_tag_items();
    for (auto& tagItem : tagItems) {
        auto& cols = tagItem.get_schema().get_columns();
        for (auto& col : cols) {
            auto retCode = MetaServiceUtils::alterColumnDefs(columns, col, tagItem.op);
            if (retCode != cpp2::ErrorCode::SUCCEEDED) {
                LOG(WARNING) << "Alter tag error " << static_cast<int32_t>(retCode);
                resp_.set_code(retCode);
                onFinished();
                return;
            }
        }
    }
    schema.set_columns(std::move(columns));
    std::vector<kvstore::KV> data;
    LOG(INFO) << "Alter Tag " << req.get_tag_name() << ", tagId " << tagId;
    data.emplace_back(MetaServiceUtils::schemaTagKey(spaceId, tagId, version),
                      MetaServiceUtils::schemaTagVal(req.get_tag_name(), schema));
    data.emplace_back(MetaServiceUtils::schemaLatestTagKey(spaceId, tagId),
                      MetaServiceUtils::schemaTagVal(req.get_tag_name(), schema));
    resp_.set_id(to(tagId, EntryType::TAG));
    doPut(std::move(data));
}


}  // namespace meta
}  // namespace nebula

