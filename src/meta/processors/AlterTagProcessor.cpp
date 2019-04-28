/* Copyright (c) 2018 - present, VE Software Inc. All rights reserved
 *
 * This source code is licensed under Apache 2.0 License
 *  (found in the LICENSE.Apache file in the root directory)
 */

#include "meta/processors/AlterTagProcessor.h"
#include "time/TimeUtils.h"

namespace nebula {
namespace meta {

void AlterTagProcessor::process(const cpp2::AlterTagReq& req) {
    if (spaceExist(req.get_space_id()) == Status::SpaceNotFound()) {
        resp_.set_code(cpp2::ErrorCode::E_NOT_FOUND);
        onFinished();
        return;
    }
    folly::SharedMutex::WriteHolder wHolder(LockUtils::tagLock());
    auto ret = getTagId(req.get_space_id(), req.get_tag_name());
    if (!ret.ok()) {
        resp_.set_code(to(ret.status()));
        onFinished();
        return;
    }
    auto tagId = ret.value();

    // Check the tag belongs to the space
    std::unique_ptr<kvstore::KVIterator> iter;
    auto code = kvstore_->prefix(kDefaultSpaceId_, kDefaultPartId_,
                                 MetaServiceUtils::schemaTagPrefix(req.get_space_id(), tagId),
                                 &iter);
    if (code != kvstore::ResultCode::SUCCEEDED || !iter->valid()) {
        LOG(WARNING) << "Tag could not be found " << req.get_tag_name()
        << ", spaceId " << req.get_space_id() << ", tagId " << tagId;
        resp_.set_code(cpp2::ErrorCode::E_NOT_FOUND);
        onFinished();
        return;
    }

    // Get lasted version of tag
    auto version = MetaServiceUtils::parseTagVersion(iter->key()) + 1;
    auto schema = MetaServiceUtils::parseSchema(iter->val());
    auto columns = schema.get_columns();
    auto& tagItems = req.get_tag_items();
    for (auto& tagItem : tagItems) {
        auto& cols = tagItem.get_schema().get_columns();
        for (auto& col : cols) {
            auto retCode = alterColumnDefs(columns, col, tagItem.op);
            if (retCode != cpp2::ErrorCode::SUCCEEDED) {
                LOG(WARNING) << "Alter tag error";
                resp_.set_code(retCode);
                onFinished();
                return;
            }
        }
    }
    schema.set_columns(std::move(columns));
    std::vector<kvstore::KV> data;
    LOG(INFO) << "Alter Tag " << req.get_tag_name() << ", tagId " << tagId;
    data.emplace_back(MetaServiceUtils::schemaTagKey(req.get_space_id(), tagId, version),
                      MetaServiceUtils::schemaTagVal(req.get_tag_name(), schema));
    resp_.set_code(cpp2::ErrorCode::SUCCEEDED);
    resp_.set_id(to(tagId, EntryType::TAG));
    doPut(std::move(data));
}

cpp2::ErrorCode AlterTagProcessor::alterColumnDefs(std::vector<nebula::cpp2::ColumnDef>& cols,
                                                   const nebula::cpp2::ColumnDef col,
                                                   const cpp2::AlterTagOp op) {
    switch (op) {
        case cpp2::AlterTagOp::ADD :
        {
            for (auto it = cols.begin(); it != cols.end(); ++it) {
                if (it->get_name() == col.get_name()) {
                    LOG(WARNING) << "Tag column existing : " << col.get_name();
                    return cpp2::ErrorCode::E_EXISTED;
                }
            }
            cols.push_back(std::move(col));
            return cpp2::ErrorCode::SUCCEEDED;
        }
        case cpp2::AlterTagOp::SET :
        {
            for (auto it = cols.begin(); it != cols.end(); ++it) {
                if (col.get_name() == it->get_name()) {
                    *it = col;
                    return cpp2::ErrorCode::SUCCEEDED;
                }
            }
            break;
        }
        case cpp2::AlterTagOp::DROP :
        {
            for (auto it = cols.begin(); it != cols.end(); ++it) {
                if (col.get_name() == it->get_name()) {
                    cols.erase(it);
                    return cpp2::ErrorCode::SUCCEEDED;
                }
            }
            break;
        }
        default :
            return cpp2::ErrorCode::E_UNKNOWN;
    }
    LOG(WARNING) << "Tag column not found : " << col.get_name();
    return cpp2::ErrorCode::E_NOT_FOUND;
}


}  // namespace meta
}  // namespace nebula

