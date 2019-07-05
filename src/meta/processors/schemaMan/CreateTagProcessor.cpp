/* Copyright (c) 2018 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "meta/processors/schemaMan/CreateTagProcessor.h"

namespace nebula {
namespace meta {

void CreateTagProcessor::process(const cpp2::CreateTagReq& req) {
    CHECK_SPACE_ID_AND_RETURN(req.get_space_id());
    folly::SharedMutex::WriteHolder wHolder(LockUtils::tagLock());
    auto ret = getTagId(req.get_space_id(), req.get_tag_name());
    std::vector<kvstore::KV> data;
    if (ret.ok()) {
        LOG(ERROR) << "Create Tag Failed :" << req.get_tag_name() << " have existed";
        resp_.set_id(to(ret.value(), EntryType::TAG));
        resp_.set_code(cpp2::ErrorCode::E_EXISTED);
        onFinished();
        return;
    }
    TagID tagId = autoIncrementId();
    data.emplace_back(MetaServiceUtils::indexTagKey(req.get_space_id(), req.get_tag_name()),
                      std::string(reinterpret_cast<const char*>(&tagId), sizeof(tagId)));
    LOG(INFO) << "Create Tag " << req.get_tag_name() << ", tagId " << tagId;
    data.emplace_back(MetaServiceUtils::schemaTagKey(req.get_space_id(), tagId, 0),
                      MetaServiceUtils::schemaTagVal(req.get_tag_name(), req.get_schema()));
    resp_.set_id(to(tagId, EntryType::TAG));
    doPut(std::move(data));
}

}  // namespace meta
}  // namespace nebula
