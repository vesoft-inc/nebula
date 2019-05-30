/* Copyright (c) 2019 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "meta/processors/CreateTagIndexProcessor.h"

namespace nebula {
namespace meta {

void CreateTagIndexProcessor::process(const cpp2::CreateTagIndexReq& req) {
    CHECK_SPACE_ID_AND_RETURN(req.get_space_id());
    folly::SharedMutex::WriteHolder wHolder(LockUtils::tagIndexLock());
    auto properties = req.get_properties();
    auto tagName = properties.get_tag_name();

    auto ret = getTagIndexID(req.get_space_id(), tagName);
    if (ret.ok()) {
        LOG(ERROR) << "Create Tag Index Failed "
                   << tagName << " already existed";
        resp_.set_id(to(ret.value(), EntryType::TAG_INDEX));
        resp_.set_code(cpp2::ErrorCode::E_EXISTED);
        onFinished();
        return;
    }

    ret = getTagId(req.get_space_id(), properties.get_tag_name());
    if (!ret.ok()) {
        LOG(ERROR) << "Create Tag Index Failed "
                   << properties.get_tag_name() << " not exist";
        resp_.set_id(to(ret.value(), EntryType::TAG));
        resp_.set_code(cpp2::ErrorCode::E_NOT_FOUND);
        onFinished();
        return;
    }

    std::vector<kvstore::KV> data;
    TagIndexID tagIndex = autoIncrementId();
    data.emplace_back(MetaServiceUtils::indexTagIndexKey(req.get_space_id(), tagName),
                      std::string(reinterpret_cast<const char*>(&tagIndex), sizeof(TagIndexID)));
    data.emplace_back(MetaServiceUtils::tagIndexKey(req.get_space_id(), tagIndex),
                      MetaServiceUtils::tagIndexVal(properties));
    LOG(INFO) << "Create Tag Index " << tagName << ", tagIndex " << tagIndex;
    resp_.set_code(cpp2::ErrorCode::SUCCEEDED);
    resp_.set_id(to(tagIndex, EntryType::TAG_INDEX));
    doPut(std::move(data));
}

}  // namespace meta
}  // namespace nebula

