/* Copyright (c) 2019 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "meta/processors/indexMan/CreateTagIndexProcessor.h"

namespace nebula {
namespace meta {

void CreateTagIndexProcessor::process(const cpp2::CreateTagIndexReq& req) {
    auto spaceID = req.get_space_id();
    CHECK_SPACE_ID_AND_RETURN(spaceID);
    const auto &indexName = req.get_index_name();
    const auto &properties = req.get_properties();
    if (properties.get_fields().empty()) {
        LOG(ERROR) << "Tag's Field should not empty";
        resp_.set_code(cpp2::ErrorCode::E_INVALID_PARM);
        onFinished();
        return;
    }

    folly::SharedMutex::WriteHolder wHolder(LockUtils::tagIndexLock());
    auto ret = getTagIndexID(spaceID, indexName);
    if (ret.ok()) {
        LOG(ERROR) << "Create Tag Index Failed: " << indexName << " have existed";
        resp_.set_code(cpp2::ErrorCode::E_EXISTED);
        onFinished();
        return;
    }

    for (auto const &element : properties.get_fields()) {
        auto tagName = element.first;
        auto tagID = getTagId(spaceID, tagName);
        if (!tagID.ok()) {
            LOG(ERROR) << "Create Tag Index Failed: Tag " << tagName << " not exist";
            resp_.set_code(cpp2::ErrorCode::E_NOT_FOUND);
            onFinished();
            return;
        }

        auto fieldsResult = getLatestTagFields(spaceID, tagName);
        if (!fieldsResult.ok()) {
            LOG(ERROR) << "Get Latest Tag Fields Failed";
            resp_.set_code(cpp2::ErrorCode::E_NOT_FOUND);
            onFinished();
            return;
        }
        auto fields = fieldsResult.value();
        for (auto &field : element.second) {
            if (std::find(fields.begin(), fields.end(), field) == fields.end()) {
                LOG(ERROR) << "Field " << field << " not found in Tag " << tagName;
                resp_.set_code(cpp2::ErrorCode::E_NOT_FOUND);
                onFinished();
                return;
            }
        }
    }

    std::vector<kvstore::KV> data;
    auto tagIndexRet = autoIncrementId();
    if (!nebula::ok(tagIndexRet)) {
        LOG(ERROR) << "Create tag index failed : Get tag index ID failed";
        resp_.set_code(nebula::error(tagIndexRet));
        onFinished();
        return;
    }

    auto tagIndex = nebula::value(tagIndexRet);
    data.emplace_back(MetaServiceUtils::indexTagIndexKey(spaceID, indexName),
                      std::string(reinterpret_cast<const char*>(&tagIndex), sizeof(TagIndexID)));
    data.emplace_back(MetaServiceUtils::tagIndexKey(spaceID, tagIndex),
                      MetaServiceUtils::tagIndexVal(properties));
    LOG(INFO) << "Create Tag Index " << indexName << ", tagIndex " << tagIndex;
    resp_.set_id(to(tagIndex, EntryType::TAG_INDEX));
    doPut(std::move(data));
}

}  // namespace meta
}  // namespace nebula

