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
    folly::SharedMutex::WriteHolder wHolder(LockUtils::tagIndexLock());
    auto properties = req.get_properties();
    auto indexName = properties.get_index_name();
    auto ret = getTagIndexID(spaceID, indexName);
    if (ret.ok()) {
        LOG(ERROR) << "Create Tag Index Failed: " << indexName << " already existed";
        resp_.set_id(to(ret.value(), EntryType::TAG_INDEX));
        resp_.set_code(cpp2::ErrorCode::E_EXISTED);
        onFinished();
        return;
    }

    if (properties.get_tag_fields().size() == 0) {
        resp_.set_id(to(ret.value(), EntryType::TAG_INDEX));
        resp_.set_code(cpp2::ErrorCode::E_INVALID_PARM);
        onFinished();
        return;
    }

    for (auto const &element : properties.get_tag_fields()) {
        ret = getTagId(spaceID, element.first);
        if (!ret.ok()) {
            LOG(ERROR) << "Create Tag Index Failed: Tag "
                       << element.first << " not exist";
            resp_.set_id(to(ret.value(), EntryType::TAG_INDEX));
            resp_.set_code(cpp2::ErrorCode::E_NOT_FOUND);
            onFinished();
            return;
        }

        auto propertyNames = getLatestTagPropertyNames(spaceID, indexName);
        if (propertyNames.ok()) {
            LOG(ERROR) << "";
        }
    }

    std::vector<kvstore::KV> data;
    TagIndexID tagIndex = autoIncrementId();
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

