/* Copyright (c) 2019 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "meta/processors/indexMan/CreateTagIndexProcessor.h"

namespace nebula {
namespace meta {

void CreateTagIndexProcessor::process(const cpp2::CreateTagIndexReq& req) {
    auto space = req.get_space_id();
    CHECK_SPACE_ID_AND_RETURN(space);
    const auto &indexName = req.get_index_name();
    auto &properties = req.get_properties();
    if (properties.get_fields().empty()) {
        LOG(ERROR) << "Tag's Field should not empty";
        resp_.set_code(cpp2::ErrorCode::E_INVALID_PARM);
        onFinished();
        return;
    }

    folly::SharedMutex::WriteHolder wHolder(LockUtils::tagIndexLock());
    auto ret = getTagIndexID(space, indexName);
    if (ret.ok()) {
        LOG(ERROR) << "Create Tag Index Failed: " << indexName << " have existed";
        resp_.set_code(cpp2::ErrorCode::E_EXISTED);
        onFinished();
        return;
    }

    std::map<std::string, std::vector<nebula::cpp2::ColumnDef>> tagColumns;
    for (auto& element : properties.get_fields()) {
        auto tagName = element.first;
        auto tagID = getTagId(space, tagName);
        if (!tagID.ok()) {
            LOG(ERROR) << "Create Tag Index Failed: Tag " << tagName << " not exist";
            resp_.set_code(cpp2::ErrorCode::E_NOT_FOUND);
            onFinished();
            return;
        }

        auto fieldsResult = getLatestTagFields(space, tagName);
        if (!fieldsResult.ok()) {
            LOG(ERROR) << "Get Latest Tag Fields Failed";
            resp_.set_code(cpp2::ErrorCode::E_NOT_FOUND);
            onFinished();
            return;
        }

        auto fields = fieldsResult.value();
        std::vector<nebula::cpp2::ColumnDef> columns;
        for (auto &field : element.second) {
            auto iter = std::find_if(std::begin(fields), std::end(fields),
                                     [field](const auto& pair) {
                                         return field == pair.first;
                                     });
            if (iter == fields.end()) {
                LOG(ERROR) << "Field " << field << " not found in Tag " << tagName;
                resp_.set_code(cpp2::ErrorCode::E_NOT_FOUND);
                onFinished();
                return;
            } else {
                auto type = fields[field];
                nebula::cpp2::ColumnDef column;
                column.set_name(std::move(field));
                column.set_type(std::move(type));
                columns.emplace_back(std::move(column));
            }
        }
        tagColumns.emplace(tagName, std::move(columns));
    }

    nebula::meta::cpp2::IndexFields indexFields;
    indexFields.set_fields(std::move(tagColumns));

    std::vector<kvstore::KV> data;
    auto tagIndexRet = autoIncrementId();
    if (!nebula::ok(tagIndexRet)) {
        LOG(ERROR) << "Create tag index failed : Get tag index ID failed";
        resp_.set_code(nebula::error(tagIndexRet));
        onFinished();
        return;
    }

    auto tagIndex = nebula::value(tagIndexRet);
    data.emplace_back(MetaServiceUtils::indexTagIndexKey(space, indexName),
                      std::string(reinterpret_cast<const char*>(&tagIndex), sizeof(TagIndexID)));
    data.emplace_back(MetaServiceUtils::tagIndexKey(space, tagIndex),
                      MetaServiceUtils::tagIndexVal(indexName, std::move(indexFields)));
    LOG(INFO) << "Create Tag Index " << indexName << ", tagIndex " << tagIndex;
    resp_.set_id(to(tagIndex, EntryType::TAG_INDEX));
    doPut(std::move(data));
}

}  // namespace meta
}  // namespace nebula

