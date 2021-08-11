/* Copyright (c) 2019 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "meta/processors/indexMan/ListTagIndexesProcessor.h"

namespace nebula {
namespace meta {

void ListTagIndexesProcessor::process(const cpp2::ListTagIndexesReq& req) {
    auto space = req.get_space_id();
    CHECK_SPACE_ID_AND_RETURN(space);

    folly::SharedMutex::ReadHolder rHolder(LockUtils::tagIndexLock());
    const auto& prefix = MetaServiceUtils::indexPrefix(space);
    auto iterRet = doPrefix(prefix);
    if (!nebula::ok(iterRet)) {
        auto retCode = nebula::error(iterRet);
        LOG(ERROR) << "List Tag Index Failed: SpaceID " << space
                   << " error: " << apache::thrift::util::enumNameSafe(retCode);
        handleErrorCode(retCode);
        onFinished();
        return;
    }

    auto iter = nebula::value(iterRet).get();
    std::vector<cpp2::IndexItem> items;
    while (iter->valid()) {
        auto val = iter->val();
        auto item = MetaServiceUtils::parseIndex(val);
        if (item.get_schema_id().getType() == cpp2::SchemaID::Type::tag_id) {
            items.emplace_back(std::move(item));
        }
        iter->next();
    }
    handleErrorCode(nebula::cpp2::ErrorCode::SUCCEEDED);
    resp_.set_items(std::move(items));
    onFinished();
}

}  // namespace meta
}  // namespace nebula

