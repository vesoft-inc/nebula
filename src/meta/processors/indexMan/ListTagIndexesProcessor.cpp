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
    auto prefix = MetaServiceUtils::indexPrefix(space);

    std::unique_ptr<kvstore::KVIterator> iter;
    auto ret = kvstore_->prefix(kDefaultSpaceId, kDefaultPartId, prefix, &iter);
    handleErrorCode(MetaCommon::to(ret));
    if (ret != kvstore::ResultCode::SUCCEEDED) {
        LOG(ERROR) << "List Tag Index Failed: SpaceID " << space;
        onFinished();
        return;
    }

    std::vector<nebula::meta::cpp2::IndexItem> items;
    while (iter->valid()) {
        auto val = iter->val();
        auto item = MetaServiceUtils::parseIndex(val);
        if (item.get_schema_id().getType() == cpp2::SchemaID::Type::tag_id) {
            items.emplace_back(std::move(item));
        }
        iter->next();
    }
    resp_.set_code(cpp2::ErrorCode::SUCCEEDED);
    resp_.set_items(std::move(items));
    onFinished();
}

}  // namespace meta
}  // namespace nebula

