/* Copyright (c) 2019 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "meta/processors/indexMan/ListEdgeIndexesProcessor.h"

namespace nebula {
namespace meta {

void ListEdgeIndexesProcessor::process(const cpp2::ListEdgeIndexesReq& req) {
    CHECK_SPACE_ID_AND_RETURN(req.get_space_id());
    folly::SharedMutex::ReadHolder rHolder(LockUtils::edgeIndexLock());
    auto spaceId = req.get_space_id();
    auto prefix = MetaServiceUtils::indexPrefix(spaceId);

    std::unique_ptr<kvstore::KVIterator> iter;
    auto ret = kvstore_->prefix(kDefaultSpaceId, kDefaultPartId, prefix, &iter);
    resp_.set_code(to(ret));
    if (kvstore::ResultCode::SUCCEEDED != ret) {
        LOG(ERROR) << "List Edge Index Failed: SpaceID " << req.get_space_id() << ", ErrorCode is " << ret;
        onFinished();
        return;
    }

    decltype(resp_.items) items;
    while (iter->valid()) {
        auto val = iter->val();
        auto item = MetaServiceUtils::parseIndex(val);
        if (item.get_schema_id().getType() == nebula::cpp2::SchemaID::Type::edge_type) {
            items.emplace_back(std::move(item));
        }
        iter->next();
    }
    resp_.set_items(std::move(items));
    onFinished();
}

}  // namespace meta
}  // namespace nebula

