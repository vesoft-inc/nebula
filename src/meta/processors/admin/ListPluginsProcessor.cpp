/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */


#include <common/fs/FileUtils.h>
#include "meta/processors/admin/ListPluginsProcessor.h"

namespace nebula {
namespace meta {

void ListPluginsProcessor::process(const cpp2::ListPluginsReq& req) {
    UNUSED(req);
    folly::SharedMutex::ReadHolder rHolder(LockUtils::pluginLock());

    auto prefix = MetaServiceUtils::pluginPrefix();
    std::unique_ptr<kvstore::KVIterator> iter;
    auto ret = kvstore_->prefix(kDefaultSpaceId, kDefaultPartId, prefix, &iter);
    if (ret != kvstore::ResultCode::SUCCEEDED) {
        handleErrorCode(MetaCommon::to(ret));
        onFinished();
        return;
    }
    decltype(resp_.items) pluginItems;
    while (iter->valid()) {
        cpp2::PluginItem item = MetaServiceUtils::parsePluginItem(iter->val());
        pluginItems.emplace_back(std::move(item));
        iter->next();
    }
    resp_.set_items(std::move(pluginItems));
    onFinished();
}
}  // namespace meta
}  // namespace nebula
