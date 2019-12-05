/* Copyright (c) 2019 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */


#include <common/fs/FileUtils.h>
#include "meta/processors/admin/ListSnapshotsProcessor.h"

namespace nebula {
namespace meta {

void ListSnapshotsProcessor::process(const cpp2::ListSnapshotsReq& req) {
    UNUSED(req);
    auto prefix = MetaServiceUtils::snapshotPrefix();
    std::unique_ptr<kvstore::KVIterator> iter;
    auto ret = kvstore_->prefix(kDefaultSpaceId, kDefaultPartId, prefix, &iter);
    if (ret != kvstore::ResultCode::SUCCEEDED) {
        resp_.set_code(to(ret));
        onFinished();
        return;
    }
    decltype(resp_.snapshots) snapshots;
    while (iter->valid()) {
        auto name = MetaServiceUtils::parseSnapshotName(iter->key());
        auto status = MetaServiceUtils::parseSnapshotStatus(iter->val());
        auto hosts = MetaServiceUtils::parseSnapshotHosts(iter->val());
        snapshots.emplace_back(apache::thrift::FragileConstructor::FRAGILE,
                               std::move(name),
                               std::move(status),
                               std::move(hosts));
        iter->next();
    }
    resp_.set_snapshots(std::move(snapshots));
    onFinished();
}
}  // namespace meta
}  // namespace nebula
