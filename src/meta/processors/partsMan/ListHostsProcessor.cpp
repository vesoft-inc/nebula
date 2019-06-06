/* Copyright (c) 2018 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "meta/processors/partsMan/ListHostsProcessor.h"

namespace nebula {
namespace meta {

void ListHostsProcessor::process(const cpp2::ListHostsReq& req) {
    UNUSED(req);
    folly::SharedMutex::ReadHolder rHolder(LockUtils::spaceLock());
    auto ret = allHostsWithStatus();
    if (!ret.ok()) {
        LOG(ERROR) << "List Hosts Failed : No hosts";
        resp_.set_code(cpp2::ErrorCode::E_NO_HOSTS);
        onFinished();
        return;
    }
    resp_.set_hosts(std::move(ret.value()));
    onFinished();
}

StatusOr<std::vector<cpp2::HostItem>> ListHostsProcessor::allHostsWithStatus() {
    std::vector<cpp2::HostItem> hostItems;
    const auto& prefix = MetaServiceUtils::hostPrefix();
    std::unique_ptr<kvstore::KVIterator> iter;
    auto ret = kvstore_->prefix(kDefaultSpaceId, kDefaultPartId, prefix, &iter);
    if (ret != kvstore::ResultCode::SUCCEEDED) {
        return Status::Error("Can't find any hosts");
    }

    while (iter->valid()) {
        cpp2::HostItem item;
        nebula::cpp2::HostAddr host;
        auto hostAddrPiece = iter->key().subpiece(prefix.size());
        memcpy(&host, hostAddrPiece.data(), hostAddrPiece.size());
        item.set_hostAddr(host);
        if (iter->val() == MetaServiceUtils::hostValOnline()) {
            item.set_status(cpp2::HostStatus::ONLINE);
        } else {
            item.set_status(cpp2::HostStatus::OFFLINE);
        }
        hostItems.emplace_back(item);
        iter->next();
    }

    return hostItems;
}


}  // namespace meta
}  // namespace nebula

