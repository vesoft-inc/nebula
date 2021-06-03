/* Copyright (c) 2021 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "meta/processors/admin/ListClusterInfoProcessor.h"

namespace nebula {
namespace meta {

void ListClusterInfoProcessor::process(const cpp2::ListClusterInfoReq& req) {
    UNUSED(req);
    auto* store = dynamic_cast<kvstore::NebulaStore*>(kvstore_);
    if (store == nullptr) {
        onFinished();
        return;
    }

    if (!store->isLeader(kDefaultSpaceId, kDefaultPartId)) {
        handleErrorCode(nebula::cpp2::ErrorCode::E_LEADER_CHANGED);
        onFinished();
        return;
    }

    const auto& prefix = MetaServiceUtils::hostPrefix();
    std::unique_ptr<kvstore::KVIterator> iter;
    auto ret = kvstore_->prefix(kDefaultSpaceId, kDefaultPartId, prefix, &iter, true);
    if (ret != nebula::cpp2::ErrorCode::SUCCEEDED) {
        LOG(ERROR) << "prefix failed:" << apache::thrift::util::enumNameSafe(ret);
        handleErrorCode(nebula::cpp2::ErrorCode::E_LIST_CLUSTER_FAILURE);
        onFinished();
        return;
    }

    std::vector<nebula::cpp2::NodeInfo> storages;
    while (iter->valid()) {
        auto host = MetaServiceUtils::parseHostKey(iter->key());
        HostInfo info = HostInfo::decode(iter->val());

        if (info.role_ != cpp2::HostRole::STORAGE) {
            iter->next();
            continue;
        }

        auto status = client_->listClusterInfo(host).get();
        if (!status.ok()) {
            LOG(ERROR) << "listcluster info from storage failed, host: " << host;
            handleErrorCode(nebula::cpp2::ErrorCode::E_LIST_CLUSTER_FAILURE);
            onFinished();
            return;
        }

        storages.emplace_back(
            apache::thrift::FragileConstructor(), std::move(host), status.value());
        iter->next();
    }

    auto* pm = store->partManager();
    auto* mpm = dynamic_cast<nebula::kvstore::MemPartManager*>(pm);
    if (mpm == nullptr) {
        resp_.set_code(nebula::cpp2::ErrorCode::E_LIST_CLUSTER_FAILURE);
        onFinished();
        return;
    }
    auto& map = mpm->partsMap();
    auto hosts = map[nebula::meta::kDefaultSpaceId][nebula::meta::kDefaultPartId].hosts_;
    LOG(INFO) << "meta servers count: " << hosts.size();
    resp_.set_meta_servers(std::move(hosts));

    resp_.set_code(nebula::cpp2::ErrorCode::SUCCEEDED);
    resp_.set_storage_servers(std::move(storages));
    onFinished();
}
}   // namespace meta
}   // namespace nebula
