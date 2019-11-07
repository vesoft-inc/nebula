/* Copyright (c) 2019 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "meta/ActiveHostsMan.h"
#include "meta/processors/partsMan/ListPartsProcessor.h"
#include "meta/processors/admin/AdminClient.h"

namespace nebula {
namespace meta {

void ListPartsProcessor::process(const cpp2::ListPartsReq& req) {
    spaceId_ = req.get_space_id();
    std::unordered_map<PartitionID, std::vector<nebula::cpp2::HostAddr>> partHostsMap;
    {
        folly::SharedMutex::ReadHolder rHolder(LockUtils::spaceLock());
        auto status = getAllParts();
        if (!status.ok()) {
            onFinished();
            return;
        }
        partHostsMap = std::move(status.value());
    }
    std::vector<cpp2::PartItem> partItems;
    for (auto& partEntry : partHostsMap) {
        cpp2::PartItem partItem;
        partItem.set_part_id(partEntry.first);
        partItem.set_peers(std::move(partEntry.second));
        std::vector<nebula::cpp2::HostAddr> losts;
        for (auto& host : partItem.get_peers()) {
            if (!ActiveHostsMan::isLived(this->kvstore_, HostAddr(host.ip, host.port))) {
                losts.emplace_back();
                losts.back().set_ip(host.ip);
                losts.back().set_port(host.port);
            }
        }
        partItem.set_losts(std::move(losts));
        partItems.emplace_back(std::move(partItem));
    }
    if (partItems.size() != partHostsMap.size()) {
        LOG(ERROR) << "Maybe lost some partitions!";
    }
    getLeaderDist(partItems);
    resp_.set_parts(std::move(partItems));
    onFinished();
}


StatusOr<std::unordered_map<PartitionID, std::vector<nebula::cpp2::HostAddr>>>
ListPartsProcessor::getAllParts() {
    std::unordered_map<PartitionID, std::vector<nebula::cpp2::HostAddr>> partHostsMap;

    folly::SharedMutex::ReadHolder rHolder(LockUtils::spaceLock());
    auto prefix = MetaServiceUtils::partPrefix(spaceId_);
    std::unique_ptr<kvstore::KVIterator> iter;
    auto kvRet = kvstore_->prefix(kDefaultSpaceId, kDefaultPartId, prefix, &iter);
    if (kvRet != kvstore::ResultCode::SUCCEEDED) {
        LOG(ERROR) << "List Parts Failed: No parts";
        resp_.set_code(cpp2::ErrorCode::E_NOT_FOUND);
        return Status::Error("Can't access kvstore, ret = %d", static_cast<int32_t>(kvRet));
    }

    while (iter->valid()) {
        auto key = iter->key();
        PartitionID partId;
        memcpy(&partId, key.data() + prefix.size(), sizeof(PartitionID));
        std::vector<nebula::cpp2::HostAddr> partHosts = MetaServiceUtils::parsePartVal(iter->val());
        partHostsMap.emplace(partId, std::move(partHosts));
        iter->next();
    }

    return partHostsMap;
}


void ListPartsProcessor::getLeaderDist(std::vector<cpp2::PartItem>& partItems) {
    if (adminClient_ == nullptr) {
        return;
    }

    HostLeaderMap hostLeaderMap;
    auto ret = adminClient_->getLeaderDist(&hostLeaderMap).get();
    if (!ret.ok() && !hostLeaderMap.empty()) {
        LOG(ERROR) << "Get leader distribution failed";
        return;
    }

    for (auto& hostEntry : hostLeaderMap) {
        auto leader = hostEntry.first;
        auto spaceIter = hostEntry.second.find(spaceId_);
        if (spaceIter != hostEntry.second.end()) {
            for (auto& partId : spaceIter->second) {
                auto it = std::find_if(partItems.begin(), partItems.end(),
                    [&] (const auto& partItem) {
                        return partItem.part_id == partId;
                    });
                if (it != partItems.end()) {
                    it->set_leader(this->toThriftHost(leader));
                } else {
                    LOG(ERROR) << "Maybe not get the leader of partition " << partId;
                }
            }
        }
    }
}

}  // namespace meta
}  // namespace nebula

