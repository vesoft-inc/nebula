/* Copyright (c) 2018 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "meta/processors/partsMan/GetPartsAllocProcessor.h"

namespace nebula {
namespace meta {

boost::optional<std::unordered_map<nebula::cpp2::HostAddr, std::string>>
GetPartsAllocProcessor::processDomains() {
    auto prefix = MetaServiceUtils::domainPrefix();
    std::unique_ptr<kvstore::KVIterator> iter;

    // first get domain
    auto ret = kvstore_->prefix(kDefaultSpaceId, kDefaultPartId, prefix, &iter);
    handleErrorCode(MetaCommon::to(ret));
    if (ret != kvstore::ResultCode::SUCCEEDED && ret != kvstore::ResultCode::ERR_KEY_NOT_FOUND) {
        LOG(WARNING) << "GetPartsAlloc processDomains failed, error: " << ret;
        return boost::none;
    }

    decltype(resp_.domains) domains;
    if (ret == kvstore::ResultCode::SUCCEEDED) {
        while (iter->valid()) {
            auto key = iter->key();
            domains.emplace(MetaServiceUtils::parseDomainVal(iter->val()),
                            MetaServiceUtils::parseDomainKey(key));
            iter->next();
        }
    }

    return domains;
}

boost::optional<std::unordered_map<PartitionID, std::vector<nebula::cpp2::HostAddr>>>
GetPartsAllocProcessor::processParts(GraphSpaceID spaceId) {
    auto prefix = MetaServiceUtils::partPrefix(spaceId);
    std::unique_ptr<kvstore::KVIterator> iter;

    auto ret = kvstore_->prefix(kDefaultSpaceId, kDefaultPartId, prefix, &iter);
    handleErrorCode(MetaCommon::to(ret));
    if (ret != kvstore::ResultCode::SUCCEEDED) {
        return boost::none;
    }

    decltype(resp_.parts) parts;
    while (iter->valid()) {
        auto key = iter->key();
        PartitionID partId;
        memcpy(&partId, key.data() + prefix.size(), sizeof(PartitionID));
        std::vector<nebula::cpp2::HostAddr> partHosts = MetaServiceUtils::parsePartVal(iter->val());
        parts.emplace(partId, std::move(partHosts));
        iter->next();
    }
    return parts;
}

void GetPartsAllocProcessor::process(const cpp2::GetPartsAllocReq& req) {
    folly::SharedMutex::ReadHolder rHolder(LockUtils::spaceLock());
    auto domainOpt = processDomains();
    if (domainOpt == boost::none) {
        onFinished();
        return;
    }

    auto partsOpt = processParts(req.get_space_id());
    if (partsOpt == boost::none) {
        onFinished();
        return;
    }
    resp_.set_parts(std::move(partsOpt.value()));
    resp_.set_domains(std::move(domainOpt.value()));
    onFinished();
}

}   // namespace meta
}   // namespace nebula
