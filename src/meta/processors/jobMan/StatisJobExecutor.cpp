/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "meta/common/MetaCommon.h"
#include "meta/MetaServiceUtils.h"
#include "meta/processors/jobMan/StatisJobExecutor.h"
#include "meta/processors/Common.h"
#include "utils/Utils.h"

namespace nebula {
namespace meta {

bool StatisJobExecutor::check() {
    // Only one parameter, the current space name
    return paras_.size() == 1;
}

nebula::cpp2::ErrorCode
StatisJobExecutor::save(const std::string& key, const std::string& val) {
    std::vector<kvstore::KV> data{std::make_pair(key, val)};
    folly::Baton<true, std::atomic> baton;
    auto rc = nebula::cpp2::ErrorCode::SUCCEEDED;
    kvstore_->asyncMultiPut(kDefaultSpaceId, kDefaultPartId, std::move(data),
                            [&] (nebula::cpp2::ErrorCode code) {
                                rc = code;
                                baton.post();
                            });
    baton.wait();
    return rc;
}

nebula::cpp2::ErrorCode StatisJobExecutor::doRemove(const std::string& key) {
    folly::Baton<true, std::atomic> baton;
    auto rc = nebula::cpp2::ErrorCode::SUCCEEDED;
    kvstore_->asyncRemove(kDefaultSpaceId, kDefaultPartId, key,
                          [&](nebula::cpp2::ErrorCode code) {
                              rc = code;
                              baton.post();
                          });
    baton.wait();
    return rc;
}

nebula::cpp2::ErrorCode StatisJobExecutor::prepare() {
    auto spaceRet = getSpaceIdFromName(paras_[0]);
    if (!nebula::ok(spaceRet)) {
        LOG(ERROR) << "Can't find the space: " << paras_[0];
        return nebula::error(spaceRet);
    }
    space_ = nebula::value(spaceRet);

    // Set the status of the statis job to running
    cpp2::StatisItem statisItem;
    statisItem.set_status(cpp2::JobStatus::RUNNING);
    auto statisKey = MetaServiceUtils::statisKey(space_);
    auto statisVal = MetaServiceUtils::statisVal(statisItem);
    return save(statisKey, statisVal);
}

folly::Future<Status>
StatisJobExecutor::executeInternal(HostAddr&& address, std::vector<PartitionID>&& parts) {
    cpp2::StatisItem item;
    statisItem_.emplace(address, item);
    return adminClient_->addTask(cpp2::AdminCmd::STATS, jobId_, taskId_++,
                                 space_, {std::move(address)}, {},
                                 std::move(parts), concurrency_, &(statisItem_[address]));
}

void showStatisItem(const cpp2::StatisItem& item, const std::string& msg) {
    std::stringstream oss;
    oss << msg << ": ";
    oss << "tag_vertices: ";
    for (auto& it : *item.tag_vertices_ref()) {
        oss << folly::sformat("[{}, {}] ", it.first, it.second);
    }
    oss << ", edges: ";
    for (auto& it : *item.edges_ref()) {
        oss << folly::sformat("[{}, {}] ", it.first, it.second);
    }
    oss << folly::sformat(", space_vertices={}", *item.space_vertices_ref());
    oss << folly::sformat(", space_edges={}", *item.space_edges_ref());
    LOG(INFO) << oss.str();
}

void StatisJobExecutor::addStatis(cpp2::StatisItem& lhs, const cpp2::StatisItem& rhs) {
    for (auto& it : *rhs.tag_vertices_ref()) {
        (*lhs.tag_vertices_ref())[it.first] += it.second;
    }

    for (auto& it : *rhs.edges_ref()) {
        (*lhs.edges_ref())[it.first] += it.second;
    }

    *lhs.space_vertices_ref() += *rhs.space_vertices_ref();
    *lhs.space_edges_ref() += *rhs.space_edges_ref();

    (*lhs.positive_part_correlativity_ref()).insert((*rhs.positive_part_correlativity_ref()).begin(),   // NOLINT
                                           (*rhs.positive_part_correlativity_ref()).end());
    (*lhs.negative_part_correlativity_ref()).insert((*rhs.negative_part_correlativity_ref()).begin(),   // NOLINT
                                           (*rhs.negative_part_correlativity_ref()).end());
}

/**
 * @brief caller will guarantee there won't be any conflict read / write.
 */
nebula::cpp2::ErrorCode
StatisJobExecutor::saveSpecialTaskStatus(const cpp2::ReportTaskReq& req) {
    if (!req.statis_ref().has_value()) {
        return nebula::cpp2::ErrorCode::SUCCEEDED;
    }
    cpp2::StatisItem statisItem;
    auto statisKey = MetaServiceUtils::statisKey(space_);
    auto tempKey = toTempKey(req.get_job_id());
    std::string val;
    auto ret = kvstore_->get(kDefaultSpaceId, kDefaultPartId, tempKey, &val);

    if (ret != nebula::cpp2::ErrorCode::SUCCEEDED) {
        if (ret != nebula::cpp2::ErrorCode::E_KEY_NOT_FOUND) {
            return ret;
        }
        ret = kvstore_->get(kDefaultSpaceId, kDefaultPartId, statisKey, &val);
    }

    if (ret != nebula::cpp2::ErrorCode::SUCCEEDED) {
        return ret;
    }

    statisItem = MetaServiceUtils::parseStatisVal(val);
    addStatis(statisItem, *req.statis_ref());
    auto statisVal = MetaServiceUtils::statisVal(statisItem);
    return save(tempKey, statisVal);
}

/**
 * @brief
 *      if two stats job run at the same time.
 *      (this may happens if leader changed)
 *      they will write to the same kv data
 *      so separate the partial result by job
 * @return std::string
 */
std::string StatisJobExecutor::toTempKey(int32_t jobId) {
    std::string key = MetaServiceUtils::statisKey(space_);;
    return key.append(reinterpret_cast<const char*>(&jobId), sizeof(int32_t));
}

nebula::cpp2::ErrorCode StatisJobExecutor::finish(bool exeSuccessed) {
    auto statisKey = MetaServiceUtils::statisKey(space_);
    auto tempKey = toTempKey(jobId_);
    std::string val;
    auto ret = kvstore_->get(kDefaultSpaceId, kDefaultPartId, tempKey, &val);
    if (ret != nebula::cpp2::ErrorCode::SUCCEEDED) {
        LOG(ERROR) << "Can't find the statis data, spaceId : " << space_;
        return ret;
    }
    auto statisItem = MetaServiceUtils::parseStatisVal(val);
    if (exeSuccessed) {
        statisItem.set_status(cpp2::JobStatus::FINISHED);
    } else {
        statisItem.set_status(cpp2::JobStatus::FAILED);
    }
    auto statisVal = MetaServiceUtils::statisVal(statisItem);
    auto retCode = save(statisKey, statisVal);
    if (retCode != nebula::cpp2::ErrorCode::SUCCEEDED) {
        LOG(ERROR) << "Sace statis data failed, error "
                   << apache::thrift::util::enumNameSafe(retCode);;
        return retCode;
    }
    return doRemove(tempKey);
}

nebula::cpp2::ErrorCode StatisJobExecutor::stop() {
    auto errOrTargetHost = getTargetHost(space_);
    if (!nebula::ok(errOrTargetHost)) {
        LOG(ERROR) << "Get target host failed";
        auto retCode = nebula::error(errOrTargetHost);
        if (retCode != nebula::cpp2::ErrorCode::E_LEADER_CHANGED) {
            retCode = nebula::cpp2::ErrorCode::E_NO_HOSTS;
        }
        return retCode;
    }

    auto& hosts = nebula::value(errOrTargetHost);
    std::vector<folly::Future<Status>> futures;
    for (auto& host : hosts) {
        auto future = adminClient_->stopTask({Utils::getAdminAddrFromStoreAddr(host.first)},
                                             jobId_, 0);
        futures.emplace_back(std::move(future));
    }

    auto tries = folly::collectAll(std::move(futures)).get();
    if (std::any_of(tries.begin(), tries.end(), [](auto& t) {
                return t.hasException();
                })) {
        LOG(ERROR) << "statis job stop() RPC failure.";
        return nebula::cpp2::ErrorCode::E_BALANCER_FAILURE;
    }

    for (const auto& t : tries) {
        if (!t.value().ok()) {
            LOG(ERROR) << "Stop statis job Failed";
            return nebula::cpp2::ErrorCode::E_BALANCER_FAILURE;
        }
    }
    return nebula::cpp2::ErrorCode::SUCCEEDED;
}

}  // namespace meta
}  // namespace nebula
