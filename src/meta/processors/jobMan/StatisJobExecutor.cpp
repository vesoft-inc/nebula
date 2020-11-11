/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

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

kvstore::ResultCode
StatisJobExecutor::save(const std::string& key, const std::string& val) {
    std::vector<kvstore::KV> data{std::make_pair(key, val)};
    folly::Baton<true, std::atomic> baton;
    nebula::kvstore::ResultCode rc = nebula::kvstore::SUCCEEDED;
    kvstore_->asyncMultiPut(kDefaultSpaceId, kDefaultPartId, std::move(data),
                            [&] (nebula::kvstore::ResultCode code) {
                                rc = code;
                                baton.post();
                            });
    baton.wait();
    return rc;
}

cpp2::ErrorCode StatisJobExecutor::prepare() {
    auto spaceRet = getSpaceIdFromName(paras_[0]);
    if (!nebula::ok(spaceRet)) {
        LOG(ERROR) << "Can't find the space: " << paras_[0];
        return nebula::error(spaceRet);
    }
    space_ = nebula::value(spaceRet);

    // Set the status of the statis job to running
    cpp2::StatisItem statisItem;
    statisItem.status = cpp2::JobStatus::RUNNING;
    auto statisKey = MetaServiceUtils::statisKey(space_);
    auto statisVal = MetaServiceUtils::statisVal(statisItem);
    save(statisKey, statisVal);
    return cpp2::ErrorCode::SUCCEEDED;
}

folly::Future<Status>
StatisJobExecutor::executeInternal(HostAddr&& address, std::vector<PartitionID>&& parts) {
    cpp2::StatisItem item;
    statisItem_.emplace(address, item);
    return adminClient_->addTask(cpp2::AdminCmd::STATIS, jobId_, taskId_++,
                                 space_, {std::move(address)}, {},
                                 std::move(parts), concurrency_, &(statisItem_[address]));
}

void StatisJobExecutor::finish(bool ExeSuccessed) {
    auto statisKey = MetaServiceUtils::statisKey(space_);
    std::string val;
    auto ret = kvstore_->get(kDefaultSpaceId, kDefaultPartId, statisKey, &val);
    if (ret != kvstore::ResultCode::SUCCEEDED) {
        LOG(ERROR) << "Can't find the statis data, spaceId : " << space_;
        return;
    }
    auto statisItem = MetaServiceUtils::parseStatisVal(val);
    if (ExeSuccessed) {
        statisItem.status = cpp2::JobStatus::FINISHED;
        statisItem.space_vertices = 0;
        statisItem.space_edges = 0;
        decltype(statisItem.tag_vertices) tagVertices;
        decltype(statisItem.edges) edges;

        for (auto& e : statisItem_) {
            auto item = e.second;
            statisItem.space_vertices += item.space_vertices;
            statisItem.space_edges += item.space_edges;

            for (auto& tag : item.tag_vertices) {
                auto tagName = tag.first;
                if (tagVertices.find(tagName) == tagVertices.end()) {
                    tagVertices[tagName] = tag.second;
                } else {
                    tagVertices[tagName] += tag.second;
                }
            }
            for (auto& edge : item.edges) {
                auto edgeName = edge.first;
                if (edges.find(edgeName) == edges.end()) {
                    edges[edgeName] = edge.second;
                } else {
                    edges[edgeName] += edge.second;
                }
            }
        }
        statisItem.set_tag_vertices(std::move(tagVertices));
        statisItem.set_edges(std::move(edges));
    } else {
        statisItem.status = cpp2::JobStatus::FAILED;
    }
    auto statisVal = MetaServiceUtils::statisVal(statisItem);
    save(statisKey, statisVal);
    return;
}

cpp2::ErrorCode StatisJobExecutor::stop() {
    auto errOrTargetHost = getTargetHost(space_);
    if (!nebula::ok(errOrTargetHost)) {
        LOG(ERROR) << "Get target host failed";
        return cpp2::ErrorCode::E_NO_HOSTS;
    }

    auto& hosts = nebula::value(errOrTargetHost);
    std::vector<folly::Future<Status>> futures;
    for (auto& host : hosts) {
        auto future = adminClient_->stopTask({Utils::getAdminAddrFromStoreAddr(host.first)},
                                             jobId_, 0);
        futures.emplace_back(std::move(future));
    }

    folly::collectAll(std::move(futures))
        .thenValue([] (const auto& tries) mutable {
            for (const auto& t : tries) {
                if (!t.value().ok()) {
                    LOG(ERROR) << "Stop statis job Failed";
                    return cpp2::ErrorCode::E_STOP_JOB_FAILURE;
                }
            }
            return cpp2::ErrorCode::SUCCEEDED;
        })
        .thenError([] (auto &&e) {
            LOG(ERROR) << "Exception caught: " << e.what();
            return cpp2::ErrorCode::E_STOP_JOB_FAILURE;
        }).wait();

    return cpp2::ErrorCode::SUCCEEDED;
}

}  // namespace meta
}  // namespace nebula
