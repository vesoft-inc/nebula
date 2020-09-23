/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "mock/MetaCache.h"
#include "MetaCache.h"
#include "common/network/NetworkUtils.h"

namespace nebula {
namespace graph {

#define CHECK_SPACE_ID(spaceId) \
    auto spaceIter = cache_.find(spaceId); \
        if (spaceIter == cache_.end()) { \
        return Status::Error("SpaceID `%d' not found", spaceId); \
    }

Status MetaCache::createSpace(const meta::cpp2::CreateSpaceReq &req, GraphSpaceID &spaceId) {
    folly::RWSpinLock::WriteHolder holder(lock_);
    auto ifNotExists = req.get_if_not_exists();
    auto properties = req.get_properties();
    auto spaceName = properties.get_space_name();
    const auto findIter = spaceIndex_.find(spaceName);
    if (ifNotExists && findIter != spaceIndex_.end()) {
        spaceId = findIter->second;
        return Status::OK();
    }
    if (findIter != spaceIndex_.end()) {
        return Status::Error("Space `%s' existed", spaceName.c_str());
    }
    spaceId = ++id_;
    spaceIndex_.emplace(spaceName, spaceId);
    meta::cpp2::SpaceItem space;
    space.set_space_id(spaceId);
    space.set_properties(std::move(properties));
    spaces_[spaceId] = space;
    auto &vid = space.get_properties().get_vid_type();
    auto vidSize = vid.__isset.type_length ? *vid.get_type_length() : 0;
    VLOG(1) << "space name: " << space.get_properties().get_space_name()
            << ", partition_num: " << space.get_properties().get_partition_num()
            << ", replica_factor: " << space.get_properties().get_replica_factor()
            << ", vid_size: " << vidSize;
    cache_[spaceId] = SpaceInfoCache();
    roles_.emplace(spaceId, UserRoles());
    return Status::OK();
}

StatusOr<meta::cpp2::SpaceItem> MetaCache::getSpace(const meta::cpp2::GetSpaceReq &req) {
    folly::RWSpinLock::ReadHolder holder(lock_);
    auto findIter = spaceIndex_.find(req.get_space_name());
    if (findIter == spaceIndex_.end()) {
        LOG(ERROR) << "Space " << req.get_space_name().c_str() << " not found";
        return Status::Error("Space `%s' not found", req.get_space_name().c_str());
    }
    const auto spaceInfo = spaces_.find(findIter->second);
    DCHECK(spaceInfo != spaces_.end());
    auto &properties = spaceInfo->second.get_properties();
    auto& vid = properties.get_vid_type();
    VLOG(1) << "space name: " << properties.get_space_name()
            << ", partition_num: " << properties.get_partition_num()
            << ", replica_factor: " << properties.get_replica_factor()
            << ", vid_size: " << (vid.__isset.type_length ? *vid.get_type_length() : 0);
    return spaceInfo->second;
}

StatusOr<std::vector<meta::cpp2::IdName>> MetaCache::listSpaces() {
    folly::RWSpinLock::ReadHolder holder(lock_);
    std::vector<meta::cpp2::IdName> spaces;
    for (const auto &index : spaceIndex_) {
        meta::cpp2::IdName idName;
        idName.set_id(to(index.second, EntryType::SPACE));
        idName.set_name(index.first);
        spaces.emplace_back(idName);
    }
    return spaces;
}

Status MetaCache::dropSpace(const meta::cpp2::DropSpaceReq &req) {
    folly::RWSpinLock::WriteHolder holder(lock_);
    auto spaceName  = req.get_space_name();
    auto findIter = spaceIndex_.find(spaceName);
    auto ifExists = req.get_if_exists();

    if (ifExists && findIter == spaceIndex_.end()) {
        Status::OK();
    }

    if (findIter == spaceIndex_.end()) {
        return Status::Error("Space `%s' not existed", req.get_space_name().c_str());
    }
    auto id = findIter->second;
    spaces_.erase(id);
    cache_.erase(id);
    roles_.erase(id);
    spaceIndex_.erase(spaceName);
    return Status::OK();
}

Status MetaCache::createTag(const meta::cpp2::CreateTagReq &req, TagID &tagId) {
    folly::RWSpinLock::WriteHolder holder(lock_);
    CHECK_SPACE_ID(req.get_space_id());
    auto ifNotExists = req.get_if_not_exists();
    auto tagName = req.get_tag_name();
    auto &tagSchemas = spaceIter->second.tagSchemas_;
    auto findIter = tagSchemas.find(tagName);
    if (ifNotExists && findIter != tagSchemas.end()) {
        tagId = findIter->second.get_tag_id();
        return Status::OK();
    }

    tagId = ++id_;
    meta::cpp2::TagItem tagItem;
    tagItem.set_tag_id(tagId);
    tagItem.set_tag_name(tagName);
    tagItem.set_version(0);
    tagItem.set_schema(req.get_schema());
    tagSchemas[tagName] = std::move(tagItem);
    return Status::OK();
}

StatusOr<meta::cpp2::Schema> MetaCache::getTag(const meta::cpp2::GetTagReq &req) {
    folly::RWSpinLock::ReadHolder holder(lock_);
    CHECK_SPACE_ID(req.get_space_id());
    auto tagName = req.get_tag_name();
    auto &tagSchemas = spaceIter->second.tagSchemas_;
    auto findIter = tagSchemas.find(tagName);
    if (findIter == tagSchemas.end()) {
        LOG(ERROR) << "Tag name: " << tagName << " not found";
        return Status::Error("Not found");
    }
    return findIter->second.get_schema();
}

StatusOr<std::vector<meta::cpp2::TagItem>>
MetaCache::listTags(const meta::cpp2::ListTagsReq &req) {
    folly::RWSpinLock::ReadHolder holder(lock_);
    CHECK_SPACE_ID(req.get_space_id());
    std::vector<meta::cpp2::TagItem> tagItems;
    for (const auto& item : spaceIter->second.tagSchemas_) {
        tagItems.emplace_back(item.second);
    }
    return tagItems;
}

Status MetaCache::dropTag(const meta::cpp2::DropTagReq& req) {
    folly::RWSpinLock::WriteHolder holder(lock_);
    CHECK_SPACE_ID(req.get_space_id());
    auto ifExists = req.get_if_exists();
    auto tagName = req.get_tag_name();
    auto &tagSchemas = spaceIter->second.tagSchemas_;
    auto findIter = tagSchemas.find(tagName);
    if (ifExists && findIter == tagSchemas.end()) {
        return Status::OK();
    }
    if (findIter == tagSchemas.end()) {
        return Status::Error("Tag `%s' not existed", req.get_tag_name().c_str());
    }

    tagSchemas.erase(findIter);
    return Status::OK();
}

Status MetaCache::createEdge(const meta::cpp2::CreateEdgeReq &req, EdgeType &edgeType) {
    folly::RWSpinLock::WriteHolder holder(lock_);
    CHECK_SPACE_ID(req.get_space_id());
    auto ifNotExists = req.get_if_not_exists();
    auto edgeName = req.get_edge_name();
    auto &edgeSchemas = spaceIter->second.edgeSchemas_;
    auto findIter = edgeSchemas.find(edgeName);
    if (ifNotExists && findIter != edgeSchemas.end()) {
        edgeType = findIter->second.get_edge_type();
        return Status::OK();
    }

    edgeType = ++id_;
    meta::cpp2::EdgeItem edgeItem;
    edgeItem.set_edge_type(edgeType);
    edgeItem.set_edge_name(edgeName);
    edgeItem.set_version(0);
    edgeItem.set_schema(req.get_schema());
    edgeSchemas[edgeName] = std::move(edgeItem);
    return Status::OK();
}

StatusOr<meta::cpp2::Schema> MetaCache::getEdge(const meta::cpp2::GetEdgeReq &req) {
    folly::RWSpinLock::ReadHolder holder(lock_);
    CHECK_SPACE_ID(req.get_space_id());
    auto edgeName = req.get_edge_name();
    auto &edgeSchemas = spaceIter->second.edgeSchemas_;
    auto findIter = edgeSchemas.find(edgeName);
    if (findIter == edgeSchemas.end()) {
        return Status::Error("Not found");
    }
    return findIter->second.get_schema();
}

StatusOr<std::vector<meta::cpp2::EdgeItem>>
MetaCache::listEdges(const meta::cpp2::ListEdgesReq &req) {
    folly::RWSpinLock::ReadHolder holder(lock_);
    CHECK_SPACE_ID(req.get_space_id());
    std::vector<meta::cpp2::EdgeItem> edgeItems;
    for (const auto& item : spaceIter->second.edgeSchemas_) {
        edgeItems.emplace_back(item.second);
    }
    return edgeItems;
}

Status MetaCache::dropEdge(const meta::cpp2::DropEdgeReq& req) {
    folly::RWSpinLock::WriteHolder holder(lock_);
    CHECK_SPACE_ID(req.get_space_id());
    auto ifExists = req.get_if_exists();
    auto edgeName = req.get_edge_name();
    auto &edgeSchemas = spaceIter->second.edgeSchemas_;
    auto findIter = edgeSchemas.find(edgeName);
    if (ifExists && findIter == edgeSchemas.end()) {
        return Status::OK();
    }

    if (findIter == edgeSchemas.end()) {
        return Status::Error("Edge `%s' not existed", req.get_edge_name().c_str());
    }

    edgeSchemas.erase(findIter);
    return Status::OK();
}

Status MetaCache::AlterTag(const meta::cpp2::AlterTagReq &req) {
    folly::RWSpinLock::WriteHolder holder(lock_);
    CHECK_SPACE_ID(req.get_space_id());
    auto tagName = req.get_tag_name();
    auto &tagSchemas = spaceIter->second.tagSchemas_;
    auto findIter = tagSchemas.find(tagName);
    if (findIter == tagSchemas.end()) {
        return Status::Error("Tag `%s' not existed", req.get_tag_name().c_str());
    }

    auto &schema = findIter->second.schema;
    auto items = req.get_tag_items();
    auto prop = req.get_schema_prop();
    auto status = alterColumnDefs(schema, items);
    if (!status.ok()) {
        return status;
    }
    return alterSchemaProp(schema, prop);
}

Status MetaCache::AlterEdge(const meta::cpp2::AlterEdgeReq &req) {
    folly::RWSpinLock::WriteHolder holder(lock_);
    CHECK_SPACE_ID(req.get_space_id());
    auto edgeName = req.get_edge_name();
    auto &edgeSchemas = spaceIter->second.edgeSchemas_;
    auto findIter = edgeSchemas.find(edgeName);
    if (findIter == edgeSchemas.end()) {
        return Status::Error("Edge `%s' not existed", req.get_edge_name().c_str());
    }

    auto &schema = findIter->second.schema;
    auto items = req.get_edge_items();
    auto prop = req.get_schema_prop();
    auto status = alterColumnDefs(schema, items);
    if (!status.ok()) {
        return status;
    }
    return alterSchemaProp(schema, prop);
}

Status MetaCache::createTagIndex(const meta::cpp2::CreateTagIndexReq&) {
    return Status::OK();
}

Status MetaCache::createEdgeIndex(const meta::cpp2::CreateEdgeIndexReq&) {
    return Status::OK();
}

Status MetaCache::dropTagIndex(const meta::cpp2::DropTagIndexReq&) {
    return Status::OK();
}

Status MetaCache::dropTagIndex(const meta::cpp2::DropEdgeIndexReq&) {
    return Status::OK();
}

Status MetaCache::regConfigs(const std::vector<meta::cpp2::ConfigItem>&) {
    return Status::OK();
}

Status MetaCache::setConfig(const meta::cpp2::ConfigItem&) {
    return Status::OK();
}

Status MetaCache::heartBeat(const meta::cpp2::HBReq& req) {
    folly::RWSpinLock::WriteHolder holder(lock_);
    auto host = req.get_host();
    if (host.port == 0) {
        return Status::OK();
    }
    hostSet_.emplace(std::move(host));
    return Status::OK();
}

std::vector<meta::cpp2::HostItem> MetaCache::listHosts() {
    folly::RWSpinLock::WriteHolder holder(lock_);
    std::vector<meta::cpp2::HostItem> hosts;
    for (auto& spaceIdIt : spaceIndex_) {
        auto spaceName = spaceIdIt.first;
        for (auto &h : hostSet_) {
            meta::cpp2::HostItem host;
            host.set_hostAddr(h);
            host.set_status(meta::cpp2::HostStatus::ONLINE);
            std::unordered_map<std::string, std::vector<PartitionID>> leaderParts;
            std::vector<PartitionID> parts = {1};
            leaderParts.emplace(spaceName, parts);
            host.set_leader_parts(leaderParts);
            host.set_all_parts(std::move(leaderParts));
        }
    }
    return hosts;
}

std::unordered_map<PartitionID, std::vector<HostAddr>> MetaCache::getParts() {
    folly::RWSpinLock::WriteHolder holder(lock_);
    std::unordered_map<PartitionID, std::vector<HostAddr>> parts;
    parts[1] = {};
    for (auto &h : hostSet_) {
        parts[1].emplace_back(h);
    }
    return parts;
}

////////////////////////////////////////////// ACL related mock ////////////////////////////////////
meta::cpp2::ExecResp MetaCache::createUser(const meta::cpp2::CreateUserReq& req) {
    meta::cpp2::ExecResp resp;
    folly::RWSpinLock::WriteHolder wh(userLock_);
    const auto user = users_.find(req.get_account());
    if (user != users_.end()) {  // already exists
        resp.set_code(req.get_if_not_exists() ?
                    meta::cpp2::ErrorCode::SUCCEEDED :
                    meta::cpp2::ErrorCode::E_EXISTED);
        return resp;
    }

    auto result = users_.emplace(req.get_account(), UserInfo{req.get_encoded_pwd()});
    resp.set_code(result.second ?
                  meta::cpp2::ErrorCode::SUCCEEDED :
                  meta::cpp2::ErrorCode::E_UNKNOWN);
    return resp;
}

meta::cpp2::ExecResp MetaCache::dropUser(const meta::cpp2::DropUserReq& req) {
    meta::cpp2::ExecResp resp;
    folly::RWSpinLock::WriteHolder wh(userLock_);
    const auto user = users_.find(req.get_account());
    if (user == users_.end()) {  // not exists
        resp.set_code(req.get_if_exists() ?
                    meta::cpp2::ErrorCode::SUCCEEDED :
                    meta::cpp2::ErrorCode::E_NOT_FOUND);
        return resp;
    }

    auto result = users_.erase(req.get_account());
    resp.set_code(result == 1 ?
                  meta::cpp2::ErrorCode::SUCCEEDED :
                  meta::cpp2::ErrorCode::E_UNKNOWN);
    return resp;
}

meta::cpp2::ExecResp MetaCache::alterUser(const meta::cpp2::AlterUserReq& req) {
    meta::cpp2::ExecResp resp;
    folly::RWSpinLock::WriteHolder wh(userLock_);
    auto user = users_.find(req.get_account());
    if (user == users_.end()) {  // not exists
        resp.set_code(meta::cpp2::ErrorCode::E_NOT_FOUND);
        return resp;
    }
    user->second.password = req.get_encoded_pwd();
    resp.set_code(meta::cpp2::ErrorCode::SUCCEEDED);
    return resp;
}

meta::cpp2::ExecResp MetaCache::grantRole(const meta::cpp2::GrantRoleReq& req) {
    meta::cpp2::ExecResp resp;
    const auto &item = req.get_role_item();
    {
        folly::RWSpinLock::ReadHolder spaceRH(roleLock_);
        folly::RWSpinLock::ReadHolder userRH(userLock_);
        // find space
        auto space = roles_.find(item.get_space_id());
        if (space == roles_.end()) {
            resp.set_code(meta::cpp2::ErrorCode::E_NOT_FOUND);
            return resp;
        }
        // find user
        auto user = users_.find(item.get_user_id());
        if (user == users_.end()) {
            resp.set_code(meta::cpp2::ErrorCode::E_NOT_FOUND);
            return resp;
        }
    }
    folly::RWSpinLock::WriteHolder roleWH(roleLock_);
    // space
    auto space = roles_.find(item.get_space_id());
    // user
    auto user = space->second.find(item.get_user_id());
    if (user == space->second.end()) {
        space->second.emplace(item.get_user_id(),
                              std::unordered_set<meta::cpp2::RoleType>{item.get_role_type()});
    } else {
        user->second.emplace(item.get_role_type());
    }
    resp.set_code(meta::cpp2::ErrorCode::SUCCEEDED);
    return resp;
}

meta::cpp2::ExecResp MetaCache::revokeRole(const meta::cpp2::RevokeRoleReq& req) {
    meta::cpp2::ExecResp resp;
    const auto &item = req.get_role_item();
    folly::RWSpinLock::WriteHolder rolesWH(roleLock_);
    // find space
    auto space = roles_.find(item.get_space_id());
    if (space == roles_.end()) {
        resp.set_code(meta::cpp2::ErrorCode::E_NOT_FOUND);
        return resp;
    }
    // find user
    auto user = space->second.find(item.get_user_id());
    if (user == space->second.end()) {
        resp.set_code(meta::cpp2::ErrorCode::E_NOT_FOUND);
        return resp;
    }
    // find role
    auto role = user->second.find(item.get_role_type());
    if (role == user->second.end()) {
        resp.set_code(meta::cpp2::ErrorCode::E_NOT_FOUND);
        return resp;
    }
    user->second.erase(item.get_role_type());
    resp.set_code(meta::cpp2::ErrorCode::SUCCEEDED);
    return resp;
}

meta::cpp2::ListUsersResp MetaCache::listUsers(const meta::cpp2::ListUsersReq&) {
    meta::cpp2::ListUsersResp resp;
    folly::RWSpinLock::ReadHolder rh(userLock_);
    std::unordered_map<std::string, std::string> users;
    for (const auto &user : users_) {
        users.emplace(user.first, user.second.password);
    }
    resp.set_code(meta::cpp2::ErrorCode::SUCCEEDED);
    resp.set_users(std::move(users));
    return resp;
}

meta::cpp2::ListRolesResp MetaCache::listRoles(const meta::cpp2::ListRolesReq& req) {
    meta::cpp2::ListRolesResp resp;
    folly::RWSpinLock::ReadHolder rh(roleLock_);
    std::vector<meta::cpp2::RoleItem> items;
    const auto space = roles_.find(req.get_space_id());
    if (space == roles_.end()) {
        resp.set_code(meta::cpp2::ErrorCode::E_NOT_FOUND);
        return resp;
    }
    for (const auto &user : space->second) {
        for (const auto &role : user.second) {
            meta::cpp2::RoleItem item;
            item.set_space_id(space->first);
            item.set_user_id(user.first);
            item.set_role_type(role);
            items.emplace_back(std::move(item));
        }
    }
    resp.set_code(meta::cpp2::ErrorCode::SUCCEEDED);
    resp.set_roles(std::move(items));
    return resp;
}

meta::cpp2::ExecResp MetaCache::changePassword(const meta::cpp2::ChangePasswordReq& req) {
    meta::cpp2::ExecResp resp;
    folly::RWSpinLock::WriteHolder wh(userLock_);
    auto user = users_.find(req.get_account());
    if (user == users_.end()) {
        resp.set_code(meta::cpp2::ErrorCode::E_NOT_FOUND);
        return resp;
    }
    if (user->second.password != req.get_old_encoded_pwd()) {
        resp.set_code(meta::cpp2::ErrorCode::E_INVALID_PASSWORD);
    }
    user->second.password = req.get_new_encoded_pwd();
    resp.set_code(meta::cpp2::ErrorCode::SUCCEEDED);
    return resp;
}

meta::cpp2::ListRolesResp MetaCache::getUserRoles(const meta::cpp2::GetUserRolesReq& req) {
    meta::cpp2::ListRolesResp resp;
    {
        folly::RWSpinLock::ReadHolder userRH(userLock_);
        // find user
        auto user = users_.find(req.get_account());
        if (user == users_.end()) {
            resp.set_code(meta::cpp2::ErrorCode::E_NOT_FOUND);
            return resp;
        }
    }

    folly::RWSpinLock::ReadHolder roleRH(roleLock_);
    std::vector<meta::cpp2::RoleItem> items;
    for (const auto& space : roles_) {
        const auto& user = space.second.find(req.get_account());
        if (user != space.second.end()) {
            for (const auto & role : user->second) {
                meta::cpp2::RoleItem item;
                item.set_space_id(space.first);
                item.set_user_id(user->first);
                item.set_role_type(role);
                items.emplace_back(std::move(item));
            }
        }
    }
    resp.set_code(meta::cpp2::ErrorCode::SUCCEEDED);
    resp.set_roles(std::move(items));
    return resp;
}

ErrorOr<meta::cpp2::ErrorCode, int64_t> MetaCache::balanceSubmit(std::vector<HostAddr> dels) {
    folly::RWSpinLock::ReadHolder rh(lock_);
    for (const auto &job : balanceJobs_) {
        if (job.second.status == meta::cpp2::TaskResult::IN_PROGRESS) {
            return meta::cpp2::ErrorCode::E_BALANCER_RUNNING;
        }
    }
    std::vector<BalanceTask> jobs;
    for (const auto &spaceInfo : spaces_) {
        for (PartitionID i = 1; i <= spaceInfo.second.get_properties().get_partition_num(); ++i) {
            for (const auto &host : hostSet_) {  // Note mock partition in each host here
                if (std::find(dels.begin(), dels.end(), host) != dels.end()) {
                    continue;
                }
                jobs.emplace_back(BalanceTask{
                    // mock
                    spaceInfo.first,
                    i,
                    host,
                    host,
                    meta::cpp2::TaskResult::IN_PROGRESS,
                });
            }
        }
    }
    auto jobId = incId();
    balanceTasks_.emplace(jobId, std::move(jobs));
    balanceJobs_.emplace(jobId, BalanceJob{
        meta::cpp2::TaskResult::IN_PROGRESS,
    });
    return jobId;
}

ErrorOr<meta::cpp2::ErrorCode, int64_t> MetaCache::balanceStop() {
    for (auto &job : balanceJobs_) {
        if (job.second.status == meta::cpp2::TaskResult::IN_PROGRESS) {
            job.second.status = meta::cpp2::TaskResult::FAILED;
            return job.first;
        }
    }
    return meta::cpp2::ErrorCode::E_NO_RUNNING_BALANCE_PLAN;
}

meta::cpp2::ErrorCode MetaCache::balanceLeaders() {
    return meta::cpp2::ErrorCode::SUCCEEDED;
}

ErrorOr<meta::cpp2::ErrorCode, std::vector<meta::cpp2::BalanceTask>>
MetaCache::showBalance(int64_t id) {
    const auto job = balanceTasks_.find(id);
    if (job == balanceTasks_.end()) {
        return meta::cpp2::ErrorCode::E_NOT_FOUND;
    }
    std::vector<meta::cpp2::BalanceTask> result;
    result.reserve(job->second.size());
    for (const auto &task : job->second) {
        meta::cpp2::BalanceTask taskInfo;
        std::stringstream idStr;
        idStr << "[";
        idStr << id << ", ";
        idStr << task.space << ":" << task.part << ", ";
        idStr << task.from << "->" << task.to;
        idStr << "]";
        taskInfo.set_id(idStr.str());
        taskInfo.set_result(task.status);
        result.emplace_back(std::move(taskInfo));
    }
    return result;
}

ErrorOr<meta::cpp2::ErrorCode, meta::cpp2::AdminJobResult>
MetaCache::runAdminJob(const meta::cpp2::AdminJobReq& req) {
    meta::cpp2::AdminJobResult result;
    switch (req.get_op()) {
        case meta::cpp2::AdminJobOp::ADD: {
            folly::RWSpinLock::WriteHolder wh(jobLock_);
            auto jobId = incId();
            jobs_.emplace(jobId, JobDesc{
                req.get_cmd(),
                req.get_paras(),
                meta::cpp2::JobStatus::QUEUE,
                0,
                0
            });
            std::vector<TaskDesc> descs;
            int32_t iTask = 0;
            for (const auto &host : hostSet_) {
                descs.reserve(hostSet_.size());
                descs.emplace_back(TaskDesc{
                    ++iTask,
                    host,
                    meta::cpp2::JobStatus::QUEUE,
                    0,
                    0
                });
            }
            tasks_.emplace(jobId, std::move(descs));
            result.set_job_id(jobId);
            return result;
        }
        case meta::cpp2::AdminJobOp::RECOVER: {
            uint32_t jobNum = 0;
            folly::RWSpinLock::WriteHolder wh(jobLock_);
            for (auto &job : jobs_) {
                if (job.second.status_ == meta::cpp2::JobStatus::FAILED) {
                    job.second.status_ = meta::cpp2::JobStatus::QUEUE;
                    ++jobNum;
                }
            }
            result.set_recovered_job_num(jobNum);
            return result;
        }
        case meta::cpp2::AdminJobOp::SHOW: {
            folly::RWSpinLock::ReadHolder rh(jobLock_);
            auto ret = checkJobId(req);
            if (!ok(ret)) {
                return error(ret);
            }
            auto job = value(ret);
            result.set_job_id(job->first);
            std::vector<meta::cpp2::JobDesc> jobsDesc;
            meta::cpp2::JobDesc jobDesc;
            jobDesc.set_id(job->first);
            jobDesc.set_cmd(job->second.cmd_);
            jobDesc.set_status(job->second.status_);
            jobDesc.set_start_time(job->second.startTime_);
            jobDesc.set_stop_time(job->second.stopTime_);
            jobsDesc.emplace_back(std::move(jobDesc));
            result.set_job_desc(std::move(jobsDesc));

            // tasks
            const auto tasks = tasks_.find(job->first);
            if (tasks == tasks_.end()) {
                LOG(FATAL) << "Impossible not find tasks of job id " << job->first;
            }
            std::vector<meta::cpp2::TaskDesc> tasksDesc;
            for (const auto &task : tasks->second) {
                meta::cpp2::TaskDesc taskDesc;
                taskDesc.set_job_id(job->first);
                taskDesc.set_task_id(task.iTask_);
                taskDesc.set_host(task.dest_);
                taskDesc.set_status(task.status_);
                taskDesc.set_start_time(task.startTime_);
                taskDesc.set_stop_time(task.stopTime_);
                tasksDesc.emplace_back(std::move(taskDesc));
            }
            result.set_task_desc(std::move(tasksDesc));
            return result;
        }
        case meta::cpp2::AdminJobOp::SHOW_All: {
            std::vector<meta::cpp2::JobDesc> jobsDesc;
            folly::RWSpinLock::ReadHolder rh(jobLock_);
            for (const auto &job : jobs_) {
                meta::cpp2::JobDesc jobDesc;
                jobDesc.set_id(job.first);
                jobDesc.set_cmd(job.second.cmd_);
                jobDesc.set_status(job.second.status_);
                jobDesc.set_start_time(job.second.startTime_);
                jobDesc.set_stop_time(job.second.stopTime_);
                jobsDesc.emplace_back(std::move(jobDesc));
            }
            result.set_job_desc(std::move(jobsDesc));
            return result;
        }
        case meta::cpp2::AdminJobOp::STOP: {
            folly::RWSpinLock::WriteHolder wh(jobLock_);
            auto ret = checkJobId(req);
            if (!ok(ret)) {
                return error(ret);
            }
            auto job = value(ret);
            if (job->second.status_ != meta::cpp2::JobStatus::QUEUE &&
                job->second.status_ != meta::cpp2::JobStatus::RUNNING) {
                return meta::cpp2::ErrorCode::E_CONFLICT;
            }
            job->second.status_ = meta::cpp2::JobStatus::STOPPED;
            return result;
        }
    }
    return meta::cpp2::ErrorCode::E_INVALID_PARM;
}

Status MetaCache::alterColumnDefs(meta::cpp2::Schema &schema,
                                  const std::vector<meta::cpp2::AlterSchemaItem> &items) {
    std::vector<meta::cpp2::ColumnDef> columns = schema.columns;
    for (auto& item : items) {
        auto& cols = item.get_schema().get_columns();
        auto op = item.op;
        for (auto& col : cols) {
            switch (op) {
                case meta::cpp2::AlterSchemaOp::ADD:
                    for (auto it = schema.columns.begin(); it != schema.columns.end(); ++it) {
                        if (it->get_name() == col.get_name()) {
                            return Status::Error("Column existing: `%s'", col.get_name().c_str());
                        }
                    }
                    columns.emplace_back(col);
                    break;
                case meta::cpp2::AlterSchemaOp::CHANGE: {
                    bool isOk = false;
                    for (auto it = columns.begin(); it != columns.end(); ++it) {
                        auto colName = col.get_name();
                        if (colName == it->get_name()) {
                            // If this col is ttl_col, change not allowed
                            if (schema.schema_prop.__isset.ttl_col &&
                                (*schema.schema_prop.get_ttl_col() == colName)) {
                                return Status::Error("Column: `%s' as ttl_col, change not allowed",
                                                     colName.c_str());
                            }
                            *it = col;
                            isOk = true;
                            break;
                        }
                    }
                    if (!isOk) {
                        return Status::Error("Column not found: `%s'", col.get_name().c_str());
                    }
                    break;
                }
                case meta::cpp2::AlterSchemaOp::DROP: {
                    bool isOk = false;
                    for (auto it = columns.begin(); it != columns.end(); ++it) {
                        auto colName = col.get_name();
                        if (colName == it->get_name()) {
                            if (schema.schema_prop.__isset.ttl_col &&
                                (*schema.schema_prop.get_ttl_col() == colName)) {
                                schema.schema_prop.set_ttl_duration(0);
                                schema.schema_prop.set_ttl_col("");
                            }
                            columns.erase(it);
                            isOk = true;
                            break;
                        }
                    }
                    if (!isOk) {
                        return Status::Error("Column not found: `%s'", col.get_name().c_str());
                    }
                    break;
                }
                default:
                    return Status::Error("Alter schema operator not supported");
            }
        }
    }
    schema.columns = std::move(columns);
    return Status::OK();
}

Status MetaCache::alterSchemaProp(meta::cpp2::Schema &schema,
                                  const meta::cpp2::SchemaProp &alterSchemaProp) {
    meta::cpp2::SchemaProp schemaProp = schema.get_schema_prop();
    if (alterSchemaProp.__isset.ttl_duration) {
        // Graph check  <=0 to = 0
        schemaProp.set_ttl_duration(*alterSchemaProp.get_ttl_duration());
    }
    if (alterSchemaProp.__isset.ttl_col) {
        auto ttlCol = *alterSchemaProp.get_ttl_col();
        // Disable ttl, ttl_col is empty, ttl_duration is 0
        if (ttlCol.empty()) {
            schemaProp.set_ttl_duration(0);
            schemaProp.set_ttl_col(ttlCol);
            return Status::OK();
        }

        auto existed = false;
        for (auto& col : schema.columns) {
            if (col.get_name() == ttlCol) {
                // Only integer and timestamp columns can be used as ttl_col
                if (col.type.type != meta::cpp2::PropertyType::INT32 &&
                    col.type.type != meta::cpp2::PropertyType::INT64 &&
                    col.type.type != meta::cpp2::PropertyType::TIMESTAMP) {
                    return Status::Error("TTL column type illegal");
                }
                existed = true;
                schemaProp.set_ttl_col(ttlCol);
                break;
            }
        }

        if (!existed) {
            return Status::Error("TTL column not found: `%s'", ttlCol.c_str());
        }
    }

    // Disable implicit TTL mode
    if ((schemaProp.get_ttl_duration() && (*schemaProp.get_ttl_duration() != 0)) &&
        (!schemaProp.get_ttl_col() || (schemaProp.get_ttl_col() &&
                                       schemaProp.get_ttl_col()->empty()))) {
        return Status::Error("Implicit ttl_col not support");
    }

    schema.set_schema_prop(std::move(schemaProp));
    return Status::OK();
}

Status MetaCache::createSnapshot() {
    folly::RWSpinLock::WriteHolder holder(lock_);
    if (cache_.empty()) {
        return Status::OK();
    }
    meta::cpp2::Snapshot snapshot;
    char ch[60];
    std::time_t t = std::time(nullptr);
    std::strftime(ch, sizeof(ch), "%Y_%m_%d_%H_%M_%S", localtime(&t));
    auto snapshotName = folly::stringPrintf("SNAPSHOT_%s", ch);
    snapshot.set_name(snapshotName);
    snapshot.set_status(meta::cpp2::SnapshotStatus::VALID);
    DCHECK(!hostSet_.empty());
    snapshot.set_hosts(network::NetworkUtils::toHostsStr(
            std::vector<HostAddr>(hostSet_.begin(), hostSet_.end())));
    snapshots_[snapshotName] = std::move(snapshot);
    return Status::OK();
}

Status MetaCache::dropSnapshot(const meta::cpp2::DropSnapshotReq& req) {
    folly::RWSpinLock::WriteHolder holder(lock_);
    auto name = req.get_name();
    auto find = snapshots_.find(name);
    if (find == snapshots_.end()) {
        return Status::Error("`%s' is nonexistent", name.c_str());
    }
    snapshots_.erase(find);
    return Status::OK();
}

StatusOr<std::vector<meta::cpp2::Snapshot>> MetaCache::listSnapshots() {
    folly::RWSpinLock::ReadHolder holder(lock_);
    std::vector<meta::cpp2::Snapshot> snapshots;
    for (auto& snapshot : snapshots_) {
        snapshots.emplace_back(snapshot.second);
    }
    return snapshots;
}

}  // namespace graph
}  // namespace nebula
