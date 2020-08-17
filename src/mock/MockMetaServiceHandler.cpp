/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "mock/MockMetaServiceHandler.h"

#include "common/time/WallClock.h"
#include "mock/MetaCache.h"

namespace nebula {
namespace graph {

#define RETURN_SUCCESSED() \
    folly::Promise<meta::cpp2::ExecResp> promise; \
    auto future = promise.getFuture(); \
    meta::cpp2::ExecResp resp; \
    resp.set_code(meta::cpp2::ErrorCode::SUCCEEDED); \
    promise.setValue(std::move(resp)); \
    return future;

folly::Future<meta::cpp2::ExecResp>
MockMetaServiceHandler::future_createSpace(const meta::cpp2::CreateSpaceReq& req) {
    folly::Promise<meta::cpp2::ExecResp> promise;
    auto future = promise.getFuture();
    meta::cpp2::ExecResp resp;
    GraphSpaceID spaceId;
    auto status = MetaCache::instance().createSpace(req, spaceId);
    if (!status.ok()) {
        resp.set_code(meta::cpp2::ErrorCode::E_UNKNOWN);
        promise.setValue(std::move(resp));
        return future;
    }
    resp.set_code(meta::cpp2::ErrorCode::SUCCEEDED);
    meta::cpp2::ID id;
    id.set_space_id(spaceId);
    resp.set_id(id);
    promise.setValue(std::move(resp));
    return future;
}

folly::Future<meta::cpp2::ExecResp>
MockMetaServiceHandler::future_dropSpace(const meta::cpp2::DropSpaceReq &req) {
    folly::Promise<meta::cpp2::ExecResp> promise;
    auto future = promise.getFuture();
    meta::cpp2::ExecResp resp;
    auto status = MetaCache::instance().dropSpace(req);
    if (!status.ok()) {
        resp.set_code(meta::cpp2::ErrorCode::E_UNKNOWN);
        promise.setValue(std::move(resp));
        return future;
    }
    resp.set_code(meta::cpp2::ErrorCode::SUCCEEDED);
    promise.setValue(std::move(resp));
    return future;
}

folly::Future<meta::cpp2::ListSpacesResp>
MockMetaServiceHandler::future_listSpaces(const meta::cpp2::ListSpacesReq&) {
    folly::Promise<meta::cpp2::ListSpacesResp> promise;
    auto future = promise.getFuture();
    auto status = MetaCache::instance().listSpaces();
    meta::cpp2::ListSpacesResp resp;
    if (!status.ok()) {
        resp.set_code(meta::cpp2::ErrorCode::E_UNKNOWN);
        promise.setValue(std::move(resp));
        return future;
    }
    resp.set_code(meta::cpp2::ErrorCode::SUCCEEDED);
    resp.set_spaces(std::move(status).value());
    promise.setValue(std::move(resp));
    return future;
}

folly::Future<meta::cpp2::AdminJobResp>
MockMetaServiceHandler::future_runAdminJob(const meta::cpp2::AdminJobReq& req) {
    meta::cpp2::AdminJobResp resp;
    auto result = MetaCache::instance().runAdminJob(req);
    if (!ok(result)) {
        resp.set_code(result.left());
        return resp;
    }
    resp.set_code(meta::cpp2::ErrorCode::SUCCEEDED);
    resp.set_result(std::move(result).right());
    return resp;
}

folly::Future<meta::cpp2::GetSpaceResp>
MockMetaServiceHandler::future_getSpace(const meta::cpp2::GetSpaceReq& req) {
    folly::Promise<meta::cpp2::GetSpaceResp> promise;
    auto future = promise.getFuture();
    auto status = MetaCache::instance().getSpace(req);
    meta::cpp2::GetSpaceResp resp;
    if (!status.ok()) {
        resp.set_code(meta::cpp2::ErrorCode::E_UNKNOWN);
        promise.setValue(std::move(resp));
        return future;
    }
    auto spaceInfo = std::move(status).value();
    resp.set_code(meta::cpp2::ErrorCode::SUCCEEDED);
    resp.set_item(std::move(spaceInfo));
    promise.setValue(std::move(resp));
    return future;
}

folly::Future<meta::cpp2::ListHostsResp>
MockMetaServiceHandler::future_listHosts(const meta::cpp2::ListHostsReq&) {
    folly::Promise<meta::cpp2::ListHostsResp> promise;
    auto future = promise.getFuture();
    meta::cpp2::ListHostsResp resp;
    resp.set_code(meta::cpp2::ErrorCode::SUCCEEDED);
    resp.set_hosts(MetaCache::instance().listHosts());
    promise.setValue(resp);
    return future;
}

folly::Future<meta::cpp2::ListPartsResp>
MockMetaServiceHandler::future_listParts(const meta::cpp2::ListPartsReq&) {
    folly::Promise<meta::cpp2::ListPartsResp> promise;
    auto future = promise.getFuture();
    meta::cpp2::ListPartsResp resp;
    resp.set_code(meta::cpp2::ErrorCode::SUCCEEDED);
    promise.setValue(std::move(resp));
    return future;
}

folly::Future<meta::cpp2::GetPartsAllocResp>
MockMetaServiceHandler::future_getPartsAlloc(const meta::cpp2::GetPartsAllocReq&) {
    folly::Promise<meta::cpp2::GetPartsAllocResp> promise;
    auto future = promise.getFuture();
    meta::cpp2::GetPartsAllocResp resp;
    resp.set_code(meta::cpp2::ErrorCode::SUCCEEDED);
    resp.set_parts(MetaCache::instance().getParts());
    promise.setValue(resp);
    return future;
}

folly::Future<meta::cpp2::ExecResp>
MockMetaServiceHandler::future_multiPut(const meta::cpp2::MultiPutReq&) {
    RETURN_SUCCESSED();
}

folly::Future<meta::cpp2::GetResp>
MockMetaServiceHandler::future_get(const meta::cpp2::GetReq&) {
    folly::Promise<meta::cpp2::GetResp> promise;
    auto future = promise.getFuture();
    meta::cpp2::GetResp resp;
    resp.set_code(meta::cpp2::ErrorCode::SUCCEEDED);
    promise.setValue(std::move(resp));
    return future;
}

folly::Future<meta::cpp2::MultiGetResp>
MockMetaServiceHandler::future_multiGet(const meta::cpp2::MultiGetReq&) {
    folly::Promise<meta::cpp2::MultiGetResp> promise;
    auto future = promise.getFuture();
    meta::cpp2::MultiGetResp resp;
    resp.set_code(meta::cpp2::ErrorCode::SUCCEEDED);
    promise.setValue(std::move(resp));
    return future;
}

folly::Future<meta::cpp2::ScanResp>
MockMetaServiceHandler::future_scan(const meta::cpp2::ScanReq&) {
    folly::Promise<meta::cpp2::ScanResp> promise;
    auto future = promise.getFuture();
    meta::cpp2::ScanResp resp;
    resp.set_code(meta::cpp2::ErrorCode::SUCCEEDED);
    promise.setValue(std::move(resp));
    return future;
}

folly::Future<meta::cpp2::ExecResp>
MockMetaServiceHandler::future_remove(const meta::cpp2::RemoveReq&) {
    RETURN_SUCCESSED();
}

folly::Future<meta::cpp2::ExecResp>
MockMetaServiceHandler::future_removeRange(const meta::cpp2::RemoveRangeReq&) {
    RETURN_SUCCESSED();
}

folly::Future<meta::cpp2::ExecResp>
MockMetaServiceHandler::future_createTag(const meta::cpp2::CreateTagReq& req) {
    folly::Promise<meta::cpp2::ExecResp> promise;
    auto future = promise.getFuture();
    meta::cpp2::ExecResp resp;
    TagID tagId = 0;
    auto status = MetaCache::instance().createTag(req, tagId);
    if (!status.ok()) {
        resp.set_code(meta::cpp2::ErrorCode::E_UNKNOWN);
        promise.setValue(std::move(resp));
        return future;
    }
    resp.set_code(meta::cpp2::ErrorCode::SUCCEEDED);
    meta::cpp2::ID id;
    id.set_tag_id(tagId);
    resp.set_id(std::move(id));
    promise.setValue(std::move(resp));
    return future;
}

folly::Future<meta::cpp2::ExecResp>
MockMetaServiceHandler::future_alterTag(const meta::cpp2::AlterTagReq &req) {
    folly::Promise<meta::cpp2::ExecResp> promise;
    auto future = promise.getFuture();
    meta::cpp2::ExecResp resp;
    auto status = MetaCache::instance().AlterTag(req);
    if (!status.ok()) {
        LOG(ERROR) << status;
        resp.set_code(meta::cpp2::ErrorCode::E_UNKNOWN);
        promise.setValue(std::move(resp));
        return future;
    }
    resp.set_code(meta::cpp2::ErrorCode::SUCCEEDED);
    promise.setValue(std::move(resp));
    return future;
}

folly::Future<meta::cpp2::ExecResp>
MockMetaServiceHandler::future_dropTag(const meta::cpp2::DropTagReq& req) {
    folly::Promise<meta::cpp2::ExecResp> promise;
    auto future = promise.getFuture();
    meta::cpp2::ExecResp resp;
    auto status = MetaCache::instance().dropTag(req);
    if (!status.ok()) {
        resp.set_code(meta::cpp2::ErrorCode::E_UNKNOWN);
        promise.setValue(std::move(resp));
        return future;
    }
    resp.set_code(meta::cpp2::ErrorCode::SUCCEEDED);
    promise.setValue(std::move(resp));
    return future;
}

folly::Future<meta::cpp2::GetTagResp>
MockMetaServiceHandler::future_getTag(const meta::cpp2::GetTagReq& req) {
    folly::Promise<meta::cpp2::GetTagResp> promise;
    auto future = promise.getFuture();
    meta::cpp2::GetTagResp resp;
    auto status = MetaCache::instance().getTag(req);
    if (!status.ok()) {
        resp.set_code(meta::cpp2::ErrorCode::E_UNKNOWN);
        promise.setValue(std::move(resp));
        return future;
    }
    resp.set_code(meta::cpp2::ErrorCode::SUCCEEDED);
    resp.set_schema(std::move(status).value());
    promise.setValue(std::move(resp));
    return future;
}

folly::Future<meta::cpp2::ListTagsResp>
MockMetaServiceHandler::future_listTags(const meta::cpp2::ListTagsReq& req) {
    folly::Promise<meta::cpp2::ListTagsResp> promise;
    auto future = promise.getFuture();
    auto status = MetaCache::instance().listTags(req);
    meta::cpp2::ListTagsResp resp;
    if (!status.ok()) {
        resp.set_code(meta::cpp2::ErrorCode::E_UNKNOWN);
        promise.setValue(std::move(resp));
        return future;
    }

    resp.set_code(meta::cpp2::ErrorCode::SUCCEEDED);
    resp.set_tags(std::move(status).value());
    promise.setValue(std::move(resp));
    return future;
}

folly::Future<meta::cpp2::ExecResp>
MockMetaServiceHandler::future_createEdge(const meta::cpp2::CreateEdgeReq& req) {
    folly::Promise<meta::cpp2::ExecResp> promise;
    auto future = promise.getFuture();
    meta::cpp2::ExecResp resp;
    EdgeType edgeType;
    auto status = MetaCache::instance().createEdge(req, edgeType);
    if (!status.ok()) {
        resp.set_code(meta::cpp2::ErrorCode::E_UNKNOWN);
        promise.setValue(std::move(resp));
        return future;
    }
    resp.set_code(meta::cpp2::ErrorCode::SUCCEEDED);
    meta::cpp2::ID id;
    id.set_edge_type(edgeType);
    resp.set_id(std::move(id));
    promise.setValue(std::move(resp));
    return future;
}

folly::Future<meta::cpp2::ExecResp>
MockMetaServiceHandler::future_alterEdge(const meta::cpp2::AlterEdgeReq &req) {
    folly::Promise<meta::cpp2::ExecResp> promise;
    auto future = promise.getFuture();
    meta::cpp2::ExecResp resp;
    auto status = MetaCache::instance().AlterEdge(req);
    if (!status.ok()) {
        resp.set_code(meta::cpp2::ErrorCode::E_UNKNOWN);
        promise.setValue(std::move(resp));
        return future;
    }
    resp.set_code(meta::cpp2::ErrorCode::SUCCEEDED);
    promise.setValue(std::move(resp));
    return future;
}

folly::Future<meta::cpp2::ExecResp>
MockMetaServiceHandler::future_dropEdge(const meta::cpp2::DropEdgeReq& req) {
    folly::Promise<meta::cpp2::ExecResp> promise;
    auto future = promise.getFuture();
    meta::cpp2::ExecResp resp;
    auto status = MetaCache::instance().dropEdge(req);
    if (!status.ok()) {
        resp.set_code(meta::cpp2::ErrorCode::E_UNKNOWN);
        promise.setValue(std::move(resp));
        return future;
    }
    resp.set_code(meta::cpp2::ErrorCode::SUCCEEDED);
    promise.setValue(std::move(resp));
    return future;
}

folly::Future<meta::cpp2::GetEdgeResp>
MockMetaServiceHandler::future_getEdge(const meta::cpp2::GetEdgeReq& req) {
    folly::Promise<meta::cpp2::GetEdgeResp> promise;
    auto future = promise.getFuture();
    meta::cpp2::GetEdgeResp resp;
    auto status = MetaCache::instance().getEdge(req);
    if (!status.ok()) {
        resp.set_code(meta::cpp2::ErrorCode::E_UNKNOWN);
        promise.setValue(std::move(resp));
        return future;
    }
    resp.set_code(meta::cpp2::ErrorCode::SUCCEEDED);
    resp.set_schema(std::move(status).value());
    promise.setValue(std::move(resp));
    return future;
}

folly::Future<meta::cpp2::ListEdgesResp>
MockMetaServiceHandler::future_listEdges(const meta::cpp2::ListEdgesReq& req) {
    folly::Promise<meta::cpp2::ListEdgesResp> promise;
    auto future = promise.getFuture();
    meta::cpp2::ListEdgesResp resp;
    auto status = MetaCache::instance().listEdges(req);
    if (!status.ok()) {
        resp.set_code(meta::cpp2::ErrorCode::E_UNKNOWN);
        promise.setValue(std::move(resp));
        return future;
    }

    resp.set_code(meta::cpp2::ErrorCode::SUCCEEDED);
    resp.set_edges(std::move(status).value());
    promise.setValue(std::move(resp));
    return future;
}

folly::Future<meta::cpp2::ExecResp>
MockMetaServiceHandler::future_createTagIndex(const meta::cpp2::CreateTagIndexReq&) {
    RETURN_SUCCESSED();
}

folly::Future<meta::cpp2::ExecResp>
MockMetaServiceHandler::future_dropTagIndex(const meta::cpp2::DropTagIndexReq&) {
    RETURN_SUCCESSED();
}

folly::Future<meta::cpp2::GetTagIndexResp>
MockMetaServiceHandler::future_getTagIndex(const meta::cpp2::GetTagIndexReq&) {
    folly::Promise<meta::cpp2::GetTagIndexResp> promise;
    auto future = promise.getFuture();
    meta::cpp2::GetTagIndexResp resp;
    resp.set_code(meta::cpp2::ErrorCode::SUCCEEDED);
    promise.setValue(std::move(resp));
    return future;
}

folly::Future<meta::cpp2::ListTagIndexesResp>
MockMetaServiceHandler::future_listTagIndexes(const meta::cpp2::ListTagIndexesReq&) {
    folly::Promise<meta::cpp2::ListTagIndexesResp> promise;
    auto future = promise.getFuture();
    meta::cpp2::ListTagIndexesResp resp;
    resp.set_code(meta::cpp2::ErrorCode::SUCCEEDED);
    promise.setValue(std::move(resp));
    return future;
}

folly::Future<meta::cpp2::ExecResp>
MockMetaServiceHandler::future_rebuildTagIndex(const meta::cpp2::RebuildIndexReq&) {
    RETURN_SUCCESSED();
}

folly::Future<meta::cpp2::ListIndexStatusResp>
MockMetaServiceHandler::future_listTagIndexStatus(const meta::cpp2::ListIndexStatusReq&) {
    folly::Promise<meta::cpp2::ListIndexStatusResp> promise;
    auto future = promise.getFuture();
    meta::cpp2::ListIndexStatusResp resp;
    resp.set_code(meta::cpp2::ErrorCode::SUCCEEDED);
    promise.setValue(std::move(resp));
    return future;
}

folly::Future<meta::cpp2::ExecResp>
MockMetaServiceHandler::future_createEdgeIndex(const meta::cpp2::CreateEdgeIndexReq&) {
    RETURN_SUCCESSED();
}

folly::Future<meta::cpp2::ExecResp>
MockMetaServiceHandler::future_dropEdgeIndex(const meta::cpp2::DropEdgeIndexReq&) {
    RETURN_SUCCESSED();
}

folly::Future<meta::cpp2::GetEdgeIndexResp>
MockMetaServiceHandler::future_getEdgeIndex(const meta::cpp2::GetEdgeIndexReq&) {
    folly::Promise<meta::cpp2::GetEdgeIndexResp> promise;
    auto future = promise.getFuture();
    meta::cpp2::GetEdgeIndexResp resp;
    resp.set_code(meta::cpp2::ErrorCode::SUCCEEDED);
    promise.setValue(std::move(resp));
    return future;
}

folly::Future<meta::cpp2::ListEdgeIndexesResp>
MockMetaServiceHandler::future_listEdgeIndexes(const meta::cpp2::ListEdgeIndexesReq&) {
    folly::Promise<meta::cpp2::ListEdgeIndexesResp> promise;
    auto future = promise.getFuture();
    meta::cpp2::ListEdgeIndexesResp resp;
    resp.set_code(meta::cpp2::ErrorCode::SUCCEEDED);
    promise.setValue(std::move(resp));
    return future;
}

folly::Future<meta::cpp2::ExecResp>
MockMetaServiceHandler::future_rebuildEdgeIndex(const meta::cpp2::RebuildIndexReq&) {
    RETURN_SUCCESSED();
}

folly::Future<meta::cpp2::ListIndexStatusResp>
MockMetaServiceHandler::future_listEdgeIndexStatus(const meta::cpp2::ListIndexStatusReq&) {
    folly::Promise<meta::cpp2::ListIndexStatusResp> promise;
    auto future = promise.getFuture();
    meta::cpp2::ListIndexStatusResp resp;
    resp.set_code(meta::cpp2::ErrorCode::SUCCEEDED);
    promise.setValue(std::move(resp));
    return future;
}

folly::Future<meta::cpp2::HBResp>
MockMetaServiceHandler::future_heartBeat(const meta::cpp2::HBReq& req) {
    folly::Promise<meta::cpp2::HBResp> promise;
    auto future = promise.getFuture();
    meta::cpp2::HBResp resp;
    auto status = MetaCache::instance().heartBeat(req);
    if (!status.ok()) {
        resp.set_code(meta::cpp2::ErrorCode::E_UNKNOWN);
        promise.setValue(std::move(resp));
        return future;
    }

    resp.set_code(meta::cpp2::ErrorCode::SUCCEEDED);
    resp.set_cluster_id(clusterId_);
    resp.set_last_update_time_in_ms(time::WallClock::fastNowInMilliSec());
    promise.setValue(std::move(resp));
    return future;
}

folly::Future<meta::cpp2::ExecResp>
MockMetaServiceHandler::future_createUser(const meta::cpp2::CreateUserReq&) {
    RETURN_SUCCESSED();
}

folly::Future<meta::cpp2::ExecResp>
MockMetaServiceHandler::future_dropUser(const meta::cpp2::DropUserReq&) {
    RETURN_SUCCESSED();
}

folly::Future<meta::cpp2::ExecResp>
MockMetaServiceHandler::future_alterUser(const meta::cpp2::AlterUserReq&) {
    RETURN_SUCCESSED();
}

folly::Future<meta::cpp2::ExecResp>
MockMetaServiceHandler::future_grantRole(const meta::cpp2::GrantRoleReq&) {
    RETURN_SUCCESSED();
}

folly::Future<meta::cpp2::ExecResp>
MockMetaServiceHandler::future_revokeRole(const meta::cpp2::RevokeRoleReq&) {
    RETURN_SUCCESSED();
}

folly::Future<meta::cpp2::ListUsersResp>
MockMetaServiceHandler::future_listUsers(const meta::cpp2::ListUsersReq&) {
    folly::Promise<meta::cpp2::ListUsersResp> promise;
    auto future = promise.getFuture();
    meta::cpp2::ListUsersResp resp;
    resp.set_code(meta::cpp2::ErrorCode::SUCCEEDED);
    promise.setValue(std::move(resp));
    return future;
}

folly::Future<meta::cpp2::ListRolesResp>
MockMetaServiceHandler::future_listRoles(const meta::cpp2::ListRolesReq&) {
    folly::Promise<meta::cpp2::ListRolesResp> promise;
    auto future = promise.getFuture();
    meta::cpp2::ListRolesResp resp;
    resp.set_code(meta::cpp2::ErrorCode::SUCCEEDED);
    promise.setValue(std::move(resp));
    return future;
}

folly::Future<meta::cpp2::ExecResp>
MockMetaServiceHandler::future_changePassword(const meta::cpp2::ChangePasswordReq&) {
    RETURN_SUCCESSED();
}

folly::Future<meta::cpp2::ListRolesResp>
MockMetaServiceHandler::future_getUserRoles(const meta::cpp2::GetUserRolesReq&) {
    folly::Promise<meta::cpp2::ListRolesResp> promise;
    auto future = promise.getFuture();
    meta::cpp2::ListRolesResp resp;
    resp.set_code(meta::cpp2::ErrorCode::SUCCEEDED);
    promise.setValue(std::move(resp));
    return future;
}

folly::Future<meta::cpp2::BalanceResp>
MockMetaServiceHandler::future_balance(const meta::cpp2::BalanceReq&) {
    folly::Promise<meta::cpp2::BalanceResp> promise;
    auto future = promise.getFuture();
    meta::cpp2::BalanceResp resp;
    resp.set_code(meta::cpp2::ErrorCode::SUCCEEDED);
    promise.setValue(std::move(resp));
    return future;
}

folly::Future<meta::cpp2::ExecResp>
MockMetaServiceHandler::future_leaderBalance(const meta::cpp2::LeaderBalanceReq&) {
    RETURN_SUCCESSED();
}

folly::Future<meta::cpp2::ExecResp>
MockMetaServiceHandler::future_regConfig(const meta::cpp2::RegConfigReq&) {
    RETURN_SUCCESSED();
}

folly::Future<meta::cpp2::GetConfigResp>
MockMetaServiceHandler::future_getConfig(const meta::cpp2::GetConfigReq&) {
    folly::Promise<meta::cpp2::GetConfigResp> promise;
    auto future = promise.getFuture();
    meta::cpp2::GetConfigResp resp;
    resp.set_code(meta::cpp2::ErrorCode::SUCCEEDED);
    promise.setValue(std::move(resp));
    return future;
}

folly::Future<meta::cpp2::ExecResp>
MockMetaServiceHandler::future_setConfig(const meta::cpp2::SetConfigReq&) {
    RETURN_SUCCESSED();
}

folly::Future<meta::cpp2::ListConfigsResp>
MockMetaServiceHandler::future_listConfigs(const meta::cpp2::ListConfigsReq&) {
    folly::Promise<meta::cpp2::ListConfigsResp> promise;
    auto future = promise.getFuture();
    meta::cpp2::ListConfigsResp resp;
    resp.set_code(meta::cpp2::ErrorCode::SUCCEEDED);
    promise.setValue(std::move(resp));
    return future;
}

folly::Future<meta::cpp2::ExecResp>
MockMetaServiceHandler::future_createSnapshot(const meta::cpp2::CreateSnapshotReq&) {
    folly::Promise<meta::cpp2::ExecResp> promise;
    auto future = promise.getFuture();
    meta::cpp2::ExecResp resp;
    auto status = MetaCache::instance().createSnapshot();
    if (!status.ok()) {
        resp.set_code(meta::cpp2::ErrorCode::E_UNKNOWN);
        promise.setValue(std::move(resp));
        return future;
    }
    resp.set_code(meta::cpp2::ErrorCode::SUCCEEDED);
    promise.setValue(std::move(resp));
    return future;
}

folly::Future<meta::cpp2::ExecResp>
MockMetaServiceHandler::future_dropSnapshot(const meta::cpp2::DropSnapshotReq& req) {
    folly::Promise<meta::cpp2::ExecResp> promise;
    auto future = promise.getFuture();
    meta::cpp2::ExecResp resp;
    auto status = MetaCache::instance().dropSnapshot(req);
    if (!status.ok()) {
        resp.set_code(meta::cpp2::ErrorCode::E_UNKNOWN);
        promise.setValue(std::move(resp));
        return future;
    }
    resp.set_code(meta::cpp2::ErrorCode::SUCCEEDED);
    promise.setValue(std::move(resp));
    return future;
}

folly::Future<meta::cpp2::ListSnapshotsResp>
MockMetaServiceHandler::future_listSnapshots(const meta::cpp2::ListSnapshotsReq&) {
    folly::Promise<meta::cpp2::ListSnapshotsResp> promise;
    auto future = promise.getFuture();
    meta::cpp2::ListSnapshotsResp resp;
    auto status = MetaCache::instance().listSnapshots();
    if (!status.ok()) {
        resp.set_code(meta::cpp2::ErrorCode::E_UNKNOWN);
        promise.setValue(std::move(resp));
        return future;
    }
    resp.set_code(meta::cpp2::ErrorCode::SUCCEEDED);
    resp.set_snapshots(std::move(status).value());
    promise.setValue(std::move(resp));
    return future;
}
}  // namespace graph
}  // namespace nebula
