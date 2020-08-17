/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef EXECUTOR_MOCKMETASERVICEHANDLER_H_
#define EXECUTOR_MOCKMETASERVICEHANDLER_H_

#include "common/base/Base.h"
#include "common/interface/gen-cpp2/MetaService.h"
#include <folly/futures/Promise.h>
#include <folly/futures/Future.h>

namespace nebula {
namespace graph {

class MockMetaServiceHandler final : public meta::cpp2::MetaServiceSvIf {
public:
    MockMetaServiceHandler() : clusterId_(100) {}

    /**
     * Parts distribution related operations.
     * */
    folly::Future<meta::cpp2::ExecResp>
    future_createSpace(const meta::cpp2::CreateSpaceReq& req) override;

    folly::Future<meta::cpp2::ExecResp>
    future_dropSpace(const meta::cpp2::DropSpaceReq& req) override;

    folly::Future<meta::cpp2::ListSpacesResp>
    future_listSpaces(const meta::cpp2::ListSpacesReq& req) override;

    folly::Future<meta::cpp2::GetSpaceResp>
    future_getSpace(const meta::cpp2::GetSpaceReq& req) override;

    folly::Future<meta::cpp2::ListHostsResp>
    future_listHosts(const meta::cpp2::ListHostsReq& req) override;

    folly::Future<meta::cpp2::ListPartsResp>
    future_listParts(const meta::cpp2::ListPartsReq& req) override;

    folly::Future<meta::cpp2::GetPartsAllocResp>
    future_getPartsAlloc(const meta::cpp2::GetPartsAllocReq& req) override;

    /**
     * Custom kv related operations.
     * */
    folly::Future<meta::cpp2::ExecResp>
    future_multiPut(const meta::cpp2::MultiPutReq& req) override;

    folly::Future<meta::cpp2::GetResp>
    future_get(const meta::cpp2::GetReq& req) override;

    folly::Future<meta::cpp2::MultiGetResp>
    future_multiGet(const meta::cpp2::MultiGetReq& req) override;

    folly::Future<meta::cpp2::ExecResp>
    future_remove(const meta::cpp2::RemoveReq& req) override;

    folly::Future<meta::cpp2::ExecResp>
    future_removeRange(const meta::cpp2::RemoveRangeReq& req) override;

    folly::Future<meta::cpp2::ScanResp>
    future_scan(const meta::cpp2::ScanReq& req) override;

    /**
     * Schema related operations.
     * */
    folly::Future<meta::cpp2::ExecResp>
    future_createTag(const meta::cpp2::CreateTagReq& req) override;

    folly::Future<meta::cpp2::ExecResp>
    future_alterTag(const meta::cpp2::AlterTagReq& req) override;

    folly::Future<meta::cpp2::ExecResp>
    future_dropTag(const meta::cpp2::DropTagReq& req) override;

    folly::Future<meta::cpp2::GetTagResp>
    future_getTag(const meta::cpp2::GetTagReq &req) override;

    folly::Future<meta::cpp2::ListTagsResp>
    future_listTags(const meta::cpp2::ListTagsReq& req) override;

    folly::Future<meta::cpp2::ExecResp>
    future_createEdge(const meta::cpp2::CreateEdgeReq& req) override;

    folly::Future<meta::cpp2::ExecResp>
    future_alterEdge(const meta::cpp2::AlterEdgeReq& req) override;

    folly::Future<meta::cpp2::ExecResp>
    future_dropEdge(const meta::cpp2::DropEdgeReq& req) override;

    folly::Future<meta::cpp2::GetEdgeResp>
    future_getEdge(const meta::cpp2::GetEdgeReq& req) override;

    folly::Future<meta::cpp2::ListEdgesResp>
    future_listEdges(const meta::cpp2::ListEdgesReq& req) override;

    /**
     * Index related operations.
     * */
    folly::Future<meta::cpp2::ExecResp>
    future_createTagIndex(const meta::cpp2::CreateTagIndexReq& req) override;

    folly::Future<meta::cpp2::ExecResp>
    future_dropTagIndex(const meta::cpp2::DropTagIndexReq& req) override;

    folly::Future<meta::cpp2::GetTagIndexResp>
    future_getTagIndex(const meta::cpp2::GetTagIndexReq &req) override;

    folly::Future<meta::cpp2::ListTagIndexesResp>
    future_listTagIndexes(const meta::cpp2::ListTagIndexesReq& req) override;

    folly::Future<meta::cpp2::ExecResp>
    future_rebuildTagIndex(const meta::cpp2::RebuildIndexReq& req) override;

    folly::Future<meta::cpp2::ExecResp>
    future_createEdgeIndex(const meta::cpp2::CreateEdgeIndexReq& req) override;

    folly::Future<meta::cpp2::ExecResp>
    future_dropEdgeIndex(const meta::cpp2::DropEdgeIndexReq& req) override;

    folly::Future<meta::cpp2::GetEdgeIndexResp>
    future_getEdgeIndex(const meta::cpp2::GetEdgeIndexReq& req) override;

    folly::Future<meta::cpp2::ListEdgeIndexesResp>
    future_listEdgeIndexes(const meta::cpp2::ListEdgeIndexesReq& req) override;

    folly::Future<meta::cpp2::ExecResp>
    future_rebuildEdgeIndex(const meta::cpp2::RebuildIndexReq& req) override;

    folly::Future<meta::cpp2::ListIndexStatusResp>
    future_listTagIndexStatus(const meta::cpp2::ListIndexStatusReq& req) override;

    folly::Future<meta::cpp2::ListIndexStatusResp>
    future_listEdgeIndexStatus(const meta::cpp2::ListIndexStatusReq& req) override;

    /**
     * User manager
     **/
    folly::Future<meta::cpp2::ExecResp>
    future_createUser(const meta::cpp2::CreateUserReq& req) override;

    folly::Future<meta::cpp2::ExecResp>
    future_dropUser(const meta::cpp2::DropUserReq& req) override;

    folly::Future<meta::cpp2::ExecResp>
    future_alterUser(const meta::cpp2::AlterUserReq& req) override;

    folly::Future<meta::cpp2::ExecResp>
    future_grantRole(const meta::cpp2::GrantRoleReq& req) override;

    folly::Future<meta::cpp2::ExecResp>
    future_revokeRole(const meta::cpp2::RevokeRoleReq& req) override;

    folly::Future<meta::cpp2::ListUsersResp>
    future_listUsers(const meta::cpp2::ListUsersReq& req) override;

    folly::Future<meta::cpp2::ListRolesResp>
    future_listRoles(const meta::cpp2::ListRolesReq& req) override;

    folly::Future<meta::cpp2::ExecResp>
    future_changePassword(const meta::cpp2::ChangePasswordReq& req) override;

    folly::Future<meta::cpp2::ListRolesResp>
    future_getUserRoles(const meta::cpp2::GetUserRolesReq& req) override;

    /**
     * HeartBeat
     * */
    folly::Future<meta::cpp2::HBResp>
    future_heartBeat(const meta::cpp2::HBReq& req) override;

    folly::Future<meta::cpp2::BalanceResp>
    future_balance(const meta::cpp2::BalanceReq& req) override;

    folly::Future<meta::cpp2::ExecResp>
    future_leaderBalance(const meta::cpp2::LeaderBalanceReq& req) override;

    folly::Future<meta::cpp2::ExecResp>
    future_regConfig(const meta::cpp2::RegConfigReq &req) override;

    folly::Future<meta::cpp2::GetConfigResp>
    future_getConfig(const meta::cpp2::GetConfigReq &req) override;

    folly::Future<meta::cpp2::ExecResp>
    future_setConfig(const meta::cpp2::SetConfigReq &req) override;

    folly::Future<meta::cpp2::ListConfigsResp>
    future_listConfigs(const meta::cpp2::ListConfigsReq &req) override;

    folly::Future<meta::cpp2::ExecResp>
    future_createSnapshot(const meta::cpp2::CreateSnapshotReq& req) override;

    folly::Future<meta::cpp2::ExecResp>
    future_dropSnapshot(const meta::cpp2::DropSnapshotReq& req) override;

    folly::Future<meta::cpp2::ListSnapshotsResp>
    future_listSnapshots(const meta::cpp2::ListSnapshotsReq& req) override;

    folly::Future<meta::cpp2::AdminJobResp>
    future_runAdminJob(const meta::cpp2::AdminJobReq& req) override;

private:
    ClusterID clusterId_{0};
};
}  // namespace graph
}  // namespace nebula
#endif  // EXECUTOR_MOCKMETASERVICEHANDLER_H_
