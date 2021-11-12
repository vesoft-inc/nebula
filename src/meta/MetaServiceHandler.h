/* Copyright (c) 2018 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#ifndef META_METASERVICEHANDLER_H_
#define META_METASERVICEHANDLER_H_

#include "common/base/Base.h"
#include "interface/gen-cpp2/MetaService.h"
#include "kvstore/KVStore.h"
#include "meta/processors/admin/AdminClient.h"
#include "meta/processors/admin/HBProcessor.h"

namespace nebula {
namespace meta {

class MetaServiceHandler final : public cpp2::MetaServiceSvIf {
 public:
  explicit MetaServiceHandler(kvstore::KVStore* kv, ClusterID clusterId = 0)
      : kvstore_(kv), clusterId_(clusterId) {
    adminClient_ = std::make_unique<AdminClient>(kvstore_);

    // Initialize counters
    kHBCounters.init();
  }

  /**
   * Parts distribution related operations.
   * */
  folly::Future<cpp2::ExecResp> future_createSpace(const cpp2::CreateSpaceReq& req) override;

  folly::Future<cpp2::ExecResp> future_createSpaceAs(const cpp2::CreateSpaceAsReq& req) override;

  folly::Future<cpp2::ExecResp> future_dropSpace(const cpp2::DropSpaceReq& req) override;

  folly::Future<cpp2::ListSpacesResp> future_listSpaces(const cpp2::ListSpacesReq& req) override;

  folly::Future<cpp2::GetSpaceResp> future_getSpace(const cpp2::GetSpaceReq& req) override;

  folly::Future<cpp2::ListHostsResp> future_listHosts(const cpp2::ListHostsReq& req) override;

  folly::Future<cpp2::ListPartsResp> future_listParts(const cpp2::ListPartsReq& req) override;

  folly::Future<cpp2::GetPartsAllocResp> future_getPartsAlloc(
      const cpp2::GetPartsAllocReq& req) override;

  /**
   * Custom kv related operations.
   * */
  folly::Future<cpp2::ExecResp> future_multiPut(const cpp2::MultiPutReq& req) override;

  folly::Future<cpp2::GetResp> future_get(const cpp2::GetReq& req) override;

  folly::Future<cpp2::MultiGetResp> future_multiGet(const cpp2::MultiGetReq& req) override;

  folly::Future<cpp2::ExecResp> future_remove(const cpp2::RemoveReq& req) override;

  folly::Future<cpp2::ExecResp> future_removeRange(const cpp2::RemoveRangeReq& req) override;

  folly::Future<cpp2::ScanResp> future_scan(const cpp2::ScanReq& req) override;

  /**
   * Schema related operations.
   * */
  folly::Future<cpp2::ExecResp> future_createTag(const cpp2::CreateTagReq& req) override;

  folly::Future<cpp2::ExecResp> future_alterTag(const cpp2::AlterTagReq& req) override;

  folly::Future<cpp2::ExecResp> future_dropTag(const cpp2::DropTagReq& req) override;

  folly::Future<cpp2::GetTagResp> future_getTag(const cpp2::GetTagReq& req) override;

  folly::Future<cpp2::ListTagsResp> future_listTags(const cpp2::ListTagsReq& req) override;

  folly::Future<cpp2::ExecResp> future_createEdge(const cpp2::CreateEdgeReq& req) override;

  folly::Future<cpp2::ExecResp> future_alterEdge(const cpp2::AlterEdgeReq& req) override;

  folly::Future<cpp2::ExecResp> future_dropEdge(const cpp2::DropEdgeReq& req) override;

  folly::Future<cpp2::GetEdgeResp> future_getEdge(const cpp2::GetEdgeReq& req) override;

  folly::Future<cpp2::ListEdgesResp> future_listEdges(const cpp2::ListEdgesReq& req) override;

  /**
   * Index related operations.
   * */
  folly::Future<cpp2::ExecResp> future_createTagIndex(const cpp2::CreateTagIndexReq& req) override;

  folly::Future<cpp2::ExecResp> future_dropTagIndex(const cpp2::DropTagIndexReq& req) override;

  folly::Future<cpp2::GetTagIndexResp> future_getTagIndex(const cpp2::GetTagIndexReq& req) override;

  folly::Future<cpp2::ListTagIndexesResp> future_listTagIndexes(
      const cpp2::ListTagIndexesReq& req) override;

  folly::Future<cpp2::ExecResp> future_createEdgeIndex(
      const cpp2::CreateEdgeIndexReq& req) override;

  folly::Future<cpp2::ExecResp> future_dropEdgeIndex(const cpp2::DropEdgeIndexReq& req) override;

  folly::Future<cpp2::GetEdgeIndexResp> future_getEdgeIndex(
      const cpp2::GetEdgeIndexReq& req) override;

  folly::Future<cpp2::ListEdgeIndexesResp> future_listEdgeIndexes(
      const cpp2::ListEdgeIndexesReq& req) override;

  folly::Future<cpp2::ListIndexStatusResp> future_listTagIndexStatus(
      const cpp2::ListIndexStatusReq& req) override;

  folly::Future<cpp2::ListIndexStatusResp> future_listEdgeIndexStatus(
      const cpp2::ListIndexStatusReq& req) override;

  folly::Future<cpp2::ExecResp> future_signInFTService(
      const cpp2::SignInFTServiceReq& req) override;

  folly::Future<cpp2::ExecResp> future_signOutFTService(
      const cpp2::SignOutFTServiceReq& req) override;

  folly::Future<cpp2::ListFTClientsResp> future_listFTClients(
      const cpp2::ListFTClientsReq& req) override;

  folly::Future<cpp2::ExecResp> future_createFTIndex(const cpp2::CreateFTIndexReq& req) override;

  folly::Future<cpp2::ExecResp> future_dropFTIndex(const cpp2::DropFTIndexReq& req) override;

  folly::Future<cpp2::ListFTIndexesResp> future_listFTIndexes(
      const cpp2::ListFTIndexesReq& req) override;

  /**
   * User manager
   **/
  folly::Future<cpp2::ExecResp> future_createUser(const cpp2::CreateUserReq& req) override;

  folly::Future<cpp2::ExecResp> future_dropUser(const cpp2::DropUserReq& req) override;

  folly::Future<cpp2::ExecResp> future_alterUser(const cpp2::AlterUserReq& req) override;

  folly::Future<cpp2::ExecResp> future_grantRole(const cpp2::GrantRoleReq& req) override;

  folly::Future<cpp2::ExecResp> future_revokeRole(const cpp2::RevokeRoleReq& req) override;

  folly::Future<cpp2::ListUsersResp> future_listUsers(const cpp2::ListUsersReq& req) override;

  folly::Future<cpp2::ListRolesResp> future_listRoles(const cpp2::ListRolesReq& req) override;

  folly::Future<cpp2::ExecResp> future_changePassword(const cpp2::ChangePasswordReq& req) override;

  folly::Future<cpp2::ListRolesResp> future_getUserRoles(const cpp2::GetUserRolesReq& req) override;

  /**
   * HeartBeat
   * */
  folly::Future<cpp2::HBResp> future_heartBeat(const cpp2::HBReq& req) override;

  folly::Future<cpp2::BalanceResp> future_balance(const cpp2::BalanceReq& req) override;

  folly::Future<cpp2::ExecResp> future_leaderBalance(const cpp2::LeaderBalanceReq& req) override;

  folly::Future<cpp2::ExecResp> future_regConfig(const cpp2::RegConfigReq& req) override;

  folly::Future<cpp2::GetConfigResp> future_getConfig(const cpp2::GetConfigReq& req) override;

  folly::Future<cpp2::ExecResp> future_setConfig(const cpp2::SetConfigReq& req) override;

  folly::Future<cpp2::ListConfigsResp> future_listConfigs(const cpp2::ListConfigsReq& req) override;

  folly::Future<cpp2::ExecResp> future_createSnapshot(const cpp2::CreateSnapshotReq& req) override;

  folly::Future<cpp2::ExecResp> future_dropSnapshot(const cpp2::DropSnapshotReq& req) override;

  folly::Future<cpp2::ListSnapshotsResp> future_listSnapshots(
      const cpp2::ListSnapshotsReq& req) override;

  folly::Future<cpp2::AdminJobResp> future_runAdminJob(const cpp2::AdminJobReq& req) override;

  folly::Future<cpp2::CreateBackupResp> future_createBackup(
      const cpp2::CreateBackupReq& req) override;
  /**
   * Zone manager
   **/
  folly::Future<cpp2::ExecResp> future_addZone(const cpp2::AddZoneReq& req) override;

  folly::Future<cpp2::ExecResp> future_dropZone(const cpp2::DropZoneReq& req) override;

  folly::Future<cpp2::GetZoneResp> future_getZone(const cpp2::GetZoneReq& req) override;

  folly::Future<cpp2::ListZonesResp> future_listZones(const cpp2::ListZonesReq& req) override;

  folly::Future<cpp2::ExecResp> future_addHostIntoZone(
      const cpp2::AddHostIntoZoneReq& req) override;

  folly::Future<cpp2::ExecResp> future_dropHostFromZone(
      const cpp2::DropHostFromZoneReq& req) override;

  folly::Future<cpp2::ExecResp> future_addGroup(const cpp2::AddGroupReq& req) override;

  folly::Future<cpp2::ExecResp> future_dropGroup(const cpp2::DropGroupReq& req) override;

  folly::Future<cpp2::GetGroupResp> future_getGroup(const cpp2::GetGroupReq& req) override;

  folly::Future<cpp2::ListGroupsResp> future_listGroups(const cpp2::ListGroupsReq& req) override;

  folly::Future<cpp2::ExecResp> future_addZoneIntoGroup(
      const cpp2::AddZoneIntoGroupReq& req) override;

  folly::Future<cpp2::ExecResp> future_dropZoneFromGroup(
      const cpp2::DropZoneFromGroupReq& req) override;

  // listener
  folly::Future<cpp2::ExecResp> future_addListener(const cpp2::AddListenerReq& req) override;

  folly::Future<cpp2::ExecResp> future_removeListener(const cpp2::RemoveListenerReq& req) override;

  folly::Future<cpp2::ListListenerResp> future_listListener(
      const cpp2::ListListenerReq& req) override;

  folly::Future<cpp2::ExecResp> future_restoreMeta(const cpp2::RestoreMetaReq& req) override;

  folly::Future<cpp2::GetStatsResp> future_getStats(const cpp2::GetStatsReq& req) override;

  folly::Future<cpp2::ExecResp> future_reportTaskFinish(const cpp2::ReportTaskReq& req) override;

  folly::Future<cpp2::ListClusterInfoResp> future_listCluster(
      const cpp2::ListClusterInfoReq& req) override;

  folly::Future<cpp2::GetMetaDirInfoResp> future_getMetaDirInfo(
      const cpp2::GetMetaDirInfoReq& req) override;
  folly::Future<cpp2::CreateSessionResp> future_createSession(
      const cpp2::CreateSessionReq& req) override;

  folly::Future<cpp2::UpdateSessionsResp> future_updateSessions(
      const cpp2::UpdateSessionsReq& req) override;

  folly::Future<cpp2::ListSessionsResp> future_listSessions(
      const cpp2::ListSessionsReq& req) override;

  folly::Future<cpp2::GetSessionResp> future_getSession(const cpp2::GetSessionReq& req) override;

  folly::Future<cpp2::ExecResp> future_removeSession(const cpp2::RemoveSessionReq& req) override;

  folly::Future<cpp2::ExecResp> future_killQuery(const cpp2::KillQueryReq& req) override;

  folly::Future<cpp2::VerifyClientVersionResp> future_verifyClientVersion(
      const cpp2::VerifyClientVersionReq& req) override;

 private:
  kvstore::KVStore* kvstore_ = nullptr;
  ClusterID clusterId_{0};
  std::unique_ptr<AdminClient> adminClient_;
};

}  // namespace meta
}  // namespace nebula
#endif  // META_METASERVICEHANDLER_H_
