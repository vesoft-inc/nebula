/* Copyright (c) 2018 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#include "meta/MetaServiceHandler.h"

#include "common/utils/MetaKeyUtils.h"
#include "meta/processors/admin/BalanceProcessor.h"
#include "meta/processors/admin/CreateBackupProcessor.h"
#include "meta/processors/admin/CreateSnapshotProcessor.h"
#include "meta/processors/admin/DropSnapshotProcessor.h"
#include "meta/processors/admin/GetMetaDirInfoProcessor.h"
#include "meta/processors/admin/HBProcessor.h"
#include "meta/processors/admin/LeaderBalanceProcessor.h"
#include "meta/processors/admin/ListClusterInfoProcessor.h"
#include "meta/processors/admin/ListSnapshotsProcessor.h"
#include "meta/processors/admin/RestoreProcessor.h"
#include "meta/processors/admin/VerifyClientVersionProcessor.h"
#include "meta/processors/config/GetConfigProcessor.h"
#include "meta/processors/config/ListConfigsProcessor.h"
#include "meta/processors/config/RegConfigProcessor.h"
#include "meta/processors/config/SetConfigProcessor.h"
#include "meta/processors/index/CreateEdgeIndexProcessor.h"
#include "meta/processors/index/CreateTagIndexProcessor.h"
#include "meta/processors/index/DropEdgeIndexProcessor.h"
#include "meta/processors/index/DropTagIndexProcessor.h"
#include "meta/processors/index/FTIndexProcessor.h"
#include "meta/processors/index/FTServiceProcessor.h"
#include "meta/processors/index/GetEdgeIndexProcessor.h"
#include "meta/processors/index/GetTagIndexProcessor.h"
#include "meta/processors/index/ListEdgeIndexesProcessor.h"
#include "meta/processors/index/ListTagIndexesProcessor.h"
#include "meta/processors/job/AdminJobProcessor.h"
#include "meta/processors/job/GetStatsProcessor.h"
#include "meta/processors/job/ListEdgeIndexStatusProcessor.h"
#include "meta/processors/job/ListTagIndexStatusProcessor.h"
#include "meta/processors/job/ReportTaskProcessor.h"
#include "meta/processors/kv/GetProcessor.h"
#include "meta/processors/kv/MultiGetProcessor.h"
#include "meta/processors/kv/MultiPutProcessor.h"
#include "meta/processors/kv/RemoveProcessor.h"
#include "meta/processors/kv/RemoveRangeProcessor.h"
#include "meta/processors/kv/ScanProcessor.h"
#include "meta/processors/listener/ListenerProcessor.h"
#include "meta/processors/parts/CreateSpaceAsProcessor.h"
#include "meta/processors/parts/CreateSpaceProcessor.h"
#include "meta/processors/parts/DropSpaceProcessor.h"
#include "meta/processors/parts/GetPartsAllocProcessor.h"
#include "meta/processors/parts/GetSpaceProcessor.h"
#include "meta/processors/parts/ListHostsProcessor.h"
#include "meta/processors/parts/ListPartsProcessor.h"
#include "meta/processors/parts/ListSpacesProcessor.h"
#include "meta/processors/schema/AlterEdgeProcessor.h"
#include "meta/processors/schema/AlterTagProcessor.h"
#include "meta/processors/schema/CreateEdgeProcessor.h"
#include "meta/processors/schema/CreateTagProcessor.h"
#include "meta/processors/schema/DropEdgeProcessor.h"
#include "meta/processors/schema/DropTagProcessor.h"
#include "meta/processors/schema/GetEdgeProcessor.h"
#include "meta/processors/schema/GetTagProcessor.h"
#include "meta/processors/schema/ListEdgesProcessor.h"
#include "meta/processors/schema/ListTagsProcessor.h"
#include "meta/processors/session/SessionManagerProcessor.h"
#include "meta/processors/user/AuthenticationProcessor.h"
#include "meta/processors/zone/AddGroupProcessor.h"
#include "meta/processors/zone/AddZoneProcessor.h"
#include "meta/processors/zone/DropGroupProcessor.h"
#include "meta/processors/zone/DropZoneProcessor.h"
#include "meta/processors/zone/GetGroupProcessor.h"
#include "meta/processors/zone/GetZoneProcessor.h"
#include "meta/processors/zone/ListGroupsProcessor.h"
#include "meta/processors/zone/ListZonesProcessor.h"
#include "meta/processors/zone/UpdateGroupProcessor.h"
#include "meta/processors/zone/UpdateZoneProcessor.h"

#define RETURN_FUTURE(processor)   \
  auto f = processor->getFuture(); \
  processor->process(req);         \
  return f;

namespace nebula {
namespace meta {

folly::Future<cpp2::ExecResp> MetaServiceHandler::future_createSpace(
    const cpp2::CreateSpaceReq& req) {
  auto* processor = CreateSpaceProcessor::instance(kvstore_);
  RETURN_FUTURE(processor);
}

folly::Future<cpp2::ExecResp> MetaServiceHandler::future_createSpaceAs(
    const cpp2::CreateSpaceAsReq& req) {
  auto* processor = CreateSpaceAsProcessor::instance(kvstore_);
  RETURN_FUTURE(processor);
}

folly::Future<cpp2::ExecResp> MetaServiceHandler::future_dropSpace(const cpp2::DropSpaceReq& req) {
  auto* processor = DropSpaceProcessor::instance(kvstore_);
  RETURN_FUTURE(processor);
}

folly::Future<cpp2::ListSpacesResp> MetaServiceHandler::future_listSpaces(
    const cpp2::ListSpacesReq& req) {
  auto* processor = ListSpacesProcessor::instance(kvstore_);
  RETURN_FUTURE(processor);
}

folly::Future<cpp2::AdminJobResp> MetaServiceHandler::future_runAdminJob(
    const cpp2::AdminJobReq& req) {
  auto* processor = AdminJobProcessor::instance(kvstore_, adminClient_.get());
  RETURN_FUTURE(processor);
}

folly::Future<cpp2::ExecResp> MetaServiceHandler::future_reportTaskFinish(
    const cpp2::ReportTaskReq& req) {
  auto* processor = ReportTaskProcessor::instance(kvstore_);
  RETURN_FUTURE(processor);
}

folly::Future<cpp2::GetSpaceResp> MetaServiceHandler::future_getSpace(
    const cpp2::GetSpaceReq& req) {
  auto* processor = GetSpaceProcessor::instance(kvstore_);
  RETURN_FUTURE(processor);
}

folly::Future<cpp2::ListHostsResp> MetaServiceHandler::future_listHosts(
    const cpp2::ListHostsReq& req) {
  auto* processor = ListHostsProcessor::instance(kvstore_);
  RETURN_FUTURE(processor);
}

folly::Future<cpp2::ListPartsResp> MetaServiceHandler::future_listParts(
    const cpp2::ListPartsReq& req) {
  auto* processor = ListPartsProcessor::instance(kvstore_);
  RETURN_FUTURE(processor);
}

folly::Future<cpp2::GetPartsAllocResp> MetaServiceHandler::future_getPartsAlloc(
    const cpp2::GetPartsAllocReq& req) {
  auto* processor = GetPartsAllocProcessor::instance(kvstore_);
  RETURN_FUTURE(processor);
}

folly::Future<cpp2::ExecResp> MetaServiceHandler::future_multiPut(const cpp2::MultiPutReq& req) {
  auto* processor = MultiPutProcessor::instance(kvstore_);
  RETURN_FUTURE(processor);
}

folly::Future<cpp2::GetResp> MetaServiceHandler::future_get(const cpp2::GetReq& req) {
  auto* processor = GetProcessor::instance(kvstore_);
  RETURN_FUTURE(processor);
}

folly::Future<cpp2::MultiGetResp> MetaServiceHandler::future_multiGet(
    const cpp2::MultiGetReq& req) {
  auto* processor = MultiGetProcessor::instance(kvstore_);
  RETURN_FUTURE(processor);
}

folly::Future<cpp2::ScanResp> MetaServiceHandler::future_scan(const cpp2::ScanReq& req) {
  auto* processor = ScanProcessor::instance(kvstore_);
  RETURN_FUTURE(processor);
}

folly::Future<cpp2::ExecResp> MetaServiceHandler::future_remove(const cpp2::RemoveReq& req) {
  auto* processor = RemoveProcessor::instance(kvstore_);
  RETURN_FUTURE(processor);
}

folly::Future<cpp2::ExecResp> MetaServiceHandler::future_removeRange(
    const cpp2::RemoveRangeReq& req) {
  auto* processor = RemoveRangeProcessor::instance(kvstore_);
  RETURN_FUTURE(processor);
}

folly::Future<cpp2::ExecResp> MetaServiceHandler::future_createTag(const cpp2::CreateTagReq& req) {
  auto* processor = CreateTagProcessor::instance(kvstore_);
  RETURN_FUTURE(processor);
}

folly::Future<cpp2::ExecResp> MetaServiceHandler::future_alterTag(const cpp2::AlterTagReq& req) {
  auto* processor = AlterTagProcessor::instance(kvstore_);
  RETURN_FUTURE(processor);
}

folly::Future<cpp2::ExecResp> MetaServiceHandler::future_dropTag(const cpp2::DropTagReq& req) {
  auto* processor = DropTagProcessor::instance(kvstore_);
  RETURN_FUTURE(processor);
}

folly::Future<cpp2::GetTagResp> MetaServiceHandler::future_getTag(const cpp2::GetTagReq& req) {
  auto* processor = GetTagProcessor::instance(kvstore_);
  RETURN_FUTURE(processor);
}

folly::Future<cpp2::ListTagsResp> MetaServiceHandler::future_listTags(
    const cpp2::ListTagsReq& req) {
  auto* processor = ListTagsProcessor::instance(kvstore_);
  RETURN_FUTURE(processor);
}

folly::Future<cpp2::ExecResp> MetaServiceHandler::future_createEdge(
    const cpp2::CreateEdgeReq& req) {
  auto* processor = CreateEdgeProcessor::instance(kvstore_);
  RETURN_FUTURE(processor);
}

folly::Future<cpp2::ExecResp> MetaServiceHandler::future_alterEdge(const cpp2::AlterEdgeReq& req) {
  auto* processor = AlterEdgeProcessor::instance(kvstore_);
  RETURN_FUTURE(processor);
}

folly::Future<cpp2::ExecResp> MetaServiceHandler::future_dropEdge(const cpp2::DropEdgeReq& req) {
  auto* processor = DropEdgeProcessor::instance(kvstore_);
  RETURN_FUTURE(processor);
}

folly::Future<cpp2::GetEdgeResp> MetaServiceHandler::future_getEdge(const cpp2::GetEdgeReq& req) {
  auto* processor = GetEdgeProcessor::instance(kvstore_);
  RETURN_FUTURE(processor);
}

folly::Future<cpp2::ListEdgesResp> MetaServiceHandler::future_listEdges(
    const cpp2::ListEdgesReq& req) {
  auto* processor = ListEdgesProcessor::instance(kvstore_);
  RETURN_FUTURE(processor);
}

folly::Future<cpp2::ExecResp> MetaServiceHandler::future_createTagIndex(
    const cpp2::CreateTagIndexReq& req) {
  auto* processor = CreateTagIndexProcessor::instance(kvstore_);
  RETURN_FUTURE(processor);
}

folly::Future<cpp2::ExecResp> MetaServiceHandler::future_dropTagIndex(
    const cpp2::DropTagIndexReq& req) {
  auto* processor = DropTagIndexProcessor::instance(kvstore_);
  RETURN_FUTURE(processor);
}

folly::Future<cpp2::GetTagIndexResp> MetaServiceHandler::future_getTagIndex(
    const cpp2::GetTagIndexReq& req) {
  auto* processor = GetTagIndexProcessor::instance(kvstore_);
  RETURN_FUTURE(processor);
}

folly::Future<cpp2::ListTagIndexesResp> MetaServiceHandler::future_listTagIndexes(
    const cpp2::ListTagIndexesReq& req) {
  auto* processor = ListTagIndexesProcessor::instance(kvstore_);
  RETURN_FUTURE(processor);
}

folly::Future<cpp2::ListIndexStatusResp> MetaServiceHandler::future_listTagIndexStatus(
    const cpp2::ListIndexStatusReq& req) {
  auto* processor = ListTagIndexStatusProcessor::instance(kvstore_);
  RETURN_FUTURE(processor);
}

folly::Future<cpp2::ExecResp> MetaServiceHandler::future_createEdgeIndex(
    const cpp2::CreateEdgeIndexReq& req) {
  auto* processor = CreateEdgeIndexProcessor::instance(kvstore_);
  RETURN_FUTURE(processor);
}

folly::Future<cpp2::ExecResp> MetaServiceHandler::future_dropEdgeIndex(
    const cpp2::DropEdgeIndexReq& req) {
  auto* processor = DropEdgeIndexProcessor::instance(kvstore_);
  RETURN_FUTURE(processor);
}

folly::Future<cpp2::GetEdgeIndexResp> MetaServiceHandler::future_getEdgeIndex(
    const cpp2::GetEdgeIndexReq& req) {
  auto* processor = GetEdgeIndexProcessor::instance(kvstore_);
  RETURN_FUTURE(processor);
}

folly::Future<cpp2::ListEdgeIndexesResp> MetaServiceHandler::future_listEdgeIndexes(
    const cpp2::ListEdgeIndexesReq& req) {
  auto* processor = ListEdgeIndexesProcessor::instance(kvstore_);
  RETURN_FUTURE(processor);
}

folly::Future<cpp2::ListIndexStatusResp> MetaServiceHandler::future_listEdgeIndexStatus(
    const cpp2::ListIndexStatusReq& req) {
  auto* processor = ListEdgeIndexStatusProcessor::instance(kvstore_);
  RETURN_FUTURE(processor);
}

folly::Future<cpp2::ExecResp> MetaServiceHandler::future_signInFTService(
    const cpp2::SignInFTServiceReq& req) {
  auto* processor = SignInFTServiceProcessor::instance(kvstore_);
  RETURN_FUTURE(processor);
}

folly::Future<cpp2::ExecResp> MetaServiceHandler::future_signOutFTService(
    const cpp2::SignOutFTServiceReq& req) {
  auto* processor = SignOutFTServiceProcessor::instance(kvstore_);
  RETURN_FUTURE(processor);
}

folly::Future<cpp2::ListFTClientsResp> MetaServiceHandler::future_listFTClients(
    const cpp2::ListFTClientsReq& req) {
  auto* processor = ListFTClientsProcessor::instance(kvstore_);
  RETURN_FUTURE(processor);
}

folly::Future<cpp2::ExecResp> MetaServiceHandler::future_createFTIndex(
    const cpp2::CreateFTIndexReq& req) {
  auto* processor = CreateFTIndexProcessor::instance(kvstore_);
  RETURN_FUTURE(processor);
}

folly::Future<cpp2::ExecResp> MetaServiceHandler::future_dropFTIndex(
    const cpp2::DropFTIndexReq& req) {
  auto* processor = DropFTIndexProcessor::instance(kvstore_);
  RETURN_FUTURE(processor);
}

folly::Future<cpp2::ListFTIndexesResp> MetaServiceHandler::future_listFTIndexes(
    const cpp2::ListFTIndexesReq& req) {
  auto* processor = ListFTIndexesProcessor::instance(kvstore_);
  RETURN_FUTURE(processor);
}

folly::Future<cpp2::HBResp> MetaServiceHandler::future_heartBeat(const cpp2::HBReq& req) {
  auto* processor = HBProcessor::instance(kvstore_, &kHBCounters, clusterId_);
  RETURN_FUTURE(processor);
}

folly::Future<cpp2::ExecResp> MetaServiceHandler::future_createUser(
    const cpp2::CreateUserReq& req) {
  auto* processor = CreateUserProcessor::instance(kvstore_);
  RETURN_FUTURE(processor);
}

folly::Future<cpp2::ExecResp> MetaServiceHandler::future_dropUser(const cpp2::DropUserReq& req) {
  auto* processor = DropUserProcessor::instance(kvstore_);
  RETURN_FUTURE(processor);
}

folly::Future<cpp2::ExecResp> MetaServiceHandler::future_alterUser(const cpp2::AlterUserReq& req) {
  auto* processor = AlterUserProcessor::instance(kvstore_);
  RETURN_FUTURE(processor);
}

folly::Future<cpp2::ExecResp> MetaServiceHandler::future_grantRole(const cpp2::GrantRoleReq& req) {
  auto* processor = GrantProcessor::instance(kvstore_);
  RETURN_FUTURE(processor);
}

folly::Future<cpp2::ExecResp> MetaServiceHandler::future_revokeRole(
    const cpp2::RevokeRoleReq& req) {
  auto* processor = RevokeProcessor::instance(kvstore_);
  RETURN_FUTURE(processor);
}

folly::Future<cpp2::ListUsersResp> MetaServiceHandler::future_listUsers(
    const cpp2::ListUsersReq& req) {
  auto* processor = ListUsersProcessor::instance(kvstore_);
  RETURN_FUTURE(processor);
}

folly::Future<cpp2::ListRolesResp> MetaServiceHandler::future_listRoles(
    const cpp2::ListRolesReq& req) {
  auto* processor = ListRolesProcessor::instance(kvstore_);
  RETURN_FUTURE(processor);
}

folly::Future<cpp2::ExecResp> MetaServiceHandler::future_changePassword(
    const cpp2::ChangePasswordReq& req) {
  auto* processor = ChangePasswordProcessor::instance(kvstore_);
  RETURN_FUTURE(processor);
}

folly::Future<cpp2::ListRolesResp> MetaServiceHandler::future_getUserRoles(
    const cpp2::GetUserRolesReq& req) {
  auto* processor = GetUserRolesProcessor::instance(kvstore_);
  RETURN_FUTURE(processor);
}

folly::Future<cpp2::BalanceResp> MetaServiceHandler::future_balance(const cpp2::BalanceReq& req) {
  auto* processor = BalanceProcessor::instance(kvstore_);
  RETURN_FUTURE(processor);
}

folly::Future<cpp2::ExecResp> MetaServiceHandler::future_leaderBalance(
    const cpp2::LeaderBalanceReq& req) {
  auto* processor = LeaderBalanceProcessor::instance(kvstore_);
  RETURN_FUTURE(processor);
}

folly::Future<cpp2::ExecResp> MetaServiceHandler::future_regConfig(const cpp2::RegConfigReq& req) {
  auto* processor = RegConfigProcessor::instance(kvstore_);
  RETURN_FUTURE(processor);
}

folly::Future<cpp2::GetConfigResp> MetaServiceHandler::future_getConfig(
    const cpp2::GetConfigReq& req) {
  auto* processor = GetConfigProcessor::instance(kvstore_);
  RETURN_FUTURE(processor);
}

folly::Future<cpp2::ExecResp> MetaServiceHandler::future_setConfig(const cpp2::SetConfigReq& req) {
  auto* processor = SetConfigProcessor::instance(kvstore_);
  RETURN_FUTURE(processor);
}

folly::Future<cpp2::ListConfigsResp> MetaServiceHandler::future_listConfigs(
    const cpp2::ListConfigsReq& req) {
  auto* processor = ListConfigsProcessor::instance(kvstore_);
  RETURN_FUTURE(processor);
}

folly::Future<cpp2::ExecResp> MetaServiceHandler::future_createSnapshot(
    const cpp2::CreateSnapshotReq& req) {
  auto* processor = CreateSnapshotProcessor::instance(kvstore_, adminClient_.get());
  RETURN_FUTURE(processor);
}

folly::Future<cpp2::ExecResp> MetaServiceHandler::future_dropSnapshot(
    const cpp2::DropSnapshotReq& req) {
  auto* processor = DropSnapshotProcessor::instance(kvstore_, adminClient_.get());
  RETURN_FUTURE(processor);
}

folly::Future<cpp2::ListSnapshotsResp> MetaServiceHandler::future_listSnapshots(
    const cpp2::ListSnapshotsReq& req) {
  auto* processor = ListSnapshotsProcessor::instance(kvstore_);
  RETURN_FUTURE(processor);
}

folly::Future<cpp2::CreateBackupResp> MetaServiceHandler::future_createBackup(
    const cpp2::CreateBackupReq& req) {
  auto* processor = CreateBackupProcessor::instance(kvstore_, adminClient_.get());
  RETURN_FUTURE(processor);
}

folly::Future<cpp2::ExecResp> MetaServiceHandler::future_addZone(const cpp2::AddZoneReq& req) {
  auto* processor = AddZoneProcessor::instance(kvstore_);
  RETURN_FUTURE(processor);
}

folly::Future<cpp2::ExecResp> MetaServiceHandler::future_dropZone(const cpp2::DropZoneReq& req) {
  auto* processor = DropZoneProcessor::instance(kvstore_);
  RETURN_FUTURE(processor);
}

folly::Future<cpp2::GetZoneResp> MetaServiceHandler::future_getZone(const cpp2::GetZoneReq& req) {
  auto* processor = GetZoneProcessor::instance(kvstore_);
  RETURN_FUTURE(processor);
}

folly::Future<cpp2::ListZonesResp> MetaServiceHandler::future_listZones(
    const cpp2::ListZonesReq& req) {
  auto* processor = ListZonesProcessor::instance(kvstore_);
  RETURN_FUTURE(processor);
}

folly::Future<cpp2::ExecResp> MetaServiceHandler::future_addHostIntoZone(
    const cpp2::AddHostIntoZoneReq& req) {
  auto* processor = AddHostIntoZoneProcessor::instance(kvstore_);
  RETURN_FUTURE(processor);
}

folly::Future<cpp2::ExecResp> MetaServiceHandler::future_dropHostFromZone(
    const cpp2::DropHostFromZoneReq& req) {
  auto* processor = DropHostFromZoneProcessor::instance(kvstore_);
  RETURN_FUTURE(processor);
}

folly::Future<cpp2::ExecResp> MetaServiceHandler::future_addGroup(const cpp2::AddGroupReq& req) {
  auto* processor = AddGroupProcessor::instance(kvstore_);
  RETURN_FUTURE(processor);
}

folly::Future<cpp2::ExecResp> MetaServiceHandler::future_dropGroup(const cpp2::DropGroupReq& req) {
  auto* processor = DropGroupProcessor::instance(kvstore_);
  RETURN_FUTURE(processor);
}

folly::Future<cpp2::GetGroupResp> MetaServiceHandler::future_getGroup(
    const cpp2::GetGroupReq& req) {
  auto* processor = GetGroupProcessor::instance(kvstore_);
  RETURN_FUTURE(processor);
}

folly::Future<cpp2::ListGroupsResp> MetaServiceHandler::future_listGroups(
    const cpp2::ListGroupsReq& req) {
  auto* processor = ListGroupsProcessor::instance(kvstore_);
  RETURN_FUTURE(processor);
}

folly::Future<cpp2::ExecResp> MetaServiceHandler::future_addZoneIntoGroup(
    const cpp2::AddZoneIntoGroupReq& req) {
  auto* processor = AddZoneIntoGroupProcessor::instance(kvstore_);
  RETURN_FUTURE(processor);
}

folly::Future<cpp2::ExecResp> MetaServiceHandler::future_dropZoneFromGroup(
    const cpp2::DropZoneFromGroupReq& req) {
  auto* processor = DropZoneFromGroupProcessor::instance(kvstore_);
  RETURN_FUTURE(processor);
}

folly::Future<cpp2::ExecResp> MetaServiceHandler::future_addListener(
    const cpp2::AddListenerReq& req) {
  auto* processor = AddListenerProcessor::instance(kvstore_);
  RETURN_FUTURE(processor);
}

folly::Future<cpp2::ExecResp> MetaServiceHandler::future_removeListener(
    const cpp2::RemoveListenerReq& req) {
  auto* processor = RemoveListenerProcessor::instance(kvstore_);
  RETURN_FUTURE(processor);
}

folly::Future<cpp2::ListListenerResp> MetaServiceHandler::future_listListener(
    const cpp2::ListListenerReq& req) {
  auto* processor = ListListenerProcessor::instance(kvstore_);
  RETURN_FUTURE(processor);
}

folly::Future<cpp2::ExecResp> MetaServiceHandler::future_restoreMeta(
    const cpp2::RestoreMetaReq& req) {
  auto* processor = RestoreProcessor::instance(kvstore_);
  RETURN_FUTURE(processor);
}

folly::Future<cpp2::GetStatsResp> MetaServiceHandler::future_getStats(
    const cpp2::GetStatsReq& req) {
  auto* processor = GetStatsProcessor::instance(kvstore_);
  RETURN_FUTURE(processor);
}

folly::Future<cpp2::ListClusterInfoResp> MetaServiceHandler::future_listCluster(
    const cpp2::ListClusterInfoReq& req) {
  auto* processor = ListClusterInfoProcessor::instance(kvstore_, adminClient_.get());
  RETURN_FUTURE(processor);
}

folly::Future<cpp2::GetMetaDirInfoResp> MetaServiceHandler::future_getMetaDirInfo(
    const cpp2::GetMetaDirInfoReq& req) {
  auto* processor = GetMetaDirInfoProcessor::instance(kvstore_);

  RETURN_FUTURE(processor);
}

folly::Future<cpp2::CreateSessionResp> MetaServiceHandler::future_createSession(
    const cpp2::CreateSessionReq& req) {
  auto* processor = CreateSessionProcessor::instance(kvstore_);
  RETURN_FUTURE(processor);
}

folly::Future<cpp2::UpdateSessionsResp> MetaServiceHandler::future_updateSessions(
    const cpp2::UpdateSessionsReq& req) {
  auto* processor = UpdateSessionsProcessor::instance(kvstore_);
  RETURN_FUTURE(processor);
}

folly::Future<cpp2::ListSessionsResp> MetaServiceHandler::future_listSessions(
    const cpp2::ListSessionsReq& req) {
  auto* processor = ListSessionsProcessor::instance(kvstore_);
  RETURN_FUTURE(processor);
}

folly::Future<cpp2::GetSessionResp> MetaServiceHandler::future_getSession(
    const cpp2::GetSessionReq& req) {
  auto* processor = GetSessionProcessor::instance(kvstore_);
  RETURN_FUTURE(processor);
}

folly::Future<cpp2::ExecResp> MetaServiceHandler::future_removeSession(
    const cpp2::RemoveSessionReq& req) {
  auto* processor = RemoveSessionProcessor::instance(kvstore_);
  RETURN_FUTURE(processor);
}

folly::Future<cpp2::ExecResp> MetaServiceHandler::future_killQuery(const cpp2::KillQueryReq& req) {
  auto* processor = KillQueryProcessor::instance(kvstore_);
  RETURN_FUTURE(processor);
}

folly::Future<cpp2::VerifyClientVersionResp> MetaServiceHandler::future_verifyClientVersion(
    const cpp2::VerifyClientVersionReq& req) {
  auto* processor = VerifyClientVersionProcessor::instance(kvstore_);
  RETURN_FUTURE(processor);
}
}  // namespace meta
}  // namespace nebula
