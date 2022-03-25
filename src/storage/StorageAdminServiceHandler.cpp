/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#include "storage/StorageAdminServiceHandler.h"

#include "storage/admin/AdminProcessor.h"
#include "storage/admin/AdminTaskProcessor.h"
#include "storage/admin/ClearSpaceProcessor.h"
#include "storage/admin/CreateCheckpointProcessor.h"
#include "storage/admin/DropCheckpointProcessor.h"
#include "storage/admin/GetLeaderProcessor.h"
#include "storage/admin/SendBlockSignProcessor.h"
#include "storage/admin/StopAdminTaskProcessor.h"

#define RETURN_FUTURE(processor)   \
  auto f = processor->getFuture(); \
  processor->process(req);         \
  return f;

namespace nebula {
namespace storage {

folly::Future<cpp2::AdminExecResp> StorageAdminServiceHandler::future_transLeader(
    const cpp2::TransLeaderReq& req) {
  auto* processor = TransLeaderProcessor::instance(env_);
  RETURN_FUTURE(processor);
}

folly::Future<cpp2::AdminExecResp> StorageAdminServiceHandler::future_addPart(
    const cpp2::AddPartReq& req) {
  auto* processor = AddPartProcessor::instance(env_);
  RETURN_FUTURE(processor);
}

folly::Future<cpp2::AdminExecResp> StorageAdminServiceHandler::future_addLearner(
    const cpp2::AddLearnerReq& req) {
  auto* processor = AddLearnerProcessor::instance(env_);
  RETURN_FUTURE(processor);
}

folly::Future<cpp2::AdminExecResp> StorageAdminServiceHandler::future_waitingForCatchUpData(
    const cpp2::CatchUpDataReq& req) {
  auto* processor = WaitingForCatchUpDataProcessor::instance(env_);
  RETURN_FUTURE(processor);
}

folly::Future<cpp2::AdminExecResp> StorageAdminServiceHandler::future_removePart(
    const cpp2::RemovePartReq& req) {
  auto* processor = RemovePartProcessor::instance(env_);
  RETURN_FUTURE(processor);
}

folly::Future<cpp2::AdminExecResp> StorageAdminServiceHandler::future_memberChange(
    const cpp2::MemberChangeReq& req) {
  auto* processor = MemberChangeProcessor::instance(env_);
  RETURN_FUTURE(processor);
}

folly::Future<cpp2::AdminExecResp> StorageAdminServiceHandler::future_checkPeers(
    const cpp2::CheckPeersReq& req) {
  auto* processor = CheckPeersProcessor::instance(env_);
  RETURN_FUTURE(processor);
}

folly::Future<cpp2::GetLeaderPartsResp> StorageAdminServiceHandler::future_getLeaderParts(
    const cpp2::GetLeaderReq& req) {
  auto* processor = GetLeaderProcessor::instance(env_);
  RETURN_FUTURE(processor);
}

folly::Future<cpp2::CreateCPResp> StorageAdminServiceHandler::future_createCheckpoint(
    const cpp2::CreateCPRequest& req) {
  auto* processor = CreateCheckpointProcessor::instance(env_);
  RETURN_FUTURE(processor);
}

folly::Future<cpp2::DropCPResp> StorageAdminServiceHandler::future_dropCheckpoint(
    const cpp2::DropCPRequest& req) {
  auto* processor = DropCheckpointProcessor::instance(env_);
  RETURN_FUTURE(processor);
}

folly::Future<cpp2::BlockingSignResp> StorageAdminServiceHandler::future_blockingWrites(
    const cpp2::BlockingSignRequest& req) {
  auto* processor = SendBlockSignProcessor::instance(env_);
  RETURN_FUTURE(processor);
}

folly::Future<cpp2::AddTaskResp> StorageAdminServiceHandler::future_addAdminTask(
    const cpp2::AddTaskRequest& req) {
  auto* processor = AdminTaskProcessor::instance(env_);
  RETURN_FUTURE(processor);
}

folly::Future<cpp2::StopTaskResp> StorageAdminServiceHandler::future_stopAdminTask(
    const cpp2::StopTaskRequest& req) {
  auto* processor = StopAdminTaskProcessor::instance(env_);
  RETURN_FUTURE(processor);
}

folly::Future<cpp2::ClearSpaceResp> StorageAdminServiceHandler::future_clearSpace(
    const cpp2::ClearSpaceReq& req) {
  auto* processor = ClearSpaceProcessor::instance(env_);
  RETURN_FUTURE(processor);
}

}  // namespace storage
}  // namespace nebula
