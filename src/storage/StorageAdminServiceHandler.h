/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#ifndef STORAGE_STORAGEADMINSERVICEHANDLER_H_
#define STORAGE_STORAGEADMINSERVICEHANDLER_H_

#include "common/base/Base.h"
#include "interface/gen-cpp2/StorageAdminService.h"

namespace nebula {
namespace storage {

class StorageEnv;

class StorageAdminServiceHandler final : public cpp2::StorageAdminServiceSvIf {
 public:
  explicit StorageAdminServiceHandler(StorageEnv* env) : env_(env) {}

  folly::Future<cpp2::AdminExecResp> future_transLeader(const cpp2::TransLeaderReq& req) override;

  folly::Future<cpp2::AdminExecResp> future_addPart(const cpp2::AddPartReq& req) override;

  folly::Future<cpp2::AdminExecResp> future_addLearner(const cpp2::AddLearnerReq& req) override;

  folly::Future<cpp2::AdminExecResp> future_waitingForCatchUpData(
      const cpp2::CatchUpDataReq& req) override;

  folly::Future<cpp2::AdminExecResp> future_removePart(const cpp2::RemovePartReq& req) override;

  folly::Future<cpp2::AdminExecResp> future_memberChange(const cpp2::MemberChangeReq& req) override;

  folly::Future<cpp2::AdminExecResp> future_checkPeers(const cpp2::CheckPeersReq& req) override;

  folly::Future<cpp2::GetLeaderPartsResp> future_getLeaderParts(
      const cpp2::GetLeaderReq& req) override;

  folly::Future<cpp2::CreateCPResp> future_createCheckpoint(
      const cpp2::CreateCPRequest& req) override;

  folly::Future<cpp2::DropCPResp> future_dropCheckpoint(const cpp2::DropCPRequest& req) override;

  folly::Future<cpp2::BlockingSignResp> future_blockingWrites(
      const cpp2::BlockingSignRequest& req) override;

  folly::Future<cpp2::AddTaskResp> future_addAdminTask(const cpp2::AddTaskRequest& req) override;

  folly::Future<cpp2::StopTaskResp> future_stopAdminTask(const cpp2::StopTaskRequest& req) override;

  folly::Future<cpp2::ClearSpaceResp> future_clearSpace(const cpp2::ClearSpaceReq& req) override;

 private:
  StorageEnv* env_{nullptr};
};

}  // namespace storage
}  // namespace nebula
#endif  // STORAGE_STORAGEADMINSERVICEHANDLER_H_
