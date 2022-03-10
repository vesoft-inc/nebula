/* Copyright (c) 2018 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#ifndef META_ADMINJOBPROCESSOR_H_
#define META_ADMINJOBPROCESSOR_H_

#include "common/stats/StatsManager.h"
#include "meta/processors/BaseProcessor.h"
#include "meta/processors/admin/AdminClient.h"
#include "meta/processors/job/JobManager.h"

namespace nebula {
namespace meta {

/**
 * @brief Make admin job operation, including ADD SHOW_All SHOW STOP RECOVER
 */
class AdminJobProcessor : public BaseProcessor<cpp2::AdminJobResp> {
 public:
  static AdminJobProcessor* instance(kvstore::KVStore* kvstore, AdminClient* adminClient) {
    return new AdminJobProcessor(kvstore, adminClient);
  }

  void process(const cpp2::AdminJobReq& req);

 protected:
  AdminJobProcessor(kvstore::KVStore* kvstore, AdminClient* adminClient)
      : BaseProcessor<cpp2::AdminJobResp>(kvstore), adminClient_(adminClient) {}

 private:
  /**
   * @brief Check whether the parameters are legal, then construct the job and join the queue.
   *
   * @param req
   * @param result
   * @return nebula::cpp2::ErrorCode
   */
  nebula::cpp2::ErrorCode addJobProcess(const cpp2::AdminJobReq& req, cpp2::AdminJobResult& result);

 protected:
  AdminClient* adminClient_{nullptr};
  JobManager* jobMgr_{nullptr};
};

}  // namespace meta
}  // namespace nebula

#endif  // META_ADMINJOBPROCESSOR_H_
