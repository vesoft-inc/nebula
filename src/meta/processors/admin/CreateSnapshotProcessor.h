/* Copyright (c) 2019 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#ifndef META_CREATESNAPSHOTPROCESSOR_H_
#define META_CREATESNAPSHOTPROCESSOR_H_

#include "meta/processors/BaseProcessor.h"
#include "meta/processors/admin/AdminClient.h"

namespace nebula {
namespace meta {

/**
 * @brief Create snapshot for all spaces, will deprecated when backup ready
 *
 */
class CreateSnapshotProcessor : public BaseProcessor<cpp2::ExecResp> {
 public:
  static CreateSnapshotProcessor* instance(kvstore::KVStore* kvstore, AdminClient* client) {
    return new CreateSnapshotProcessor(kvstore, client);
  }
  void process(const cpp2::CreateSnapshotReq& req);

 private:
  CreateSnapshotProcessor(kvstore::KVStore* kvstore, AdminClient* client)
      : BaseProcessor<cpp2::ExecResp>(kvstore), client_(client) {}
  /**
   * @brief Cancel write blocking when create snapshot failed
   *
   * @return nebula::cpp2::ErrorCode
   */
  nebula::cpp2::ErrorCode cancelWriteBlocking();

 private:
  AdminClient* client_;
};

}  // namespace meta
}  // namespace nebula

#endif  // META_CREATESNAPSHOTPROCESSOR_H_
