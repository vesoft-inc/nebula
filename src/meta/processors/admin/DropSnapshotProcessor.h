/* Copyright (c) 2019 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#ifndef META_DROPSNAPSHOTPROCESSOR_H_
#define META_DROPSNAPSHOTPROCESSOR_H_

#include "meta/processors/BaseProcessor.h"
#include "meta/processors/admin/AdminClient.h"

namespace nebula {
namespace meta {

/**
 * @brief Drop snapshot for all spaces. It could drop snapshots
 *        created by CreateBackupProcessor or CreateCheckpointProcessor.
 *        It will drop the snapshots as possible as it can, but there maybe some snapshots left
 *        in some hosts since:
 *        1. some storaged remove snapshot failed due to some reason.
 *        2. the metad may change leader if it have multi-replications.
 *
 */
class DropSnapshotProcessor : public BaseProcessor<cpp2::ExecResp> {
 public:
  static DropSnapshotProcessor* instance(kvstore::KVStore* kvstore, AdminClient* client) {
    return new DropSnapshotProcessor(kvstore, client);
  }

  void process(const cpp2::DropSnapshotReq& req);

 private:
  DropSnapshotProcessor(kvstore::KVStore* kvstore, AdminClient* client)
      : BaseProcessor<cpp2::ExecResp>(kvstore), client_(client) {}

 private:
  AdminClient* client_;
};

}  // namespace meta
}  // namespace nebula

#endif  // META_DROPSNAPSHOTPROCESSOR_H_
