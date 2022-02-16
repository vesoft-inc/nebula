/* Copyright (c) 2019 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#ifndef META_LISTSNAPSHOTSPROCESSOR_H_
#define META_LISTSNAPSHOTSPROCESSOR_H_

#include <folly/Try.h>              // for Try::~Try<T>
#include <folly/futures/Promise.h>  // for PromiseException::Promise...

#include <utility>  // for move

#include "interface/gen-cpp2/meta_types.h"  // for ListSnapshotsResp, ListSn...
#include "meta/processors/BaseProcessor.h"  // for BaseProcessor

namespace nebula {
namespace kvstore {
class KVStore;

class KVStore;
}  // namespace kvstore

namespace meta {

/**
 * @brief List all snapshot info, CreateBackupProcessor
 *        and CreateCheckpointProcessor will both
 *        add snapshot info to metad's kv store.
 *
 */
class ListSnapshotsProcessor : public BaseProcessor<cpp2::ListSnapshotsResp> {
 public:
  static ListSnapshotsProcessor* instance(kvstore::KVStore* kvstore) {
    return new ListSnapshotsProcessor(kvstore);
  }
  void process(const cpp2::ListSnapshotsReq& req);

 private:
  explicit ListSnapshotsProcessor(kvstore::KVStore* kvstore)
      : BaseProcessor<cpp2::ListSnapshotsResp>(kvstore) {}
};
}  // namespace meta
}  // namespace nebula

#endif  // META_LISTSNAPSHOTSPROCESSOR_H_
