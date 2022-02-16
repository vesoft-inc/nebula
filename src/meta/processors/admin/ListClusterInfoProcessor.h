/* Copyright (c) 2021 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#ifndef META_LISTCLUSTERINFOSPROCESSOR_H_
#define META_LISTCLUSTERINFOSPROCESSOR_H_

#include <folly/Try.h>              // for Try::~Try<T>
#include <folly/futures/Promise.h>  // for PromiseException::Promise...

#include <utility>  // for move

#include "interface/gen-cpp2/meta_types.h"  // for ListClusterInfoResp, List...
#include "meta/processors/BaseProcessor.h"  // for BaseProcessor
#include "meta/processors/admin/AdminClient.h"

namespace nebula {
namespace kvstore {
class KVStore;

class KVStore;
}  // namespace kvstore

namespace meta {

/**
 * @brief Get cluster topology, grouping all the cluster service(metad/storaged/graphd/agent)
 *        by hostname(or ip).
 *        Now it is only used in BR tools.
 */
class ListClusterInfoProcessor : public BaseProcessor<cpp2::ListClusterInfoResp> {
 public:
  static ListClusterInfoProcessor* instance(kvstore::KVStore* kvstore) {
    return new ListClusterInfoProcessor(kvstore);
  }
  void process(const cpp2::ListClusterInfoReq& req);

 private:
  explicit ListClusterInfoProcessor(kvstore::KVStore* kvstore)
      : BaseProcessor<cpp2::ListClusterInfoResp>(kvstore) {}
};
}  // namespace meta
}  // namespace nebula

#endif  // META_LISTCLUSTERINFOSPROCESSOR_H_
