/* Copyright (c) 2019 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#ifndef META_GETEDGEINDEXPROCESSOR_H
#define META_GETEDGEINDEXPROCESSOR_H

#include <folly/Try.h>              // for Try::~Try<T>
#include <folly/futures/Promise.h>  // for PromiseException::Promise...

#include <utility>  // for move

#include "interface/gen-cpp2/meta_types.h"  // for GetEdgeIndexResp, GetEdge...
#include "meta/processors/BaseProcessor.h"  // for BaseProcessor

namespace nebula {
namespace kvstore {
class KVStore;

class KVStore;
}  // namespace kvstore

namespace meta {

class GetEdgeIndexProcessor : public BaseProcessor<cpp2::GetEdgeIndexResp> {
 public:
  static GetEdgeIndexProcessor* instance(kvstore::KVStore* kvstore) {
    return new GetEdgeIndexProcessor(kvstore);
  }

  void process(const cpp2::GetEdgeIndexReq& req);

 private:
  explicit GetEdgeIndexProcessor(kvstore::KVStore* kvstore)
      : BaseProcessor<cpp2::GetEdgeIndexResp>(kvstore) {}
};

}  // namespace meta
}  // namespace nebula

#endif  // META_GETEDGEINDEXPROCESSOR_H
