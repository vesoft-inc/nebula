/* Copyright (c) 2018 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#ifndef META_GETEDGEPROCESSOR_H_
#define META_GETEDGEPROCESSOR_H_

#include <folly/Try.h>              // for Try::~Try<T>
#include <folly/futures/Promise.h>  // for PromiseException::Promise...

#include <utility>  // for move

#include "interface/gen-cpp2/meta_types.h"  // for GetEdgeResp, GetEdgeReq (...
#include "meta/processors/BaseProcessor.h"  // for BaseProcessor

namespace nebula {
namespace kvstore {
class KVStore;

class KVStore;
}  // namespace kvstore

namespace meta {

class GetEdgeProcessor : public BaseProcessor<cpp2::GetEdgeResp> {
 public:
  static GetEdgeProcessor* instance(kvstore::KVStore* kvstore) {
    return new GetEdgeProcessor(kvstore);
  }

  void process(const cpp2::GetEdgeReq& req);

 private:
  explicit GetEdgeProcessor(kvstore::KVStore* kvstore)
      : BaseProcessor<cpp2::GetEdgeResp>(kvstore) {}
};

}  // namespace meta
}  // namespace nebula

#endif  // META_GETEDGEPROCESSOR_H_
