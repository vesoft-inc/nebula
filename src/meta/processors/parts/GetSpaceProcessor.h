/* Copyright (c) 2019 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#ifndef META_GETSPACEPROCESSOR_H_
#define META_GETSPACEPROCESSOR_H_

#include <folly/Try.h>              // for Try::~Try<T>
#include <folly/futures/Promise.h>  // for PromiseException::Promise...

#include <utility>  // for move

#include "interface/gen-cpp2/meta_types.h"  // for GetSpaceResp, GetSpaceReq...
#include "meta/processors/BaseProcessor.h"  // for BaseProcessor

namespace nebula {
namespace kvstore {
class KVStore;

class KVStore;
}  // namespace kvstore

namespace meta {

class GetSpaceProcessor : public BaseProcessor<cpp2::GetSpaceResp> {
 public:
  static GetSpaceProcessor* instance(kvstore::KVStore* kvstore) {
    return new GetSpaceProcessor(kvstore);
  }

  void process(const cpp2::GetSpaceReq& req);

 private:
  explicit GetSpaceProcessor(kvstore::KVStore* kvstore)
      : BaseProcessor<cpp2::GetSpaceResp>(kvstore) {}
};

}  // namespace meta
}  // namespace nebula

#endif  // META_GETSPACEPROCESSOR_H_
