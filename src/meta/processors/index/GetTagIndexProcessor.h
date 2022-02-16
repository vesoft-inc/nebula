/* Copyright (c) 2019 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#ifndef META_GETTAGINDEXPROCESSOR_H
#define META_GETTAGINDEXPROCESSOR_H

#include <folly/Try.h>              // for Try::~Try<T>
#include <folly/futures/Promise.h>  // for PromiseException::Promise...

#include <utility>  // for move

#include "interface/gen-cpp2/meta_types.h"  // for GetTagIndexResp, GetTagIn...
#include "meta/processors/BaseProcessor.h"  // for BaseProcessor

namespace nebula {
namespace kvstore {
class KVStore;

class KVStore;
}  // namespace kvstore

namespace meta {

class GetTagIndexProcessor : public BaseProcessor<cpp2::GetTagIndexResp> {
 public:
  static GetTagIndexProcessor* instance(kvstore::KVStore* kvstore) {
    return new GetTagIndexProcessor(kvstore);
  }

  void process(const cpp2::GetTagIndexReq& req);

 private:
  explicit GetTagIndexProcessor(kvstore::KVStore* kvstore)
      : BaseProcessor<cpp2::GetTagIndexResp>(kvstore) {}
};

}  // namespace meta
}  // namespace nebula

#endif  // META_GETTAGINDEXPROCESSOR_H
