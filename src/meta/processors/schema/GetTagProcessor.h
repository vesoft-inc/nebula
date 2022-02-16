/* Copyright (c) 2018 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#ifndef META_GETTAGPROCESSOR_H_
#define META_GETTAGPROCESSOR_H_

#include <folly/Try.h>              // for Try::~Try<T>
#include <folly/futures/Promise.h>  // for PromiseException::Promise...

#include <utility>  // for move

#include "interface/gen-cpp2/meta_types.h"  // for GetTagResp, GetTagReq (pt...
#include "meta/processors/BaseProcessor.h"  // for BaseProcessor

namespace nebula {
namespace kvstore {
class KVStore;

class KVStore;
}  // namespace kvstore

namespace meta {

class GetTagProcessor : public BaseProcessor<cpp2::GetTagResp> {
 public:
  static GetTagProcessor* instance(kvstore::KVStore* kvstore) {
    return new GetTagProcessor(kvstore);
  }

  void process(const cpp2::GetTagReq& req);

 private:
  explicit GetTagProcessor(kvstore::KVStore* kvstore) : BaseProcessor<cpp2::GetTagResp>(kvstore) {}
};

}  // namespace meta
}  // namespace nebula
#endif  // META_GETTAGPROCESSOR_H_
