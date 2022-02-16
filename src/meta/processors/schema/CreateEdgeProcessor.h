/* Copyright (c) 2018 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#ifndef META_CREATEEDGEPROCESSOR_H_
#define META_CREATEEDGEPROCESSOR_H_

#include <folly/Try.h>              // for Try::~Try<T>
#include <folly/futures/Promise.h>  // for PromiseException::Promise...

#include <utility>  // for move

#include "interface/gen-cpp2/meta_types.h"  // for ExecResp, CreateEdgeReq (...
#include "meta/processors/BaseProcessor.h"  // for BaseProcessor

namespace nebula {
namespace kvstore {
class KVStore;

class KVStore;
}  // namespace kvstore

namespace meta {

class CreateEdgeProcessor : public BaseProcessor<cpp2::ExecResp> {
 public:
  static CreateEdgeProcessor* instance(kvstore::KVStore* kvstore) {
    return new CreateEdgeProcessor(kvstore);
  }

  void process(const cpp2::CreateEdgeReq& req);

 private:
  explicit CreateEdgeProcessor(kvstore::KVStore* kvstore)
      : BaseProcessor<cpp2::ExecResp>(kvstore) {}
};

}  // namespace meta
}  // namespace nebula

#endif  // META_CREATEEDGEPROCESSOR_H_
