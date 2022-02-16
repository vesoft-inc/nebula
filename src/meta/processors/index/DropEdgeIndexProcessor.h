/* Copyright (c) 2019 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#ifndef META_DROPEDGEINDEXPROCESSOR_H
#define META_DROPEDGEINDEXPROCESSOR_H

#include <folly/Try.h>              // for Try::~Try<T>
#include <folly/futures/Promise.h>  // for PromiseException::Promise...

#include <utility>  // for move

#include "interface/gen-cpp2/meta_types.h"  // for ExecResp, DropEdgeIndexRe...
#include "meta/processors/BaseProcessor.h"  // for BaseProcessor

namespace nebula {
namespace kvstore {
class KVStore;

class KVStore;
}  // namespace kvstore

namespace meta {

class DropEdgeIndexProcessor : public BaseProcessor<cpp2::ExecResp> {
 public:
  static DropEdgeIndexProcessor* instance(kvstore::KVStore* kvstore) {
    return new DropEdgeIndexProcessor(kvstore);
  }

  void process(const cpp2::DropEdgeIndexReq& req);

 private:
  explicit DropEdgeIndexProcessor(kvstore::KVStore* kvstore)
      : BaseProcessor<cpp2::ExecResp>(kvstore) {}
};

}  // namespace meta
}  // namespace nebula

#endif  // META_DROPEDGEINDEXPROCESSOR_H
