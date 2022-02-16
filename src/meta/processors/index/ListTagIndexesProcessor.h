/* Copyright (c) 2019 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#ifndef META_LISTTAGINDEXESPROCESSOR_H
#define META_LISTTAGINDEXESPROCESSOR_H

#include <folly/Try.h>              // for Try::~Try<T>
#include <folly/futures/Promise.h>  // for PromiseException::Promise...

#include <utility>  // for move

#include "interface/gen-cpp2/meta_types.h"  // for ListTagIndexesResp, ListT...
#include "meta/processors/BaseProcessor.h"  // for BaseProcessor

namespace nebula {
namespace kvstore {
class KVStore;

class KVStore;
}  // namespace kvstore

namespace meta {

class ListTagIndexesProcessor : public BaseProcessor<cpp2::ListTagIndexesResp> {
 public:
  static ListTagIndexesProcessor* instance(kvstore::KVStore* kvstore) {
    return new ListTagIndexesProcessor(kvstore);
  }

  void process(const cpp2::ListTagIndexesReq& req);

 private:
  explicit ListTagIndexesProcessor(kvstore::KVStore* kvstore)
      : BaseProcessor<cpp2::ListTagIndexesResp>(kvstore) {}
};

}  // namespace meta
}  // namespace nebula

#endif  // META_LISTTAGINDEXESPROCESSOR_H
