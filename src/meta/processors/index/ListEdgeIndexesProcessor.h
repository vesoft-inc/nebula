/* Copyright (c) 2019 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#ifndef META_LISTEDGEINDEXESPROCESSOR_H
#define META_LISTEDGEINDEXESPROCESSOR_H

#include <folly/Try.h>              // for Try::~Try<T>
#include <folly/futures/Promise.h>  // for PromiseException::Promise...

#include <utility>  // for move

#include "interface/gen-cpp2/meta_types.h"  // for ListEdgeIndexesResp, List...
#include "meta/processors/BaseProcessor.h"  // for BaseProcessor

namespace nebula {
namespace kvstore {
class KVStore;

class KVStore;
}  // namespace kvstore

namespace meta {

class ListEdgeIndexesProcessor : public BaseProcessor<cpp2::ListEdgeIndexesResp> {
 public:
  static ListEdgeIndexesProcessor* instance(kvstore::KVStore* kvstore) {
    return new ListEdgeIndexesProcessor(kvstore);
  }

  void process(const cpp2::ListEdgeIndexesReq& req);

 private:
  explicit ListEdgeIndexesProcessor(kvstore::KVStore* kvstore)
      : BaseProcessor<cpp2::ListEdgeIndexesResp>(kvstore) {}
};

}  // namespace meta
}  // namespace nebula

#endif  // META_LISTEDGEINDEXESPROCESSOR_H
