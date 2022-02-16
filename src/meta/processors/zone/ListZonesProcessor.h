/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#ifndef META_LISTZONESPROCESSOR_H
#define META_LISTZONESPROCESSOR_H

#include <folly/Try.h>              // for Try::~Try<T>
#include <folly/futures/Promise.h>  // for PromiseException::Promise...

#include <utility>  // for move

#include "interface/gen-cpp2/meta_types.h"  // for ListZonesResp, ListZonesReq
#include "meta/processors/BaseProcessor.h"  // for BaseProcessor

namespace nebula {
namespace kvstore {
class KVStore;

class KVStore;
}  // namespace kvstore

namespace meta {

/**
 * @brief Get all zones info
 */
class ListZonesProcessor : public BaseProcessor<cpp2::ListZonesResp> {
 public:
  static ListZonesProcessor* instance(kvstore::KVStore* kvstore) {
    return new ListZonesProcessor(kvstore);
  }

  void process(const cpp2::ListZonesReq& req);

 private:
  explicit ListZonesProcessor(kvstore::KVStore* kvstore)
      : BaseProcessor<cpp2::ListZonesResp>(kvstore) {}
};

}  // namespace meta
}  // namespace nebula
#endif  // META_LISTZONESPROCESSOR_H
