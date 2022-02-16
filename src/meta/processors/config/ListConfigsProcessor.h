/* Copyright (c) 2019 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#ifndef META_LISTCONFIGPROCESSOR_H
#define META_LISTCONFIGPROCESSOR_H

#include <folly/Try.h>              // for Try::~Try<T>
#include <folly/futures/Promise.h>  // for PromiseException::Promise...

#include <utility>  // for move

#include "interface/gen-cpp2/meta_types.h"  // for ListConfigsResp, ListConf...
#include "meta/processors/BaseProcessor.h"  // for BaseProcessor

namespace nebula {
namespace kvstore {
class KVStore;

class KVStore;
}  // namespace kvstore

namespace meta {

/**
 * @brief List all registered configs in given module
 *
 */
class ListConfigsProcessor : public BaseProcessor<cpp2::ListConfigsResp> {
 public:
  static ListConfigsProcessor* instance(kvstore::KVStore* kvstore) {
    return new ListConfigsProcessor(kvstore);
  }

  void process(const cpp2::ListConfigsReq& req);

 private:
  explicit ListConfigsProcessor(kvstore::KVStore* kvstore)
      : BaseProcessor<cpp2::ListConfigsResp>(kvstore) {}
};

}  // namespace meta
}  // namespace nebula

#endif  // META_LISTCONFIGPROCESSOR_H
