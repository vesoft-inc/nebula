/* Copyright (c) 2018 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#ifndef META_LISTSPACESPROCESSOR_H_
#define META_LISTSPACESPROCESSOR_H_

#include <folly/Try.h>              // for Try::~Try<T>
#include <folly/futures/Promise.h>  // for PromiseException::Promise...

#include <utility>  // for move

#include "interface/gen-cpp2/meta_types.h"  // for ListSpacesResp, ListSpace...
#include "meta/processors/BaseProcessor.h"  // for BaseProcessor

namespace nebula {
namespace kvstore {
class KVStore;

class KVStore;
}  // namespace kvstore

namespace meta {

class ListSpacesProcessor : public BaseProcessor<cpp2::ListSpacesResp> {
 public:
  static ListSpacesProcessor* instance(kvstore::KVStore* kvstore) {
    return new ListSpacesProcessor(kvstore);
  }

  void process(const cpp2::ListSpacesReq& req);

 private:
  explicit ListSpacesProcessor(kvstore::KVStore* kvstore)
      : BaseProcessor<cpp2::ListSpacesResp>(kvstore) {}
};

}  // namespace meta
}  // namespace nebula

#endif  // META_LISTSPACESPROCESSOR_H_
