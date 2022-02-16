/* Copyright (c) 2018 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#ifndef META_GETPARTSALLOCPROCESSOR_H_
#define META_GETPARTSALLOCPROCESSOR_H_

#include <folly/Try.h>              // for Try::~Try<T>
#include <folly/futures/Promise.h>  // for PromiseException::Promise...

#include <unordered_map>  // for unordered_map
#include <utility>        // for move

#include "common/thrift/ThriftTypes.h"      // for GraphSpaceID, PartitionID
#include "interface/gen-cpp2/meta_types.h"  // for GetPartsAllocResp, GetPar...
#include "meta/processors/BaseProcessor.h"  // for BaseProcessor

namespace nebula {
namespace kvstore {
class KVStore;

class KVStore;
}  // namespace kvstore

namespace meta {

class GetPartsAllocProcessor : public BaseProcessor<cpp2::GetPartsAllocResp> {
 public:
  static GetPartsAllocProcessor* instance(kvstore::KVStore* kvstore) {
    return new GetPartsAllocProcessor(kvstore);
  }

  void process(const cpp2::GetPartsAllocReq& req);

 private:
  explicit GetPartsAllocProcessor(kvstore::KVStore* kvstore)
      : BaseProcessor<cpp2::GetPartsAllocResp>(kvstore) {}

  std::unordered_map<PartitionID, TermID> getTerm(GraphSpaceID space);
};

}  // namespace meta
}  // namespace nebula

#endif  // META_GETPARTSALLOCPROCESSOR_H_
