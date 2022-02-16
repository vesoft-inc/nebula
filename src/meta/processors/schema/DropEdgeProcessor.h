/* Copyright (c) 2018 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#ifndef META_DROPEDGEPROCESSOR_H_
#define META_DROPEDGEPROCESSOR_H_

#include <folly/Try.h>              // for Try::~Try<T>
#include <folly/futures/Promise.h>  // for PromiseException::Promi...

#include <string>   // for string
#include <utility>  // for move
#include <vector>   // for vector

#include "common/base/ErrorOr.h"              // for ErrorOr
#include "common/thrift/ThriftTypes.h"        // for EdgeType, GraphSpaceID
#include "interface/gen-cpp2/common_types.h"  // for ErrorCode
#include "interface/gen-cpp2/meta_types.h"    // for ExecResp, DropEdgeReq (...
#include "meta/processors/BaseProcessor.h"    // for BaseProcessor

namespace nebula {
namespace kvstore {
class KVStore;

class KVStore;
}  // namespace kvstore

namespace meta {

class DropEdgeProcessor : public BaseProcessor<cpp2::ExecResp> {
 public:
  static DropEdgeProcessor* instance(kvstore::KVStore* kvstore) {
    return new DropEdgeProcessor(kvstore);
  }

  void process(const cpp2::DropEdgeReq& req);

 private:
  explicit DropEdgeProcessor(kvstore::KVStore* kvstore) : BaseProcessor<cpp2::ExecResp>(kvstore) {}

  ErrorOr<nebula::cpp2::ErrorCode, std::vector<std::string>> getEdgeKeys(GraphSpaceID id,
                                                                         EdgeType edgeType);
};

}  // namespace meta
}  // namespace nebula

#endif  // META_DROPEDGEPROCESSOR_H_
