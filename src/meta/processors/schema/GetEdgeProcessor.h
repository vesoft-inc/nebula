/* Copyright (c) 2018 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#ifndef META_GETEDGEPROCESSOR_H_
#define META_GETEDGEPROCESSOR_H_

#include "meta/processors/BaseProcessor.h"

namespace nebula {
namespace meta {

/**
 * @brief Get the edge schema with given version or latest version.
 *
 */
class GetEdgeProcessor : public BaseProcessor<cpp2::GetEdgeResp> {
 public:
  static GetEdgeProcessor* instance(kvstore::KVStore* kvstore) {
    return new GetEdgeProcessor(kvstore);
  }

  void process(const cpp2::GetEdgeReq& req);

 private:
  explicit GetEdgeProcessor(kvstore::KVStore* kvstore)
      : BaseProcessor<cpp2::GetEdgeResp>(kvstore) {}
};

}  // namespace meta
}  // namespace nebula

#endif  // META_GETEDGEPROCESSOR_H_
