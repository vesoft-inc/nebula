/* Copyright (c) 2019 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#ifndef META_GETSPACEPROCESSOR_H_
#define META_GETSPACEPROCESSOR_H_

#include "meta/processors/BaseProcessor.h"

namespace nebula {
namespace meta {

/**
 * @brief Get space description(SpaceDesc) with space name
 *
 */
class GetSpaceProcessor : public BaseProcessor<cpp2::GetSpaceResp> {
 public:
  static GetSpaceProcessor* instance(kvstore::KVStore* kvstore) {
    return new GetSpaceProcessor(kvstore);
  }

  void process(const cpp2::GetSpaceReq& req);

 private:
  explicit GetSpaceProcessor(kvstore::KVStore* kvstore)
      : BaseProcessor<cpp2::GetSpaceResp>(kvstore) {}
};

}  // namespace meta
}  // namespace nebula

#endif  // META_GETSPACEPROCESSOR_H_
