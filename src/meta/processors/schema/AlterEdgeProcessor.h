/* Copyright (c) 2018 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#ifndef META_ALTEREDGEPROCESSOR_H_
#define META_ALTEREDGEPROCESSOR_H_

#include "meta/processors/BaseProcessor.h"

/**
 * @brief Alter edge properties. It will get the edge schema with latest version, then check if
 *        there are indexes on it.
 *
 */
namespace nebula {
namespace meta {

class AlterEdgeProcessor : public BaseProcessor<cpp2::ExecResp> {
 public:
  static AlterEdgeProcessor* instance(kvstore::KVStore* kvstore) {
    return new AlterEdgeProcessor(kvstore);
  }

  void process(const cpp2::AlterEdgeReq& req);

 private:
  explicit AlterEdgeProcessor(kvstore::KVStore* kvstore) : BaseProcessor<cpp2::ExecResp>(kvstore) {}
};

}  // namespace meta
}  // namespace nebula
#endif  // META_ALTEREDGEPROCESSOR_H_
