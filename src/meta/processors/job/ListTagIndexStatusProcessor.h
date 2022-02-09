/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#ifndef META_LISTTAGINDEXSTATUSPROCESSOR_H_
#define META_LISTTAGINDEXSTATUSPROCESSOR_H_

#include "meta/processors/BaseProcessor.h"
#include "meta/processors/job/JobDescription.h"
#include "meta/processors/job/JobUtils.h"

namespace nebula {
namespace meta {

/**
 * @brief Show status all rebuild-tag-index jobs
 */
class ListTagIndexStatusProcessor : public BaseProcessor<cpp2::ListIndexStatusResp> {
 public:
  static ListTagIndexStatusProcessor* instance(kvstore::KVStore* kvstore) {
    return new ListTagIndexStatusProcessor(kvstore);
  }

  void process(const cpp2::ListIndexStatusReq& req);

 private:
  explicit ListTagIndexStatusProcessor(kvstore::KVStore* kvstore)
      : BaseProcessor<cpp2::ListIndexStatusResp>(kvstore) {}
};

}  // namespace meta
}  // namespace nebula

#endif  // META_LISTTAGINDEXSTATUSPROCESSOR_H_
