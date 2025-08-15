/* Copyright (c) 2019 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#ifndef META_LISTTAGANNINDEXESPROCESSOR_H
#define META_LISTTAGANNINDEXESPROCESSOR_H

#include "meta/processors/BaseProcessor.h"

namespace nebula {
namespace meta {

/**
 * @brief Get all tag index items by scanning index prefix and then filter out the
 *        indexes with tag type.
 *
 */
class ListTagAnnIndexesProcessor : public BaseProcessor<cpp2::ListTagAnnIndexesResp> {
 public:
  static ListTagAnnIndexesProcessor* instance(kvstore::KVStore* kvstore) {
    return new ListTagAnnIndexesProcessor(kvstore);
  }

  void process(const cpp2::ListTagIndexesReq& req);

 private:
  explicit ListTagAnnIndexesProcessor(kvstore::KVStore* kvstore)
      : BaseProcessor<cpp2::ListTagAnnIndexesResp>(kvstore) {}
};

}  // namespace meta
}  // namespace nebula

#endif  // META_LISTTAGANNINDEXESPROCESSOR_H
