/* Copyright (c) 2019 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#ifndef META_LISTEDGEANNINDEXESPROCESSOR_H
#define META_LISTEDGEANNINDEXESPROCESSOR_H

#include "meta/processors/BaseProcessor.h"

namespace nebula {
namespace meta {

/**
 * @brief Get all Edge index items by scanning index prefix and then filter out the
 *        indexes with Edge type.
 *
 */
class ListEdgeAnnIndexesProcessor : public BaseProcessor<cpp2::ListEdgeAnnIndexesResp> {
 public:
  static ListEdgeAnnIndexesProcessor* instance(kvstore::KVStore* kvstore) {
    return new ListEdgeAnnIndexesProcessor(kvstore);
  }

  void process(const cpp2::ListEdgeIndexesReq& req);

 private:
  explicit ListEdgeAnnIndexesProcessor(kvstore::KVStore* kvstore)
      : BaseProcessor<cpp2::ListEdgeAnnIndexesResp>(kvstore) {}
};

}  // namespace meta
}  // namespace nebula

#endif  // META_LISTEDGEANNINDEXESPROCESSOR_H
