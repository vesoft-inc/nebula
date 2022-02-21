/* Copyright (c) 2019 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#ifndef META_CREATEEDGEINDEXPROCESSOR_H
#define META_CREATEEDGEINDEXPROCESSOR_H

#include "meta/processors/BaseProcessor.h"

namespace nebula {
namespace meta {

/**
 * @brief Create edge index on given space with:
 *        1. given index name
 *        2. given edge name
 *        3. fields in the given edge
 *
 *        It will first check if all the parameters valid, then create index item
 *        to save these meta without building the index on existing data actually.
 *        But when new data inserted, it will generate index data according to the index
 *        created before.
 *        Or user could call `rebuild index` to generate indexes for all existing data.
 *
 */
class CreateEdgeIndexProcessor : public BaseProcessor<cpp2::ExecResp> {
 public:
  static CreateEdgeIndexProcessor* instance(kvstore::KVStore* kvstore) {
    return new CreateEdgeIndexProcessor(kvstore);
  }

  void process(const cpp2::CreateEdgeIndexReq& req);

 private:
  explicit CreateEdgeIndexProcessor(kvstore::KVStore* kvstore)
      : BaseProcessor<cpp2::ExecResp>(kvstore) {}
};

}  // namespace meta
}  // namespace nebula

#endif  // META_CREATEEDGEINDEXPROCESSOR_H
