/* Copyright (c) 2018 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#ifndef META_LISTSPACESPROCESSOR_H_
#define META_LISTSPACESPROCESSOR_H_

#include "meta/processors/BaseProcessor.h"

namespace nebula {
namespace meta {

/**
 * @brief Used for command `show spaces`, return all spaces' names.
 *
 */
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
