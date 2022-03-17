/* Copyright (c) 2019 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#ifndef META_LISTPARTSPROCESSOR_H_
#define META_LISTPARTSPROCESSOR_H_

#include "meta/processors/BaseProcessor.h"
#include "meta/processors/admin/AdminClient.h"

namespace nebula {
namespace meta {

/**
 * @brief Used for command `show parts`, will show leader peer, active peers and lost peers of given
 *        parts or all parts.
 *
 */
class ListPartsProcessor : public BaseProcessor<cpp2::ListPartsResp> {
 public:
  static ListPartsProcessor* instance(kvstore::KVStore* kvstore) {
    return new ListPartsProcessor(kvstore);
  }

  void process(const cpp2::ListPartsReq& req);

 private:
  explicit ListPartsProcessor(kvstore::KVStore* kvstore)
      : BaseProcessor<cpp2::ListPartsResp>(kvstore) {}

  /**
   * @brief Fill the given partItems with leader distribution.
   *
   * @param partItems
   * @return nebula::cpp2::ErrorCode
   */
  nebula::cpp2::ErrorCode getLeaderDist(std::vector<cpp2::PartItem>& partItems);

 private:
  GraphSpaceID spaceId_;
  std::vector<PartitionID> partIds_;
  bool showAllParts_{true};
};

}  // namespace meta
}  // namespace nebula

#endif  // META_LISTPARTSPROCESSOR_H_
