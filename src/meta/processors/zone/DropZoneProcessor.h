/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#ifndef META_DROPZONEPROCESSOR_H
#define META_DROPZONEPROCESSOR_H

#include "meta/processors/BaseProcessor.h"

namespace nebula {
namespace meta {

/**
 * @brief Drop zone from the cluster
 * The hosts that belong to the zone should not contain any parts
 * It will drop the hosts too
 */
class DropZoneProcessor : public BaseProcessor<cpp2::ExecResp> {
 public:
  static DropZoneProcessor* instance(kvstore::KVStore* kvstore) {
    return new DropZoneProcessor(kvstore);
  }

  void process(const cpp2::DropZoneReq& req);

 private:
  explicit DropZoneProcessor(kvstore::KVStore* kvstore) : BaseProcessor<cpp2::ExecResp>(kvstore) {}

  /**
   * @brief check all spaces if they have enough zones to hold replica when dropping one zone
   *
   * @return
   */
  nebula::cpp2::ErrorCode checkSpaceReplicaZone();

  /**
   * @brief Check whether the node holds zones on each space
   *
   * @param address
   * @return
   */
  nebula::cpp2::ErrorCode checkHostPartition(const HostAddr& address);
};

}  // namespace meta
}  // namespace nebula

#endif  // META_DROPZONEPROCESSOR_H
