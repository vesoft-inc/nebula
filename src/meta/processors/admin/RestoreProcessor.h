/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#ifndef META_RESTOREPROCESSOR_H_
#define META_RESTOREPROCESSOR_H_

#include "kvstore/KVEngine.h"
#include "meta/processors/BaseProcessor.h"

namespace nebula {
namespace meta {

/**
 * @brief Rebuild the host related meta info after ingesting the table backup data to
 *        the new cluster metad KV store.
 *
 *        For example, we have an cluster A which consists of hosts
 *        [ip1:port1, ip2:port2, ip3:port3]. Now we use backup data from cluster A to restore in a
 *        new cluster B which consists of hosts [ip4:port4, ip5:port5, ip6:port6].
 *        Because after ingesting, the meta info has data consists of old addresses,
 *        we should replace the related info with [from, to] pairs:
 *        ip1:port1->ip4:port4, ip2:port2->ip5:port5, ip3:port3->ip6:port6.
 *
 *        Now the related meta info including:
 *        1. part info: graph + part -> part peers list
 *        2. zone info: zone id -> zone hosts list
 *        3. machine info: machine key -> ""
 *
 */
class RestoreProcessor : public BaseProcessor<cpp2::RestoreMetaResp> {
 public:
  static RestoreProcessor* instance(kvstore::KVStore* kvstore) {
    return new RestoreProcessor(kvstore);
  }
  void process(const cpp2::RestoreMetaReq& req);

 private:
  explicit RestoreProcessor(kvstore::KVStore* kvstore)
      : BaseProcessor<cpp2::RestoreMetaResp>(kvstore) {}

  nebula::cpp2::ErrorCode replaceHostInPartition(kvstore::WriteBatch* batch,
                                                 std::map<HostAddr, HostAddr>& hostMap);

  nebula::cpp2::ErrorCode replaceHostInZone(kvstore::WriteBatch* batch,
                                            std::map<HostAddr, HostAddr>& hostMap);

  nebula::cpp2::ErrorCode replaceHostInMachine(kvstore::WriteBatch* batch,
                                               std::map<HostAddr, HostAddr>& hostMap);
};

}  // namespace meta
}  // namespace nebula

#endif  // META_RESTOREPROCESSOR_H_
