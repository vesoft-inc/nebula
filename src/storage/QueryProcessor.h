/* Copyright (c) 2018 - present, VE Software Inc. All rights reserved
 *
 * This source code is licensed under Apache 2.0 License
 *  (found in the LICENSE.Apache file in the root directory)
 */

#ifndef STORAGE_QUERYPROCESSOR_H_
#define STORAGE_QUERYPROCESSOR_H_

#include "base/Base.h"
#include <folly/SpinLock.h>
#include <folly/futures/Promise.h>
#include <folly/futures/Future.h>
#include "kvstore/include/KVStore.h"
#include "interface/gen-cpp2/storage_types.h"
#include "storage/BaseProcessor.h"

namespace nebula {
namespace storage {

class QueryStatsProcessor : public BaseProcessor<cpp2::NeighborsStatsRequest, cpp2::QueryResponse> {
public:
    static QueryStatsProcessor* instance(kvstore::KVStore* kvstore) {
        return new QueryStatsProcessor(kvstore);
    }

    void process(const cpp2::NeighborsStatsRequest& req) override {
        VLOG(1) << req.get_space_id();
    }

private:
    QueryStatsProcessor(kvstore::KVStore* kvstore)
        : BaseProcessor<cpp2::NeighborsStatsRequest, cpp2::QueryResponse>(kvstore) {}
};

}  // namespace storage
}  // namespace nebula
#endif  // STORAGE_QUERYPROCESSOR_H_
