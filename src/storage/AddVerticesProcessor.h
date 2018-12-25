/* Copyright (c) 2018 - present, VE Software Inc. All rights reserved
 *
 * This source code is licensed under Apache 2.0 License
 *  (found in the LICENSE.Apache file in the root directory)
 */

#ifndef STORAGE_ADDVERTICESPROCESSOR_H_
#define STORAGE_ADDVERTICESPROCESSOR_H_

#include "base/Base.h"
#include <folly/SpinLock.h>
#include <folly/futures/Promise.h>
#include <folly/futures/Future.h>
#include "kvstore/include/KVStore.h"
#include "interface/gen-cpp2/storage_types.h"
#include "storage/BaseProcessor.h"

namespace nebula {
namespace storage {

class AddVerticesProcessor : public BaseProcessor<cpp2::ExecResponse> {
public:
    static AddVerticesProcessor* instance(kvstore::KVStore* kvstore) {
        return new AddVerticesProcessor(kvstore);
    }

    void process(const cpp2::AddVerticesRequest& req);

private:
    AddVerticesProcessor(kvstore::KVStore* kvstore)
        : BaseProcessor<cpp2::ExecResponse>(kvstore) {}

private:
    std::vector<cpp2::ResultCode> codes_;
    folly::SpinLock lock_;
    int64_t startMs_;
    int32_t callingNum_;
};


}  // namespace storage
}  // namespace nebula
#endif  // STORAGE_ADDVERTICESPROCESSOR_H_
