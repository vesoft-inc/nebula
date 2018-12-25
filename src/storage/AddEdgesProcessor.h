/* Copyright (c) 2018 - present, VE Software Inc. All rights reserved
 *
 * This source code is licensed under Apache 2.0 License
 *  (found in the LICENSE.Apache file in the root directory)
 */

#ifndef STORAGE_ADDEDGESPROCESSOR_H_
#define STORAGE_ADDEDGESPROCESSOR_H_

#include "base/Base.h"
#include <folly/SpinLock.h>
#include <folly/futures/Promise.h>
#include <folly/futures/Future.h>
#include "kvstore/include/KVStore.h"
#include "interface/gen-cpp2/storage_types.h"
#include "storage/BaseProcessor.h"

namespace nebula {
namespace storage {

class AddEdgesProcessor : public BaseProcessor<cpp2::AddEdgesRequest, cpp2::ExecResponse> {
public:
    static AddEdgesProcessor* instance(kvstore::KVStore* kvstore) {
        return new AddEdgesProcessor(kvstore);
    }

    void process(const cpp2::AddEdgesRequest& req) override {
        VLOG(1) << req.get_space_id();
    }

private:
    AddEdgesProcessor(kvstore::KVStore* kvstore)
        : BaseProcessor<cpp2::AddEdgesRequest, cpp2::ExecResponse>(kvstore) {}
};


}  // namespace storage
}  // namespace nebula
#endif  // STORAGE_ADDEDGESPROCESSOR_H_
