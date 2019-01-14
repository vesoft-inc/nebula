/* Copyright (c) 2018 - present, VE Software Inc. All rights reserved
 *
 * This source code is licensed under Apache 2.0 License
 *  (found in the LICENSE.Apache file in the root directory)
 */

#ifndef STORAGE_BASEPROCESSOR_H_
#define STORAGE_BASEPROCESSOR_H_

#include "base/Base.h"
#include <folly/SpinLock.h>
#include <folly/futures/Promise.h>
#include <folly/futures/Future.h>
#include "interface/gen-cpp2/storage_types.h"
#include "kvstore/include/KVStore.h"
#include "meta/SchemaManager.h"
#include "dataman/RowSetWriter.h"
#include "dataman/RowReader.h"
#include "dataman/RowWriter.h"
#include "storage/Collector.h"
#include "time/Duration.h"

namespace nebula {
namespace storage {

template<typename RESP>
class BaseProcessor {
public:
    explicit BaseProcessor(kvstore::KVStore* kvstore)
            : kvstore_(kvstore) {}

    virtual ~BaseProcessor() = default;

    folly::Future<RESP> getFuture() {
        return promise_.getFuture();
    }

protected:
    /**
     * Destroy current instance when finished.
     * */
    void onFinished() {
        promise_.setValue(std::move(resp_));
        delete this;
    }

    void doPut(GraphSpaceID spaceId, PartitionID partId, std::vector<kvstore::KV> data);

    cpp2::ColumnDef columnDef(std::string name, cpp2::SupportedType type) {
        cpp2::ColumnDef column;
        column.name = std::move(name);
        column.type.type = type;
        return column;
    }

    cpp2::ErrorCode to(kvstore::ResultCode code);

    void pushResultCode(cpp2::ErrorCode code, PartitionID partId) {
        cpp2::ResultCode thriftRet;
        thriftRet.code = code;
        thriftRet.part_id = partId;
        resp_.codes.emplace_back(std::move(thriftRet));
    }

protected:
    kvstore::KVStore* kvstore_ = nullptr;
    RESP resp_;
    folly::Promise<RESP> promise_;

    time::Duration duration_;
    std::vector<cpp2::ResultCode> codes_;
    folly::SpinLock lock_;
    int32_t callingNum_;
};

}  // namespace storage
}  // namespace nebula

#include "storage/BaseProcessor.inl"

#endif  // STORAGE_BASEPROCESSOR_H_
