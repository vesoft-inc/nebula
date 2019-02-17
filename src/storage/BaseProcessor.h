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
#include "kvstore/KVStore.h"
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
        result_.set_latency_in_ms(duration_.elapsedInMSec());
        resp_.set_result(std::move(result_));
        promise_.setValue(std::move(resp_));
        delete this;
    }

    void doPut(GraphSpaceID spaceId, PartitionID partId, std::vector<kvstore::KV> data);

    cpp2::ColumnDef columnDef(std::string name, cpp2::SupportedType type) {
        cpp2::ColumnDef column;
        column.set_name(std::move(name));
        cpp2::ValueType vType;
        vType.set_type(type);
        column.set_type(std::move(vType));
        return column;
    }

    cpp2::ErrorCode to(kvstore::ResultCode code);

    void pushResultCode(cpp2::ErrorCode code, PartitionID partId) {
        if (code != cpp2::ErrorCode::SUCCEEDED) {
            cpp2::ResultCode thriftRet;
            thriftRet.set_code(code);
            thriftRet.set_part_id(partId);
            result_.failed_codes.emplace_back(std::move(thriftRet));
        }
    }

protected:
    kvstore::KVStore* kvstore_ = nullptr;
    RESP resp_;
    folly::Promise<RESP> promise_;
    cpp2::ResponseCommon result_;

    time::Duration duration_;
    std::vector<cpp2::ResultCode> codes_;
    folly::SpinLock lock_;
    int32_t callingNum_;
};

}  // namespace storage
}  // namespace nebula

#include "storage/BaseProcessor.inl"

#endif  // STORAGE_BASEPROCESSOR_H_
