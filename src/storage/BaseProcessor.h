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
#include "meta/SchemaManager.h"
#include "time/Duration.h"

namespace nebula {
namespace storage {

template<typename RESP>
class BaseProcessor {
public:
    explicit BaseProcessor(kvstore::KVStore* kvstore, meta::SchemaManager* schemaMan)
            : kvstore_(kvstore)
            , schemaMan_(schemaMan) {}

    virtual ~BaseProcessor() = default;

    folly::Future<RESP> getFuture() {
        return promise_.getFuture();
    }

protected:
    /**
     * Destroy current instance when finished.
     * */
    void onFinished() {
        result_.set_latency_in_us(duration_.elapsedInUSec());
        resp_.set_result(std::move(result_));
        promise_.setValue(std::move(resp_));
        delete this;
    }

    void doPut(GraphSpaceID spaceId, PartitionID partId, std::vector<kvstore::KV> data);

    nebula::cpp2::ColumnDef columnDef(std::string name, nebula::cpp2::SupportedType type) {
        nebula::cpp2::ColumnDef column;
        column.set_name(std::move(name));
        nebula::cpp2::ValueType vType;
        vType.set_type(type);
        column.set_type(std::move(vType));
        return column;
    }

    cpp2::ErrorCode to(kvstore::ResultCode code);

    void pushResultCode(cpp2::ErrorCode code, PartitionID partId) {
        if (code != cpp2::ErrorCode::SUCCEEDED) {
            std::lock_guard<folly::SpinLock> lg(this->lock_);
            cpp2::ResultCode thriftRet;
            thriftRet.set_code(code);
            thriftRet.set_part_id(partId);
            result_.failed_codes.emplace_back(std::move(thriftRet));
        }
    }

protected:
    kvstore::KVStore*       kvstore_ = nullptr;
    meta::SchemaManager*    schemaMan_ = nullptr;
    RESP                    resp_;
    folly::Promise<RESP>    promise_;
    cpp2::ResponseCommon    result_;

    time::Duration          duration_;
    std::vector<cpp2::ResultCode> codes_;
    folly::SpinLock         lock_;
    int32_t                 callingNum_ = 0;
};

}  // namespace storage
}  // namespace nebula

#include "storage/BaseProcessor.inl"

#endif  // STORAGE_BASEPROCESSOR_H_
