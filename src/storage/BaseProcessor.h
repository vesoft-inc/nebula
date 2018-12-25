/* Copyright (c) 2018 - present, VE Software Inc. All rights reserved
 *
 * This source code is licensed under Apache 2.0 License
 *  (found in the LICENSE.Apache file in the root directory)
 */

#ifndef STORAGE_BASEPROCESSOR_H_
#define STORAGE_BASEPROCESSOR_H_

#include "base/Base.h"
#include <folly/futures/Promise.h>
#include <folly/futures/Future.h>
#include "interface/gen-cpp2/storage_types.h"
#include "kvstore/include/KVStore.h"
#include "meta/SchemaManager.h"
#include "dataman/RowSetWriter.h"
#include "dataman/RowReader.h"
#include "dataman/RowWriter.h"

#ifndef CHECK_AND_WRITE
#define CHECK_AND_WRITE(ret) \
    if (ResultType::SUCCEEDED == ret) { \
        writer << v; \
    }
#endif

namespace nebula {
namespace storage {

template<typename REQ, typename RESP>
class BaseProcessor {
public:
    BaseProcessor(kvstore::KVStore* kvstore)
        : kvstore_(kvstore) {}

    virtual ~BaseProcessor() = default;

    folly::Future<RESP> getFuture() {
        return promise_.getFuture();
    }

    virtual void process(const REQ& req) = 0;

protected:
    /**
     * Destroy current instance when finished.
     * */
    void onFinished() {
        promise_.setValue(std::move(resp_));
        delete this;
    }

    cpp2::ErrorCode to(kvstore::ResultCode code) {
        switch (code) {
        case kvstore::ResultCode::SUCCESSED:
            return cpp2::ErrorCode::SUCCEEDED;
        case kvstore::ResultCode::ERR_LEADER_CHANAGED:
            return cpp2::ErrorCode::E_LEADER_CHANGED;
        case kvstore::ResultCode::ERR_SPACE_NOT_FOUND:
            return cpp2::ErrorCode::E_SPACE_NOT_FOUND;
        case kvstore::ResultCode::ERR_PART_NOT_FOUND:
            return cpp2::ErrorCode::E_PART_NOT_FOUND;
        default:
            return cpp2::ErrorCode::E_UNKNOWN;
        }
    }

    cpp2::ColumnDef columnDef(std::string name, cpp2::SupportedType type) {
        cpp2::ColumnDef column;
        column.name = std::move(name);
        column.type.type = type;
        return column;
    }
    /**
     * It will parse inputRow as inputSchema, and output data into rowWriter.
     * */
    void output(SchemaProviderIf* inputSchema, folly::StringPiece inputRow,
                int32_t ver, SchemaProviderIf* outputSchema, RowWriter& writer) {
        CHECK_NOTNULL(inputSchema);
        CHECK_NOTNULL(outputSchema);
        for (auto i = 0; i < inputSchema->getNumFields(ver); i++) {
            const auto* name = inputSchema->getFieldName(i, ver);
            VLOG(3) << "input schema: name = " << name;
        }
        RowReader reader(inputSchema, inputRow);
        auto numFields = outputSchema->getNumFields(ver);
        VLOG(3) << "Total output columns is " << numFields;
        for (auto i = 0; i < numFields; i++) {
            const auto* name = outputSchema->getFieldName(i, ver);
            const auto* type = outputSchema->getFieldType(i, ver);
            VLOG(3) << "output schema " << name;
            switch (type->type) {
                case cpp2::SupportedType::BOOL: {
                    bool v;
                    auto ret = reader.getBool(name, v);
                    CHECK_AND_WRITE(ret);
                    break;
                }
                case cpp2::SupportedType::INT: {
                    int32_t v;
                    auto ret = reader.getInt<int32_t>(name, v);
                    CHECK_AND_WRITE(ret);
                    break;
                }
                case cpp2::SupportedType::VID: {
                    int64_t v;
                    auto ret = reader.getVid(name, v);
                    CHECK_AND_WRITE(ret);
                    break;
                }
                case cpp2::SupportedType::FLOAT: {
                    float v;
                    auto ret = reader.getFloat(name, v);
                    CHECK_AND_WRITE(ret);
                    break;
                }
                case cpp2::SupportedType::DOUBLE: {
                    double v;
                    auto ret = reader.getDouble(name, v);
                    CHECK_AND_WRITE(ret);
                    break;
                }
                case cpp2::SupportedType::STRING: {
                    folly::StringPiece v;
                    auto ret = reader.getString(name, v);
                    CHECK_AND_WRITE(ret);
                    break;
                }
                default: {
                    LOG(FATAL) << "Unsupport type!";
                    break;
                }
            } // switch
        } // for
    }

protected:
    kvstore::KVStore* kvstore_ = nullptr;
    RESP resp_;
    folly::Promise<RESP> promise_;
};

}  // namespace storage
}  // namespace nebula
#endif  // STORAGE_BASEPROCESSOR_H_
