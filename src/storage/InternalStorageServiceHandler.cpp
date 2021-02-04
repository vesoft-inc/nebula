/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "storage/InternalStorageServiceHandler.h"
#include "storage/transaction/TransactionProcessor.h"
#include "storage/transaction/GetValueProcessor.h"

#define RETURN_FUTURE(processor) \
    auto f = processor->getFuture(); \
    processor->process(req); \
    return f;

namespace nebula {
namespace storage {

InternalStorageServiceHandler::InternalStorageServiceHandler(StorageEnv* env)
        : env_(env) {
    kForwardTranxCounters.init("forward_tranx");
    kGetValueCounters.init("get_value");
}


folly::Future<cpp2::ExecResponse>
InternalStorageServiceHandler::future_forwardTransaction(const cpp2::InternalTxnRequest& req) {
    auto* processor = InterTxnProcessor::instance(env_);
    RETURN_FUTURE(processor);
}


folly::Future<cpp2::GetValueResponse>
InternalStorageServiceHandler::future_getValue(const cpp2::GetValueRequest& req) {
    auto* processor = GetValueProcessor::instance(env_);
    RETURN_FUTURE(processor);
}


}  // namespace storage
}  // namespace nebula
