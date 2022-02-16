/* Copyright (c) 2021 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#ifndef STORAGE_TRANSACTION_CHAINDELETEEDGESREMOTEPROCESSOR_H
#define STORAGE_TRANSACTION_CHAINDELETEEDGESREMOTEPROCESSOR_H

#include <folly/Try.h>              // for Try::~Try<T>
#include <folly/futures/Promise.h>  // for PromiseException::Prom...

#include <string>   // for string
#include <utility>  // for move

#include "interface/gen-cpp2/storage_types.h"  // for ExecResponse, ChainDel...
#include "storage/BaseProcessor.h"             // for BaseProcessor
#include "storage/transaction/ChainBaseProcessor.h"

namespace nebula {
namespace storage {
class StorageEnv;

class StorageEnv;

class ChainDeleteEdgesRemoteProcessor : public BaseProcessor<cpp2::ExecResponse> {
 public:
  static ChainDeleteEdgesRemoteProcessor* instance(StorageEnv* env) {
    return new ChainDeleteEdgesRemoteProcessor(env);
  }

  void process(const cpp2::ChainDeleteEdgesRequest& req);

 private:
  explicit ChainDeleteEdgesRemoteProcessor(StorageEnv* env)
      : BaseProcessor<cpp2::ExecResponse>(env) {}

  void commit(const cpp2::DeleteEdgesRequest& req);

  cpp2::DeleteEdgesRequest toDeleteEdgesRequest(const cpp2::ChainDeleteEdgesRequest& req);

 private:
  std::string txnId_;
};

}  // namespace storage
}  // namespace nebula
#endif
