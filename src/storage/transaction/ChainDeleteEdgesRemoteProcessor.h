/* Copyright (c) 2021 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#ifndef STORAGE_TRANSACTION_CHAINDELETEEDGESREMOTEPROCESSOR_H
#define STORAGE_TRANSACTION_CHAINDELETEEDGESREMOTEPROCESSOR_H

#include "storage/BaseProcessor.h"
#include "storage/transaction/ChainBaseProcessor.h"

namespace nebula {
namespace storage {

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
