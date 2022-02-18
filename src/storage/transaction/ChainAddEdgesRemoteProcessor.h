/* Copyright (c) 2021 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#ifndef STORAGE_TRANSACTION_CHAINADDEDGESPROCESSORREMOTE_H
#define STORAGE_TRANSACTION_CHAINADDEDGESPROCESSORREMOTE_H

#include "storage/BaseProcessor.h"
#include "storage/transaction/ChainBaseProcessor.h"

namespace nebula {
namespace storage {

class ChainAddEdgesRemoteProcessor : public BaseProcessor<cpp2::ExecResponse> {
 public:
  static ChainAddEdgesRemoteProcessor* instance(StorageEnv* env) {
    return new ChainAddEdgesRemoteProcessor(env);
  }

  void process(const cpp2::ChainAddEdgesRequest& req);

 private:
  explicit ChainAddEdgesRemoteProcessor(StorageEnv* env) : BaseProcessor<cpp2::ExecResponse>(env) {}

  void commit(const cpp2::ChainAddEdgesRequest& req);

  std::vector<std::string> getStrEdgeKeys(const cpp2::ChainAddEdgesRequest& req);

 private:
  std::string uuid_;  // for debug purpose
};

}  // namespace storage
}  // namespace nebula
#endif
