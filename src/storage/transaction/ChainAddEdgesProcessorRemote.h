/* Copyright (c) 2021 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#pragma once

#include "storage/BaseProcessor.h"
#include "storage/transaction/ChainBaseProcessor.h"

namespace nebula {
namespace storage {

class ChainAddEdgesProcessorRemote : public BaseProcessor<cpp2::ExecResponse> {
 public:
  static ChainAddEdgesProcessorRemote* instance(StorageEnv* env) {
    return new ChainAddEdgesProcessorRemote(env);
  }

  void process(const cpp2::ChainAddEdgesRequest& req);

 private:
  explicit ChainAddEdgesProcessorRemote(StorageEnv* env) : BaseProcessor<cpp2::ExecResponse>(env) {}

  bool checkTerm(const cpp2::ChainAddEdgesRequest& req);

  bool checkVersion(const cpp2::ChainAddEdgesRequest& req);

  void forwardRequest(const cpp2::ChainAddEdgesRequest& req);

  std::vector<std::string> getStrEdgeKeys(const cpp2::ChainAddEdgesRequest& req);
};

}  // namespace storage
}  // namespace nebula
