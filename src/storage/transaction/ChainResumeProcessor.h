/* Copyright (c) 2021 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#pragma once

#include "clients/storage/InternalStorageClient.h"
#include "common/utils/NebulaKeyUtils.h"
#include "storage/transaction/ChainAddEdgesProcessorLocal.h"
#include "storage/transaction/ChainBaseProcessor.h"
#include "storage/transaction/ChainUpdateEdgeProcessorLocal.h"
#include "storage/transaction/TransactionManager.h"

namespace nebula {
namespace storage {

class ChainResumeProcessor {
  friend class ChainResumeProcessorTestHelper;

 public:
  explicit ChainResumeProcessor(StorageEnv* env) : env_(env) {}

  void process();

 private:
  StorageEnv* env_{nullptr};
};

}  // namespace storage
}  // namespace nebula
