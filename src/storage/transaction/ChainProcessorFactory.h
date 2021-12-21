/* Copyright (c) 2021 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#pragma once

#include "storage/CommonUtils.h"
#include "storage/transaction/ChainBaseProcessor.h"
#include "storage/transaction/ConsistUtil.h"

namespace nebula {
namespace storage {

class ChainProcessorFactory {
 public:
  static ChainBaseProcessor* makeProcessor(StorageEnv* env, const ResumeOptions& options);
};

}  // namespace storage
}  // namespace nebula
