/* Copyright (c) 2021 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#ifndef STORAGE_TRANSACTION_CHAINPROCESSORFACTORY_H
#define STORAGE_TRANSACTION_CHAINPROCESSORFACTORY_H

#include "storage/CommonUtils.h"
#include "storage/transaction/ChainBaseProcessor.h"
#include "storage/transaction/ConsistUtil.h"

namespace nebula {
namespace storage {

class ChainProcessorFactory {
 public:
  static ChainBaseProcessor* makeProcessor(StorageEnv* env,
                                           TermID termId,
                                           const ResumeOptions& options);

  static ChainBaseProcessor* make(StorageEnv* env,
                                  GraphSpaceID spaceId,
                                  TermID termId,
                                  const std::string& edgeKey,
                                  ResumeType type);
};

}  // namespace storage
}  // namespace nebula
#endif
