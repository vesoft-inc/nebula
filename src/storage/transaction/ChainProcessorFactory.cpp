/* Copyright (c) 2021 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "storage/transaction/ChainProcessorFactory.h"

#include "storage/transaction/ConsistUtil.h"
#include "storage/transaction/ResumeAddEdgeProcessor.h"
#include "storage/transaction/ResumeAddEdgeRemoteProcessor.h"
#include "storage/transaction/ResumeUpdateProcessor.h"
#include "storage/transaction/ResumeUpdateRemoteProcessor.h"

namespace nebula {
namespace storage {

ChainBaseProcessor* ChainProcessorFactory::makeProcessor(StorageEnv* env,
                                                         const ResumeOptions& options) {
  ChainBaseProcessor* ret = nullptr;
  auto requestType = ConsistUtil::parseType(options.primeValue);
  switch (requestType) {
    case RequestType::INSERT: {
      switch (options.resumeType) {
        case ResumeType::RESUME_CHAIN: {
          ret = ResumeAddEdgeProcessor::instance(env, options.primeValue);
          break;
        }
        case ResumeType::RESUME_REMOTE: {
          ret = ResumeAddEdgeRemoteProcessor::instance(env, options.primeValue);
          break;
        }
        case ResumeType::UNKNOWN: {
          LOG(FATAL) << "ResumeType::UNKNOWN: not supposed run here";
        }
      }
      break;
    }
    case RequestType::UPDATE: {
      switch (options.resumeType) {
        case ResumeType::RESUME_CHAIN: {
          ret = ResumeUpdateProcessor::instance(env, options.primeValue);
          break;
        }
        case ResumeType::RESUME_REMOTE: {
          ret = ResumeUpdateRemoteProcessor::instance(env, options.primeValue);
          break;
        }
        case ResumeType::UNKNOWN: {
          LOG(FATAL) << "ResumeType::UNKNOWN: not supposed run here";
        }
      }
      break;
    }
    case RequestType::UNKNOWN: {
      LOG(FATAL) << "RequestType::UNKNOWN: not supposed run here";
    }
  }
  return ret;
}

}  // namespace storage
}  // namespace nebula
