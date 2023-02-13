/* Copyright (c) 2021 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#include "storage/transaction/ChainProcessorFactory.h"

#include "storage/transaction/ChainDeleteEdgesResumeProcessor.h"
#include "storage/transaction/ChainDeleteEdgesResumeRemoteProcessor.h"
#include "storage/transaction/ChainResumeAddDoublePrimeProcessor.h"
#include "storage/transaction/ChainResumeAddPrimeProcessor.h"
#include "storage/transaction/ChainResumeUpdateDoublePrimeProcessor.h"
#include "storage/transaction/ChainResumeUpdatePrimeProcessor.h"
#include "storage/transaction/ConsistUtil.h"

namespace nebula {
namespace storage {

ChainBaseProcessor* ChainProcessorFactory::make(StorageEnv* env,
                                                GraphSpaceID spaceId,
                                                TermID termId,
                                                const std::string& edgeKey,
                                                ResumeType type) {
  auto partId = NebulaKeyUtils::getPart(edgeKey);
  auto prefix = (type == ResumeType::RESUME_CHAIN) ? ConsistUtil::primeTable(partId)
                                                   : ConsistUtil::doublePrimeTable(partId);
  auto key = prefix + edgeKey;
  std::string val;
  auto rc = Code::SUCCEEDED;
  do {
    rc = env->kvstore_->get(spaceId, partId, key, &val);
  } while (rc == Code::E_LEADER_LEASE_FAILED);

  if (rc != Code::SUCCEEDED) {
    VLOG(1) << "resume edge space=" << spaceId << ", part=" << partId
            << ", hex = " << folly::hexlify(edgeKey)
            << ", rc = " << apache::thrift::util::enumNameSafe(rc);
    return nullptr;
  }

  ResumeOptions opt(type, val);
  return makeProcessor(env, termId, opt);
}

ChainBaseProcessor* ChainProcessorFactory::makeProcessor(StorageEnv* env,
                                                         TermID termId,
                                                         const ResumeOptions& options) {
  ChainBaseProcessor* ret = nullptr;
  auto requestType = ConsistUtil::parseType(options.primeValue);
  switch (requestType) {
    case RequestType::INSERT: {
      switch (options.resumeType) {
        case ResumeType::RESUME_CHAIN: {
          VLOG(2) << "make ChainResumeAddPrimeProcessor";
          ret = ChainResumeAddPrimeProcessor::instance(env, options.primeValue);
          break;
        }
        case ResumeType::RESUME_REMOTE: {
          VLOG(2) << "make ChainResumeAddDoublePrimeProcessor";
          ret = ChainResumeAddDoublePrimeProcessor::instance(env, options.primeValue);
          break;
        }
        case ResumeType::UNKNOWN: {
          DLOG(FATAL) << "ResumeType::UNKNOWN: not supposed run here";
          return nullptr;
        }
      }
      break;
    }
    case RequestType::UPDATE: {
      switch (options.resumeType) {
        case ResumeType::RESUME_CHAIN: {
          VLOG(2) << "make ChainResumeUpdatePrimeProcessor";
          ret = ChainResumeUpdatePrimeProcessor::instance(env, options.primeValue);
          break;
        }
        case ResumeType::RESUME_REMOTE: {
          VLOG(2) << "make ChainResumeUpdateDoublePrimeProcessor";
          ret = ChainResumeUpdateDoublePrimeProcessor::instance(env, options.primeValue);
          break;
        }
        case ResumeType::UNKNOWN: {
          DLOG(FATAL) << "ResumeType::UNKNOWN: not supposed run here";
          return nullptr;
        }
      }
      break;
    }
    case RequestType::DELETE: {
      switch (options.resumeType) {
        case ResumeType::RESUME_CHAIN: {
          VLOG(1) << "make ChainDeleteEdgesResumeProcessor";
          ret = ChainDeleteEdgesResumeProcessor::instance(env, options.primeValue);
          break;
        }
        case ResumeType::RESUME_REMOTE: {
          VLOG(2) << "make ChainDeleteEdgesResumeRemoteProcessor";
          ret = ChainDeleteEdgesResumeRemoteProcessor::instance(env, options.primeValue);
          break;
        }
        case ResumeType::UNKNOWN: {
          DLOG(FATAL) << "ResumeType::UNKNOWN: not supposed run here";
          return nullptr;
        }
      }
      break;
    }
    case RequestType::UNKNOWN: {
      DLOG(FATAL) << "RequestType::UNKNOWN: not supposed run here";
      return nullptr;
    }
  }
  ret->term_ = termId;
  return ret;
}

}  // namespace storage
}  // namespace nebula
