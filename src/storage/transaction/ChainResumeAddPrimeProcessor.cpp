/* Copyright (c) 2021 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#include "storage/transaction/ChainResumeAddPrimeProcessor.h"

#include <folly/futures/Future.h>           // for SemiFuture::rele...
#include <thrift/lib/cpp/util/EnumUtils.h>  // for enumNameSafe

#include <memory>         // for allocator_traits...
#include <unordered_map>  // for _Node_const_iter...
#include <utility>        // for pair
#include <vector>         // for vector

#include "clients/meta/MetaClient.h"                 // for MetaClient
#include "common/base/Logging.h"                     // for COMPACT_GOOGLE_L...
#include "common/base/StatusOr.h"                    // for StatusOr
#include "common/datatypes/Value.h"                  // for Value
#include "interface/gen-cpp2/storage_types.h"        // for AddEdgesRequest
#include "kvstore/Common.h"                          // for KV
#include "storage/CommonUtils.h"                     // for StorageEnv
#include "storage/transaction/ChainBaseProcessor.h"  // for Code
#include "storage/transaction/ConsistUtil.h"         // for ConsistUtil
#include "storage/transaction/TransactionManager.h"  // for TransactionManager

namespace nebula {
namespace storage {

ChainResumeAddPrimeProcessor::ChainResumeAddPrimeProcessor(StorageEnv* env, const std::string& val)
    : ChainAddEdgesLocalProcessor(env) {
  req_ = ConsistUtil::parseAddRequest(val);

  uuid_ = ConsistUtil::strUUID();
  execDesc_ = ", ResumePrime. ";
}

folly::SemiFuture<nebula::cpp2::ErrorCode> ChainResumeAddPrimeProcessor::prepareLocal() {
  VLOG(2) << uuid_ << " resume prime " << readableEdgeDesc_;
  prepareRequest(req_);
  if (rcPrepare_ != Code::SUCCEEDED) {
    return rcPrepare_;
  }
  auto spaceId = req_.get_space_id();
  auto numOfPart = env_->metaClient_->partsNum(spaceId);
  if (!numOfPart.ok()) {
    rcPrepare_ = Code::E_SPACE_NOT_FOUND;
    return rcPrepare_;
  }
  auto& parts = req_.get_parts();
  auto& srcId = parts.begin()->second.back().get_key().get_src().getStr();
  auto& dstId = parts.begin()->second.back().get_key().get_dst().getStr();
  localPartId_ = env_->metaClient_->partId(numOfPart.value(), srcId);
  remotePartId_ = env_->metaClient_->partId(numOfPart.value(), dstId);

  return rcPrepare_;
}

folly::SemiFuture<Code> ChainResumeAddPrimeProcessor::processRemote(Code code) {
  VLOG(2) << uuid_ << " prepareLocal() " << apache::thrift::util::enumNameSafe(code);
  return ChainAddEdgesLocalProcessor::processRemote(code);
}

/**
 * @brief this most import difference to ChainAddEdgesLocalProcessor is
 *        we can not abort, (delete an exist prime)
 * @return folly::SemiFuture<Code>
 */
folly::SemiFuture<Code> ChainResumeAddPrimeProcessor::processLocal(Code) {
  VLOG(2) << uuid_ << " processRemote() " << apache::thrift::util::enumNameSafe(rcRemote_);
  if (rcPrepare_ != Code::SUCCEEDED) {
    return rcPrepare_;
  }

  auto currTerm = env_->txnMan_->getTermFromKVStore(spaceId_, localPartId_);
  if (currTerm.first != term_) {
    rcCommit_ = Code::E_LEADER_CHANGED;
  }

  if (rcRemote_ == Code::E_RPC_FAILURE) {
    kvAppend_ = ChainAddEdgesLocalProcessor::makeDoublePrime();
  }

  if (rcRemote_ == Code::E_RPC_FAILURE || rcRemote_ == Code::SUCCEEDED) {
    // if there are something wrong other than rpc failure
    // we need to keep the resume retry(by not remove those prime key)
    erasePrime();
    return commit();
  }

  return rcRemote_;
}

}  // namespace storage
}  // namespace nebula
