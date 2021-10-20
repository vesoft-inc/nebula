/* Copyright (c) 2021 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "storage/transaction/ChainResumeProcessor.h"

#include "storage/transaction/ChainAddEdgesProcessorLocal.h"
#include "storage/transaction/ChainProcessorFactory.h"
#include "storage/transaction/ChainUpdateEdgeProcessorLocal.h"
#include "storage/transaction/ConsistUtil.h"
#include "storage/transaction/TransactionManager.h"

namespace nebula {
namespace storage {

void ChainResumeProcessor::process() {
  auto* table = env_->txnMan_->getReserveTable();
  std::unique_ptr<kvstore::KVIterator> iter;
  for (auto it = table->begin(); it != table->end(); ++it) {
    auto spaceId = *reinterpret_cast<GraphSpaceID*>(const_cast<char*>(it->first.c_str()));
    auto edgeKey = std::string(it->first.c_str() + sizeof(GraphSpaceID),
                               it->first.size() - sizeof(GraphSpaceID));
    auto partId = NebulaKeyUtils::getPart(edgeKey);
    auto prefix = (it->second == ResumeType::RESUME_CHAIN) ? ConsistUtil::primeTable()
                                                           : ConsistUtil::doublePrimeTable();
    auto key = prefix + edgeKey;
    std::string val;
    auto rc = env_->kvstore_->get(spaceId, partId, key, &val);
    VLOG(1) << "resume edge space=" << spaceId << ", part=" << partId
            << ", hex = " << folly::hexlify(edgeKey)
            << ", rc = " << apache::thrift::util::enumNameSafe(rc);
    if (rc == nebula::cpp2::ErrorCode::SUCCEEDED) {
      // do nothing
    } else if (rc == nebula::cpp2::ErrorCode::E_LEADER_CHANGED) {
      VLOG(1) << "kvstore->get() leader changed";
      auto getPart = env_->kvstore_->part(spaceId, partId);
      if (nebula::ok(getPart) && !nebula::value(getPart)->isLeader()) {
        // not leader any more, stop trying resume
        env_->txnMan_->delPrime(spaceId, edgeKey);
      }
      continue;
    } else {
      LOG(WARNING) << "kvstore->get() failed, " << apache::thrift::util::enumNameSafe(rc);
      continue;
    }
    ResumeOptions opt(it->second, val);
    auto* proc = ChainProcessorFactory::makeProcessor(env_, opt);
    auto fut = proc->getFinished();
    env_->txnMan_->addChainTask(proc);
    std::move(fut)
        .thenValue([=](auto&& code) {
          if (code == Code::SUCCEEDED) {
            env_->txnMan_->delPrime(spaceId, edgeKey);
          }
        })
        .get();
  }
}

}  // namespace storage
}  // namespace nebula
