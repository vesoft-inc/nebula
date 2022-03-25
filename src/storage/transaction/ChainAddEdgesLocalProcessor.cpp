/* Copyright (c) 2021 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#include "storage/transaction/ChainAddEdgesLocalProcessor.h"

#include <thrift/lib/cpp/util/EnumUtils.h>

#include "common/utils/DefaultValueContext.h"
#include "kvstore/Part.h"
#include "storage/StorageFlags.h"
#include "storage/mutate/AddEdgesProcessor.h"
#include "storage/transaction/ConsistUtil.h"
#include "storage/transaction/TransactionManager.h"

namespace nebula {
namespace storage {

void ChainAddEdgesLocalProcessor::process(const cpp2::AddEdgesRequest& req) {
  if (!prepareRequest(req)) {
    finish();
    return;
  }

  uuid_ = ConsistUtil::strUUID();
  execDesc_ = ", AddEdges, ";
  env_->txnMan_->addChainTask(this);
}

folly::SemiFuture<Code> ChainAddEdgesLocalProcessor::prepareLocal() {
  VLOG(2) << uuid_ << __func__ << "()";
  std::tie(term_, rcPrepare_) = env_->txnMan_->getTermFromKVStore(spaceId_, localPartId_);
  if (rcPrepare_ != Code::SUCCEEDED) {
    finish();
    return rcPrepare_;
  }

  if (!lockEdges(req_)) {
    rcPrepare_ = Code::E_WRITE_WRITE_CONFLICT;
    return Code::E_WRITE_WRITE_CONFLICT;
  }
  replaceNullWithDefaultValue(req_);

  auto [pro, fut] = folly::makePromiseContract<Code>();
  auto primes = makePrime();

  erasePrime();
  env_->kvstore_->asyncMultiPut(
      spaceId_, localPartId_, std::move(primes), [p = std::move(pro), this](auto rc) mutable {
        rcPrepare_ = rc;
        p.setValue(rc);
      });
  return std::move(fut);
}

folly::SemiFuture<Code> ChainAddEdgesLocalProcessor::processRemote(Code code) {
  VLOG(2) << uuid_ << " prepareLocal() " << apache::thrift::util::enumNameSafe(code);
  if (rcPrepare_ != Code::SUCCEEDED) {
    return rcPrepare_;
  }
  CHECK_EQ(req_.get_parts().size(), 1);
  auto reversedRequest = reverseRequest(req_);
  CHECK_EQ(reversedRequest.get_parts().size(), 1);
  auto [pro, fut] = folly::makePromiseContract<Code>();
  doRpc(std::move(pro), std::move(reversedRequest));
  return std::move(fut);
}

folly::SemiFuture<Code> ChainAddEdgesLocalProcessor::processLocal(Code) {
  VLOG(2) << uuid_ << " processRemote(), code = " << apache::thrift::util::enumNameSafe(rcRemote_);
  if (rcPrepare_ != Code::SUCCEEDED) {
    return rcPrepare_;
  }

  auto currTerm = env_->txnMan_->getTermFromKVStore(spaceId_, localPartId_);
  if (currTerm.first != term_) {
    rcCommit_ = Code::E_LEADER_CHANGED;
    return rcCommit_;
  }

  if (rcRemote_ == Code::E_RPC_FAILURE) {
    kvAppend_ = makeDoublePrime();
  }

  if (rcRemote_ != Code::SUCCEEDED && rcRemote_ != Code::E_RPC_FAILURE) {
    // prepare succeed and remote failed
    return abort();
  }

  return commit();
}

void ChainAddEdgesLocalProcessor::reportFailed(ResumeType type) {
  if (lk_ != nullptr) {
    lk_->setAutoUnlock(false);
  }
  execDesc_ += ", reportFailed";
  auto keys = toStrKeys(req_);
  for (auto& key : keys) {
    VLOG(1) << uuid_ << " term=" << term_ << ", reportFailed(), " << folly::hexlify(key);
    env_->txnMan_->addPrime(spaceId_, localPartId_, term_, key, type);
  }
}

bool ChainAddEdgesLocalProcessor::prepareRequest(const cpp2::AddEdgesRequest& req) {
  CHECK_EQ(req.get_parts().size(), 1);
  req_ = req;
  spaceId_ = req_.get_space_id();
  localPartId_ = req.get_parts().begin()->first;
  return getSpaceVidLen(spaceId_) == Code::SUCCEEDED;
}

folly::SemiFuture<Code> ChainAddEdgesLocalProcessor::commit() {
  auto* proc = AddEdgesProcessor::instance(env_, nullptr);
  proc->consistOp_ = [&](kvstore::BatchHolder& a, std::vector<kvstore::KV>* b) {
    callbackOfChainOp(a, b);
  };
  auto futProc = proc->getFuture();
  auto [pro, fut] = folly::makePromiseContract<Code>();
  std::move(futProc).thenTry([&, p = std::move(pro)](auto&& t) mutable {
    execDesc_ += ", commit(), ";
    if (t.hasException()) {
      LOG(INFO) << "catch ex: " << t.exception().what();
      rcCommit_ = Code::E_UNKNOWN;
    } else {
      auto& resp = t.value();
      rcCommit_ = extractRpcError(resp);
    }
    p.setValue(rcCommit_);
  });
  proc->process(req_);
  return std::move(fut);
}

Code ChainAddEdgesLocalProcessor::extractRpcError(const cpp2::ExecResponse& resp) {
  Code ret = Code::SUCCEEDED;
  auto& respComn = resp.get_result();
  for (auto& part : respComn.get_failed_parts()) {
    ret = part.code;
  }
  return ret;
}

void ChainAddEdgesLocalProcessor::doRpc(folly::Promise<Code>&& promise,
                                        cpp2::AddEdgesRequest&& req,
                                        int retry) noexcept {
  if (retry > retryLimit_) {
    promise.setValue(Code::E_LEADER_CHANGED);
    return;
  }
  auto* iClient = env_->interClient_;
  folly::Promise<Code> p;
  auto f = p.getFuture();
  iClient->chainAddEdges(req, term_, edgeVer_, std::move(p));

  std::move(f).thenTry([=, p = std::move(promise)](auto&& t) mutable {
    rcRemote_ = t.hasValue() ? t.value() : Code::E_RPC_FAILURE;
    // we treat it E_RAFT_UNKNOWN_APPEND_LOG as SUCCEEDED
    // please check this error in RaftPart.cpp for more details
    if (rcRemote_ == Code::E_RAFT_UNKNOWN_APPEND_LOG) {
      rcRemote_ = Code::SUCCEEDED;
    }
    switch (rcRemote_) {
      case Code::E_LEADER_CHANGED:
        doRpc(std::move(p), std::move(req), ++retry);
        break;
      default:
        p.setValue(rcRemote_);
        break;
    }
    return rcRemote_;
  });
}

void ChainAddEdgesLocalProcessor::callbackOfChainOp(kvstore::BatchHolder& batch,
                                                    std::vector<kvstore::KV>* pData) {
  if (pData != nullptr) {
    for (auto& kv : *pData) {
      batch.put(std::string(kv.first), std::string(kv.second));
    }
  }
  for (auto& key : kvErased_) {
    batch.remove(std::string(key));
  }
  for (auto& kv : kvAppend_) {
    batch.put(std::string(kv.first), std::string(kv.second));
  }
}

folly::SemiFuture<Code> ChainAddEdgesLocalProcessor::abort() {
  if (kvErased_.empty()) {
    return Code::SUCCEEDED;
  }

  std::vector<std::string> debugErased;
  if (FLAGS_trace_toss) {
    debugErased = kvErased_;
  }

  auto [pro, fut] = folly::makePromiseContract<Code>();
  env_->kvstore_->asyncMultiRemove(req_.get_space_id(),
                                   localPartId_,
                                   std::move(kvErased_),
                                   [p = std::move(pro), debugErased, this](auto rc) mutable {
                                     execDesc_ += ", abort(), ";
                                     this->rcCommit_ = rc;
                                     p.setValue(rc);
                                   });
  return std::move(fut);
}

std::vector<kvstore::KV> ChainAddEdgesLocalProcessor::makePrime() {
  std::vector<kvstore::KV> ret;
  for (auto& edge : req_.get_parts().begin()->second) {
    auto key = ConsistUtil::primeKey(spaceVidLen_, localPartId_, edge.get_key());

    auto req = makeSingleEdgeRequest(localPartId_, edge);
    std::string val;
    apache::thrift::CompactSerializer::serialize(req, &val);
    val.append(ConsistUtil::insertIdentifier());

    ret.emplace_back(std::make_pair(std::move(key), std::move(val)));
  }
  return ret;
}

std::vector<kvstore::KV> ChainAddEdgesLocalProcessor::makeDoublePrime() {
  std::vector<kvstore::KV> ret;
  for (auto& edge : req_.get_parts().begin()->second) {
    auto key = ConsistUtil::doublePrime(spaceVidLen_, localPartId_, edge.get_key());

    auto req = makeSingleEdgeRequest(localPartId_, edge);
    std::string val;
    apache::thrift::CompactSerializer::serialize(req, &val);
    val.append(ConsistUtil::insertIdentifier());

    ret.emplace_back(std::make_pair(std::move(key), std::move(val)));
  }
  return ret;
}

void ChainAddEdgesLocalProcessor::erasePrime() {
  auto fn = [&](const cpp2::NewEdge& edge) {
    auto key = ConsistUtil::primeKey(spaceVidLen_, localPartId_, edge.get_key());
    return key;
  };
  for (auto& edge : req_.get_parts().begin()->second) {
    kvErased_.push_back(fn(edge));
  }
}

bool ChainAddEdgesLocalProcessor::lockEdges(const cpp2::AddEdgesRequest& req) {
  auto partId = req.get_parts().begin()->first;
  lkCore_ = env_->txnMan_->getLockCore(req.get_space_id(), partId, term_);
  if (!lkCore_) {
    return false;
  }

  std::vector<std::string> keys;
  for (auto& edge : req.get_parts().begin()->second) {
    keys.emplace_back(ConsistUtil::edgeKey(spaceVidLen_, partId, edge.get_key()));
  }
  lk_ = std::make_unique<TransactionManager::LockGuard>(lkCore_.get(), keys);
  return lk_->isLocked();
}

std::vector<std::string> ChainAddEdgesLocalProcessor::toStrKeys(const cpp2::AddEdgesRequest& req) {
  std::vector<std::string> ret;
  for (auto& edgesOfPart : req.get_parts()) {
    auto partId = edgesOfPart.first;
    for (auto& edge : edgesOfPart.second) {
      ret.emplace_back(ConsistUtil::edgeKey(spaceVidLen_, partId, edge.get_key()));
    }
  }
  return ret;
}

cpp2::AddEdgesRequest ChainAddEdgesLocalProcessor::reverseRequest(
    const cpp2::AddEdgesRequest& req) {
  cpp2::AddEdgesRequest reversedRequest;
  for (auto& edgesOfPart : *req.parts_ref()) {
    for (auto& newEdge : edgesOfPart.second) {
      (*reversedRequest.parts_ref())[remotePartId_].emplace_back(newEdge);
      auto& newEdgeRef = (*reversedRequest.parts_ref())[remotePartId_].back();
      ConsistUtil::reverseEdgeKeyInplace(*newEdgeRef.key_ref());
    }
  }
  reversedRequest.space_id_ref() = req.get_space_id();
  reversedRequest.prop_names_ref() = req.get_prop_names();
  reversedRequest.if_not_exists_ref() = req.get_if_not_exists();
  return reversedRequest;
}

void ChainAddEdgesLocalProcessor::finish() {
  if (rcPrepare_ == Code::SUCCEEDED) {
    VLOG(1) << uuid_ << execDesc_ << makeReadableEdge(req_)
            << ", rcPrepare_=" << apache::thrift::util::enumNameSafe(rcPrepare_)
            << ", rcRemote_=" << apache::thrift::util::enumNameSafe(rcRemote_)
            << ", rcCommit_=" << apache::thrift::util::enumNameSafe(rcCommit_);
  }
  do {
    if (rcPrepare_ != Code::SUCCEEDED) {
      break;  // nothing written, no need to recover.
    }

    if (rcCommit_ != Code::SUCCEEDED) {
      reportFailed(ResumeType::RESUME_CHAIN);
      break;
    }

    if (rcRemote_ == Code::E_RPC_FAILURE) {
      reportFailed(ResumeType::RESUME_REMOTE);
      break;
    }
  } while (0);

  auto rc = Code::SUCCEEDED;
  do {
    if (rcPrepare_ != Code::SUCCEEDED) {
      rc = rcPrepare_;
      break;
    }

    if (rcCommit_ != Code::SUCCEEDED) {
      rc = rcCommit_;
      break;
    }

    // rcCommit_ may be set SUCCEEDED in abort().
    // which we should return the error code or remote.
    if (rcRemote_ != Code::E_RPC_FAILURE) {
      rc = rcRemote_;
      break;
    }
  } while (0);

  pushResultCode(rc, localPartId_);
  finished_.setValue(rc);
  onFinished();
}

cpp2::AddEdgesRequest ChainAddEdgesLocalProcessor::makeSingleEdgeRequest(
    PartitionID partId, const cpp2::NewEdge& edge) {
  cpp2::AddEdgesRequest req;
  req.space_id_ref() = (req_.get_space_id());
  req.prop_names_ref() = (req_.get_prop_names());
  req.if_not_exists_ref() = (req_.get_if_not_exists());

  std::unordered_map<PartitionID, std::vector<cpp2::NewEdge>> newParts;
  newParts[partId].emplace_back(edge);

  req.parts_ref() = (newParts);
  return req;
}

std::string ChainAddEdgesLocalProcessor::makeReadableEdge(const cpp2::AddEdgesRequest& req) {
  std::stringstream oss;
  oss << "term=" << term_ << ", ";
  auto rawKeyVec = toStrKeys(req);
  for (auto& rawKey : rawKeyVec) {
    oss << ConsistUtil::readableKey(spaceVidLen_, isIntId_, rawKey) << ", ";
  }

  auto& edge = req.get_parts().begin()->second.back();
  for (auto& val : edge.get_props()) {
    oss << val.toString() << " ";
  }
  return oss.str();
}

void ChainAddEdgesLocalProcessor::eraseDoublePrime() {
  auto fn = [&](const cpp2::NewEdge& edge) {
    auto key = ConsistUtil::doublePrime(spaceVidLen_, localPartId_, edge.get_key());
    return key;
  };
  for (auto& edge : req_.get_parts().begin()->second) {
    kvErased_.push_back(fn(edge));
  }
}

/*** consider the following case:
 *
 * create edge known(kdate datetime default datetime(), degree int);
 * insert edge known(degree) VALUES "100" -> "101":(95);
 *
 * storage will insert datetime() as default value on both
 * in/out edge, but they will calculate independent
 * which lead to inconsistency
 *
 * that's why we need to replace the inconsistency prone value
 * at the moment the request comes
 * */
void ChainAddEdgesLocalProcessor::replaceNullWithDefaultValue(cpp2::AddEdgesRequest& req) {
  auto& edgesOfPart = *req.parts_ref();
  if (edgesOfPart.empty()) {
    return;
  }
  auto& edgesOfFirstPart = edgesOfPart.begin()->second;
  if (edgesOfFirstPart.empty()) {
    return;
  }
  auto firstEdgeKey = edgesOfFirstPart.front().get_key();
  auto edgeType = std::abs(*firstEdgeKey.edge_type_ref());
  auto schema = env_->schemaMan_->getEdgeSchema(spaceId_, edgeType);

  DefaultValueContext expCtx;
  // the coming request has two forms
  // 1st "propNames" is empty,
  //    which means all vals should be write as the same sequence of schema
  // 2nd "propNames" is not empty
  //    vals of request should be write according to propName of schema
  // use the following "idxVec" to identify which index a val should be write to.
  std::vector<int64_t> idxVec;
  auto& propNames = *req.prop_names_ref();
  if (propNames.empty()) {
    for (auto i = 0U; i < schema->getNumFields(); ++i) {
      idxVec.emplace_back(i);
    }
  } else {
    // first scan the origin input propNames
    for (auto& name : propNames) {
      int64_t index = schema->getFieldIndex(name);
      idxVec.emplace_back(index);
    }
    // second, check if there any cols not filled but has default val
    // we need to append these cols
    for (auto i = 0U; i < schema->getNumFields(); ++i) {
      auto it = std::find(idxVec.begin(), idxVec.end(), i);
      if (it == idxVec.end()) {
        auto field = schema->field(i);
        if (field->hasDefault()) {
          idxVec.emplace_back(i);
        }
      }
    }
  }

  for (auto& part : *req.parts_ref()) {
    for (auto& edge : part.second) {
      auto& vals = *edge.props_ref();
      if (vals.size() > idxVec.size()) {
        LOG(ERROR) << folly::sformat(
            "error vals.size()={} > idxVec.size()={}", vals.size(), idxVec.size());
        continue;
      }
      for (auto i = vals.size(); i < idxVec.size(); ++i) {
        auto field = schema->field(idxVec[i]);
        if (field->hasDefault()) {
          auto exprStr = field->defaultValue();
          ObjectPool pool;
          auto expr = Expression::decode(&pool, folly::StringPiece(exprStr.data(), exprStr.size()));
          auto defVal = Expression::eval(expr, expCtx);
          switch (defVal.type()) {
            case Value::Type::BOOL:
              vals.emplace_back(defVal.getBool());
              break;
            case Value::Type::INT:
              vals.emplace_back(defVal.getInt());
              break;
            case Value::Type::FLOAT:
              vals.emplace_back(defVal.getFloat());
              break;
            case Value::Type::STRING:
              vals.emplace_back(defVal.getStr());
              break;
            case Value::Type::DATE:
              vals.emplace_back(defVal.getDate());
              break;
            case Value::Type::TIME:
              vals.emplace_back(defVal.getTime());
              break;
            case Value::Type::DATETIME:
              vals.emplace_back(defVal.getDateTime());
              break;
            default:
              // for other type, local and remote should behavior same.
              break;
          }
        } else {
          // set null
          vals.emplace_back(Value::kNullValue);
        }
      }
    }
  }
}

}  // namespace storage
}  // namespace nebula
