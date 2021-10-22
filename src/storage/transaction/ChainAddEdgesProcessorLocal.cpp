/* Copyright (c) 2021 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "storage/transaction/ChainAddEdgesProcessorLocal.h"

#include <thrift/lib/cpp/util/EnumUtils.h>

#include "common/utils/DefaultValueContext.h"
#include "kvstore/Part.h"
#include "storage/StorageFlags.h"
#include "storage/mutate/AddEdgesProcessor.h"
#include "storage/transaction/ConsistUtil.h"
#include "storage/transaction/TransactionManager.h"

namespace nebula {
namespace storage {

void ChainAddEdgesProcessorLocal::process(const cpp2::AddEdgesRequest& req) {
  if (!prepareRequest(req)) {
    finish();
    return;
  }
  env_->txnMan_->addChainTask(this);
}

/**
 * @brief
 * 1. check term
 * 2. set mem lock
 * 3. write edge prime(key = edge prime, val = )
 */
folly::SemiFuture<Code> ChainAddEdgesProcessorLocal::prepareLocal() {
  if (FLAGS_trace_toss) {
    uuid_ = ConsistUtil::strUUID();
    readableEdgeDesc_ = makeReadableEdge(req_);
    if (!readableEdgeDesc_.empty()) {
      uuid_.append(" ").append(readableEdgeDesc_);
    }
  }

  if (!lockEdges(req_)) {
    return Code::E_WRITE_WRITE_CONFLICT;
  }

  auto [pro, fut] = folly::makePromiseContract<Code>();
  auto primes = makePrime();
  std::vector<kvstore::KV> debugPrimes;
  if (FLAGS_trace_toss) {
    debugPrimes = primes;
  }

  erasePrime();
  env_->kvstore_->asyncMultiPut(
      spaceId_,
      localPartId_,
      std::move(primes),
      [p = std::move(pro), debugPrimes, this](auto rc) mutable {
        if (rc == nebula::cpp2::ErrorCode::SUCCEEDED) {
          primeInserted_ = true;
          if (FLAGS_trace_toss) {
            for (auto& kv : debugPrimes) {
              VLOG(1) << uuid_ << " put prime " << folly::hexlify(kv.first);
            }
          }
        } else {
          LOG(WARNING) << uuid_ << "kvstore err: " << apache::thrift::util::enumNameSafe(rc);
        }

        p.setValue(rc);
      });
  return std::move(fut);
}

folly::SemiFuture<Code> ChainAddEdgesProcessorLocal::processRemote(Code code) {
  VLOG(1) << uuid_ << " prepareLocal(), code = " << apache::thrift::util::enumNameSafe(code);
  if (code != Code::SUCCEEDED) {
    return code;
  }
  CHECK_EQ(req_.get_parts().size(), 1);
  auto reversedRequest = reverseRequest(req_);
  CHECK_EQ(reversedRequest.get_parts().size(), 1);
  auto [pro, fut] = folly::makePromiseContract<Code>();
  doRpc(std::move(pro), std::move(reversedRequest));
  return std::move(fut);
}

folly::SemiFuture<Code> ChainAddEdgesProcessorLocal::processLocal(Code code) {
  if (FLAGS_trace_toss) {
    VLOG(1) << uuid_ << " processRemote(), code = " << apache::thrift::util::enumNameSafe(code);
  }

  bool remoteFailed{true};

  if (code == Code::SUCCEEDED) {
    // do nothing
    remoteFailed = false;
  } else if (code == Code::E_RPC_FAILURE) {
    code_ = Code::SUCCEEDED;
    remoteFailed = false;
  } else {
    code_ = code;
  }

  if (!checkTerm(req_)) {
    LOG(WARNING) << "E_OUTDATED_TERM";
    code_ = Code::E_OUTDATED_TERM;
  }

  if (code == Code::E_RPC_FAILURE) {
    kvAppend_ = makeDoublePrime();
    addUnfinishedEdge(ResumeType::RESUME_REMOTE);
  }

  if (code_ == Code::SUCCEEDED) {
    return forwardToDelegateProcessor();
  } else {
    if (primeInserted_ && remoteFailed) {
      return abort();
    }
  }

  return code_;
}

void ChainAddEdgesProcessorLocal::addUnfinishedEdge(ResumeType type) {
  if (lk_ != nullptr) {
    lk_->forceUnlock();
  }
  auto keys = sEdgeKey(req_);
  for (auto& key : keys) {
    env_->txnMan_->addPrime(spaceId_, key, type);
  }
}

bool ChainAddEdgesProcessorLocal::prepareRequest(const cpp2::AddEdgesRequest& req) {
  CHECK_EQ(req.get_parts().size(), 1);
  req_ = req;
  spaceId_ = req_.get_space_id();
  auto vidType = env_->metaClient_->getSpaceVidType(spaceId_);
  if (!vidType.ok()) {
    LOG(WARNING) << "can't get vidType";
    return false;
  } else {
    spaceVidType_ = vidType.value();
  }
  localPartId_ = req.get_parts().begin()->first;
  replaceNullWithDefaultValue(req_);
  auto part = env_->kvstore_->part(spaceId_, localPartId_);
  if (!nebula::ok(part)) {
    pushResultCode(nebula::error(part), localPartId_);
    return false;
  }
  restrictTerm_ = (nebula::value(part))->termId();

  auto vidLen = env_->schemaMan_->getSpaceVidLen(spaceId_);
  if (!vidLen.ok()) {
    LOG(ERROR) << "getSpaceVidLen failed, spaceId_: " << spaceId_
               << ", status: " << vidLen.status();
    setErrorCode(Code::E_INVALID_SPACEVIDLEN);
    return false;
  }
  spaceVidLen_ = vidLen.value();
  return true;
}

folly::SemiFuture<Code> ChainAddEdgesProcessorLocal::forwardToDelegateProcessor() {
  auto* proc = AddEdgesProcessor::instance(env_, nullptr);
  proc->consistOp_ = [&](kvstore::BatchHolder& a, std::vector<kvstore::KV>* b) {
    callbackOfChainOp(a, b);
  };
  auto futProc = proc->getFuture();
  auto [pro, fut] = folly::makePromiseContract<Code>();
  std::move(futProc).thenValue([&, p = std::move(pro)](auto&& resp) mutable {
    auto rc = extractRpcError(resp);
    if (rc == Code::SUCCEEDED) {
      if (FLAGS_trace_toss) {
        for (auto& k : kvErased_) {
          VLOG(1) << uuid_ << " erase prime " << folly::hexlify(k);
        }
      }
    } else {
      VLOG(1) << uuid_
              << " forwardToDelegateProcessor(), code = " << apache::thrift::util::enumNameSafe(rc);
      addUnfinishedEdge(ResumeType::RESUME_CHAIN);
    }
    p.setValue(rc);
  });
  proc->process(req_);
  return std::move(fut);
}

Code ChainAddEdgesProcessorLocal::extractRpcError(const cpp2::ExecResponse& resp) {
  Code ret = Code::SUCCEEDED;
  auto& respComn = resp.get_result();
  for (auto& part : respComn.get_failed_parts()) {
    ret = part.code;
  }
  return ret;
}

void ChainAddEdgesProcessorLocal::doRpc(folly::Promise<Code>&& promise,
                                        cpp2::AddEdgesRequest&& req,
                                        int retry) noexcept {
  if (retry > retryLimit_) {
    promise.setValue(Code::E_LEADER_CHANGED);
    return;
  }
  auto* iClient = env_->txnMan_->getInternalClient();
  folly::Promise<Code> p;
  auto f = p.getFuture();
  iClient->chainAddEdges(req, restrictTerm_, edgeVer_, std::move(p));

  std::move(f).thenTry([=, p = std::move(promise)](auto&& t) mutable {
    auto code = t.hasValue() ? t.value() : Code::E_RPC_FAILURE;
    switch (code) {
      case Code::E_LEADER_CHANGED:
        doRpc(std::move(p), std::move(req), ++retry);
        break;
      default:
        p.setValue(code);
        break;
    }
    return code;
  });
}

void ChainAddEdgesProcessorLocal::callbackOfChainOp(kvstore::BatchHolder& batch,
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

folly::SemiFuture<Code> ChainAddEdgesProcessorLocal::abort() {
  if (kvErased_.empty()) {
    return Code::SUCCEEDED;
  }

  std::vector<std::string> debugErased;
  if (FLAGS_trace_toss) {
    debugErased = kvErased_;
  }

  auto [pro, fut] = folly::makePromiseContract<Code>();
  env_->kvstore_->asyncMultiRemove(
      req_.get_space_id(),
      localPartId_,
      std::move(kvErased_),
      [p = std::move(pro), debugErased, this](auto rc) mutable {
        VLOG(1) << uuid_ << " abort()=" << apache::thrift::util::enumNameSafe(rc);
        if (rc == Code::SUCCEEDED) {
          if (FLAGS_trace_toss) {
            for (auto& k : debugErased) {
              VLOG(1) << uuid_ << "erase prime " << folly::hexlify(k);
            }
          }
        } else {
          addUnfinishedEdge(ResumeType::RESUME_CHAIN);
        }
        p.setValue(rc);
      });
  return std::move(fut);
}

std::vector<kvstore::KV> ChainAddEdgesProcessorLocal::makePrime() {
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

std::vector<kvstore::KV> ChainAddEdgesProcessorLocal::makeDoublePrime() {
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

void ChainAddEdgesProcessorLocal::erasePrime() {
  auto fn = [&](const cpp2::NewEdge& edge) {
    auto key = ConsistUtil::primeKey(spaceVidLen_, localPartId_, edge.get_key());
    return key;
  };
  for (auto& edge : req_.get_parts().begin()->second) {
    kvErased_.push_back(fn(edge));
  }
}

void ChainAddEdgesProcessorLocal::eraseDoublePrime() {
  auto fn = [&](const cpp2::NewEdge& edge) {
    auto key = ConsistUtil::doublePrime(spaceVidLen_, localPartId_, edge.get_key());
    return key;
  };
  for (auto& edge : req_.get_parts().begin()->second) {
    kvErased_.push_back(fn(edge));
  }
}

bool ChainAddEdgesProcessorLocal::lockEdges(const cpp2::AddEdgesRequest& req) {
  auto partId = req.get_parts().begin()->first;
  auto* lockCore = env_->txnMan_->getLockCore(req.get_space_id(), partId);
  if (!lockCore) {
    return false;
  }

  std::vector<std::string> keys;
  for (auto& edge : req.get_parts().begin()->second) {
    keys.emplace_back(ConsistUtil::edgeKey(spaceVidLen_, partId, edge.get_key()));
  }
  lk_ = std::make_unique<TransactionManager::LockGuard>(lockCore, keys);
  return lk_->isLocked();
}

// we need to check term at both remote phase and local commit
bool ChainAddEdgesProcessorLocal::checkTerm(const cpp2::AddEdgesRequest& req) {
  auto space = req.get_space_id();
  auto partId = req.get_parts().begin()->first;

  auto part = env_->kvstore_->part(space, partId);
  if (!nebula::ok(part)) {
    pushResultCode(nebula::error(part), localPartId_);
    return false;
  }
  auto curTerm = (nebula::value(part))->termId();
  if (restrictTerm_ != curTerm) {
    VLOG(1) << folly::sformat(
        "check term failed, restrictTerm_={}, currTerm={}", restrictTerm_, curTerm);
    return false;
  }
  return true;
}

// check if current edge is not newer than the one trying to resume.
// this function only take effect in resume mode
bool ChainAddEdgesProcessorLocal::checkVersion(const cpp2::AddEdgesRequest& req) {
  auto part = req.get_parts().begin()->first;
  auto sKeys = sEdgeKey(req);
  auto currVer = ConsistUtil::getMultiEdgeVers(env_->kvstore_, spaceId_, part, sKeys);
  for (auto i = 0U; i != currVer.size(); ++i) {
    if (currVer[i] < resumedEdgeVer_) {
      return false;
    }
  }
  return true;
}

std::vector<std::string> ChainAddEdgesProcessorLocal::sEdgeKey(const cpp2::AddEdgesRequest& req) {
  std::vector<std::string> ret;
  for (auto& edgesOfPart : req.get_parts()) {
    auto partId = edgesOfPart.first;
    for (auto& edge : edgesOfPart.second) {
      ret.emplace_back(ConsistUtil::edgeKey(spaceVidLen_, partId, edge.get_key()));
    }
  }
  return ret;
}

cpp2::AddEdgesRequest ChainAddEdgesProcessorLocal::reverseRequest(
    const cpp2::AddEdgesRequest& req) {
  cpp2::AddEdgesRequest reversedRequest;
  for (auto& edgesOfPart : *req.parts_ref()) {
    for (auto& newEdge : edgesOfPart.second) {
      (*reversedRequest.parts_ref())[remotePartId_].emplace_back(newEdge);
      auto& newEdgeRef = (*reversedRequest.parts_ref())[remotePartId_].back();
      ConsistUtil::reverseEdgeKeyInplace(*newEdgeRef.key_ref());
    }
  }
  reversedRequest.set_space_id(req.get_space_id());
  reversedRequest.set_prop_names(req.get_prop_names());
  reversedRequest.set_if_not_exists(req.get_if_not_exists());
  return reversedRequest;
}

void ChainAddEdgesProcessorLocal::finish() {
  VLOG(1) << uuid_ << " commitLocal(), code_ = " << apache::thrift::util::enumNameSafe(code_);
  pushResultCode(code_, localPartId_);
  finished_.setValue(code_);
  onFinished();
}

cpp2::AddEdgesRequest ChainAddEdgesProcessorLocal::makeSingleEdgeRequest(
    PartitionID partId, const cpp2::NewEdge& edge) {
  cpp2::AddEdgesRequest req;
  req.set_space_id(req_.get_space_id());
  req.set_prop_names(req_.get_prop_names());
  req.set_if_not_exists(req_.get_if_not_exists());

  std::unordered_map<PartitionID, std::vector<cpp2::NewEdge>> newParts;
  newParts[partId].emplace_back(edge);

  req.set_parts(newParts);
  return req;
}

int64_t ChainAddEdgesProcessorLocal::toInt(const ::nebula::Value& val) {
  if (spaceVidType_ == meta::cpp2::PropertyType::FIXED_STRING) {
    auto str = val.toString();
    if (str.size() < 3) {
      return 0;
    }
    auto str2 = str.substr(1, str.size() - 2);
    return atoll(str2.c_str());
  } else if (spaceVidType_ == meta::cpp2::PropertyType::INT64) {
    return *reinterpret_cast<int64_t*>(const_cast<char*>(val.toString().c_str() + 1));
  }
  return 0;
}

std::string ChainAddEdgesProcessorLocal::makeReadableEdge(const cpp2::AddEdgesRequest& req) {
  if (req.get_parts().size() != 1) {
    LOG(INFO) << req.get_parts().size();
    return "";
  }
  if (req.get_parts().begin()->second.size() != 1) {
    LOG(INFO) << req.get_parts().begin()->second.size();
    return "";
  }
  auto& edge = req.get_parts().begin()->second.back();

  int64_t isrc = toInt(edge.get_key().get_src());
  int64_t idst = toInt(edge.get_key().get_dst());

  std::stringstream oss;
  oss << isrc << "->" << idst << ", val: ";
  for (auto& val : edge.get_props()) {
    oss << val.toString() << " ";
  }
  return oss.str();
}

/*** consider the following case:
 *
 * create edge known(kdate datetime default datetime(), degree int);
 * insert edge known(degree) VALUES "100" -> "101":(95);
 *
 * storage will insert datetime() as default value on both
 * in/out edge, but they will calculate independent
 * which lead to inconsistance
 *
 * that's why we need to replace the inconsistance prone value
 * at the monment the request comes
 * */
void ChainAddEdgesProcessorLocal::replaceNullWithDefaultValue(cpp2::AddEdgesRequest& req) {
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
          auto expr = field->defaultValue()->clone();
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
