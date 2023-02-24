/* Copyright (c) 2018 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#include "storage/mutate/AddEdgesProcessor.h"

#include <algorithm>

#include "codec/RowWriterV2.h"
#include "common/memory/MemoryTracker.h"
#include "common/stats/StatsManager.h"
#include "common/utils/IndexKeyUtils.h"
#include "common/utils/NebulaKeyUtils.h"
#include "common/utils/OperationKeyUtils.h"
#include "storage/stats/StorageStats.h"

namespace nebula {
namespace storage {

ProcessorCounters kAddEdgesCounters;

void AddEdgesProcessor::process(const cpp2::AddEdgesRequest& req) {
  spaceId_ = req.get_space_id();
  ifNotExists_ = req.get_if_not_exists();
  const auto& partEdges = req.get_parts();

  CHECK_NOTNULL(env_->schemaMan_);
  auto ret = env_->schemaMan_->getSpaceVidLen(spaceId_);
  if (!ret.ok()) {
    LOG(ERROR) << ret.status();
    for (auto& part : partEdges) {
      pushResultCode(nebula::cpp2::ErrorCode::E_INVALID_SPACEVIDLEN, part.first);
    }
    onFinished();
    return;
  }

  spaceVidLen_ = ret.value();
  callingNum_ = partEdges.size();

  CHECK_NOTNULL(env_->indexMan_);
  auto iRet = env_->indexMan_->getEdgeIndexes(spaceId_);
  if (!iRet.ok()) {
    LOG(ERROR) << iRet.status();
    for (auto& part : partEdges) {
      pushResultCode(nebula::cpp2::ErrorCode::E_SPACE_NOT_FOUND, part.first);
    }
    onFinished();
    return;
  }
  indexes_ = std::move(iRet).value();
  ignoreExistedIndex_ = req.get_ignore_existed_index();

  CHECK_NOTNULL(env_->kvstore_);

  if (indexes_.empty()) {
    doProcess(req);
  } else {
    doProcessWithIndex(req);
  }
}

void AddEdgesProcessor::doProcess(const cpp2::AddEdgesRequest& req) {
  const auto& partEdges = req.get_parts();
  const auto& propNames = req.get_prop_names();
  for (auto& part : partEdges) {
    auto partId = part.first;
    const auto& newEdges = part.second;

    std::vector<kvstore::KV> data;
    data.reserve(32);
    auto code = nebula::cpp2::ErrorCode::SUCCEEDED;
    std::unordered_set<std::string> visited;
    visited.reserve(newEdges.size());

    for (auto& newEdge : newEdges) {
      auto edgeKey = *newEdge.key_ref();
      VLOG(3) << "PartitionID: " << partId << ", VertexID: " << *edgeKey.src_ref()
              << ", EdgeType: " << *edgeKey.edge_type_ref()
              << ", EdgeRanking: " << *edgeKey.ranking_ref()
              << ", VertexID: " << *edgeKey.dst_ref();

      if (!NebulaKeyUtils::isValidVidLen(
              spaceVidLen_, edgeKey.src_ref()->getStr(), edgeKey.dst_ref()->getStr())) {
        LOG(ERROR) << "Space " << spaceId_ << " vertex length invalid, "
                   << "space vid len: " << spaceVidLen_ << ", edge srcVid: " << *edgeKey.src_ref()
                   << ", dstVid: " << *edgeKey.dst_ref();
        code = nebula::cpp2::ErrorCode::E_INVALID_VID;
        break;
      }

      auto key = NebulaKeyUtils::edgeKey(spaceVidLen_,
                                         partId,
                                         edgeKey.src_ref()->getStr(),
                                         *edgeKey.edge_type_ref(),
                                         *edgeKey.ranking_ref(),
                                         edgeKey.dst_ref()->getStr());
      if (ifNotExists_) {
        if (!visited.emplace(key).second) {
          continue;
        }
        auto obsIdx = findOldValue(partId, key);
        if (nebula::ok(obsIdx)) {
          // already exists in kvstore
          if (!nebula::value(obsIdx).empty()) {
            continue;
          }
        } else {
          code = nebula::error(obsIdx);
          break;
        }
      }
      auto schema = env_->schemaMan_->getEdgeSchema(spaceId_, std::abs(*edgeKey.edge_type_ref()));
      if (!schema) {
        LOG(ERROR) << "Space " << spaceId_ << ", Edge " << *edgeKey.edge_type_ref() << " invalid";
        code = nebula::cpp2::ErrorCode::E_EDGE_NOT_FOUND;
        break;
      }

      auto props = newEdge.get_props();
      WriteResult wRet;
      auto retEnc = encodeRowVal(schema.get(), propNames, props, wRet);
      if (!retEnc.ok()) {
        LOG(ERROR) << retEnc.status();
        code = writeResultTo(wRet, true);
        break;
      } else {
        data.emplace_back(std::move(key), std::move(retEnc.value()));
      }
    }
    if (code != nebula::cpp2::ErrorCode::SUCCEEDED) {
      handleAsync(spaceId_, partId, code);
    } else {
      if (consistOp_) {
        auto batchHolder = std::make_unique<kvstore::BatchHolder>();
        (*consistOp_)(*batchHolder, &data);
        auto batch = encodeBatchValue(std::move(batchHolder)->getBatch());

        env_->kvstore_->asyncAppendBatch(
            spaceId_, partId, std::move(batch), [partId, this](auto rc) {
              handleAsync(spaceId_, partId, rc);
            });
      } else {
        doPut(spaceId_, partId, std::move(data));
        stats::StatsManager::addValue(kNumEdgesInserted, data.size());
      }
    }
  }
}

void AddEdgesProcessor::doProcessWithIndex(const cpp2::AddEdgesRequest& req) {
  const auto& partEdges = req.get_parts();
  const auto& propNames = req.get_prop_names();
  for (const auto& part : partEdges) {
    auto partId = part.first;
    const auto& edges = part.second;
    // cache edgeKey
    std::vector<kvstore::KV> kvs;
    kvs.reserve(edges.size());
    auto code = deleteDupEdge(const_cast<std::vector<cpp2::NewEdge>&>(edges));
    if (code != nebula::cpp2::ErrorCode::SUCCEEDED) {
      handleAsync(spaceId_, partId, code);
      continue;
    }
    for (const auto& edge : edges) {
      auto edgeKey = *edge.key_ref();
      VLOG(3) << "PartitionID: " << partId << ", VertexID: " << *edgeKey.src_ref()
              << ", EdgeType: " << *edgeKey.edge_type_ref()
              << ", EdgeRanking: " << *edgeKey.ranking_ref()
              << ", VertexID: " << *edgeKey.dst_ref();

      auto schema = env_->schemaMan_->getEdgeSchema(spaceId_, std::abs(*edgeKey.edge_type_ref()));
      if (!schema) {
        LOG(ERROR) << "Space " << spaceId_ << ", Edge " << *edgeKey.edge_type_ref() << " invalid";
        code = nebula::cpp2::ErrorCode::E_EDGE_NOT_FOUND;
        break;
      }

      auto key = NebulaKeyUtils::edgeKey(spaceVidLen_,
                                         partId,
                                         edgeKey.src_ref()->getStr(),
                                         *edgeKey.edge_type_ref(),
                                         *edgeKey.ranking_ref(),
                                         (*edgeKey.dst_ref()).getStr());
      // collect values
      WriteResult writeResult;
      const auto& props = edge.get_props();
      auto encode = encodeRowVal(schema.get(), propNames, props, writeResult);
      if (!encode.ok()) {
        LOG(ERROR) << encode.status();
        code = writeResultTo(writeResult, true);
        break;
      }
      kvs.emplace_back(std::move(key), std::move(encode.value()));
    }

    if (code != nebula::cpp2::ErrorCode::SUCCEEDED) {
      handleAsync(spaceId_, partId, code);
    } else {
      stats::StatsManager::addValue(kNumEdgesInserted, kvs.size());
      auto atomicOp =
          [partId, data = std::move(kvs), this]() mutable -> kvstore::MergeableAtomicOpResult {
        return addEdgesWithIndex(partId, std::move(data));
      };
      auto cb = [partId, this](nebula::cpp2::ErrorCode ec) { handleAsync(spaceId_, partId, ec); };

      env_->kvstore_->asyncAtomicOp(spaceId_, partId, std::move(atomicOp), std::move(cb));
    }
  }
}

kvstore::MergeableAtomicOpResult AddEdgesProcessor::addEdgesWithIndex(
    PartitionID partId, std::vector<kvstore::KV>&& data) {
  kvstore::MergeableAtomicOpResult ret;
  ret.code = nebula::cpp2::ErrorCode::E_RAFT_ATOMIC_OP_FAILED;
  IndexCountWrapper wrapper(env_);
  std::unique_ptr<kvstore::BatchHolder> batchHolder = std::make_unique<kvstore::BatchHolder>();
  for (auto& [key, value] : data) {
    auto edgeType = NebulaKeyUtils::getEdgeType(spaceVidLen_, key);
    RowReaderWrapper oldReader;
    RowReaderWrapper newReader =
        RowReaderWrapper::getEdgePropReader(env_->schemaMan_, spaceId_, std::abs(edgeType), value);
    auto schema = env_->schemaMan_->getEdgeSchema(spaceId_, std::abs(edgeType));
    if (!schema) {
      return ret;
    }

    // only out-edge need to handle index
    if (edgeType > 0) {
      std::string oldVal;
      if (!ignoreExistedIndex_) {
        // read the old key value and initialize row reader if exists
        auto result = findOldValue(partId, key);
        if (nebula::ok(result)) {
          if (ifNotExists_ && !nebula::value(result).empty()) {
            continue;
          } else if (!nebula::value(result).empty()) {
            oldVal = std::move(nebula::value(result));
            oldReader =
                RowReaderWrapper::getEdgePropReader(env_->schemaMan_, spaceId_, edgeType, oldVal);
            ret.readSet.emplace_back(key);
          }
        } else {
          // read old value failed
          return ret;
        }
      }
      for (const auto& index : indexes_) {
        if (edgeType == index->get_schema_id().get_edge_type()) {
          // step 1, Delete old version index if exists.
          if (oldReader != nullptr) {
            auto oldIndexKeys = indexKeys(partId, oldReader.get(), key, index, nullptr);
            if (!oldIndexKeys.empty()) {
              ret.writeSet.insert(ret.writeSet.end(), oldIndexKeys.begin(), oldIndexKeys.end());
              // Check the index is building for the specified partition or
              // not.
              auto indexState = env_->getIndexState(spaceId_, partId);
              if (env_->checkRebuilding(indexState)) {
                auto delOpKey = OperationKeyUtils::deleteOperationKey(partId);
                for (auto& idxKey : oldIndexKeys) {
                  ret.writeSet.push_back(idxKey);
                  batchHolder->put(std::string(delOpKey), std::move(idxKey));
                }
              } else if (env_->checkIndexLocked(indexState)) {
                return ret;
              } else {
                for (auto& idxKey : oldIndexKeys) {
                  ret.writeSet.push_back(idxKey);
                  batchHolder->remove(std::move(idxKey));
                }
              }
            }
          }

          // step 2, Insert new edge index
          if (newReader != nullptr) {
            auto newIndexKeys = indexKeys(partId, newReader.get(), key, index, nullptr);
            if (!newIndexKeys.empty()) {
              // check if index has ttl field, write it to index value if exists
              auto field = CommonUtils::ttlValue(schema.get(), newReader.get());
              auto indexVal = field.ok() ? IndexKeyUtils::indexVal(std::move(field).value()) : "";
              auto indexState = env_->getIndexState(spaceId_, partId);
              if (env_->checkRebuilding(indexState)) {
                for (auto& idxKey : newIndexKeys) {
                  auto opKey = OperationKeyUtils::modifyOperationKey(partId, idxKey);
                  ret.writeSet.push_back(opKey);
                  batchHolder->put(std::move(opKey), std::string(indexVal));
                }
              } else if (env_->checkIndexLocked(indexState)) {
                // return folly::Optional<std::string>();
                return ret;
              } else {
                for (auto& idxKey : newIndexKeys) {
                  ret.writeSet.push_back(idxKey);
                  batchHolder->put(std::move(idxKey), std::string(indexVal));
                }
              }
            }
          }
        }
      }
    }
    // step 3, Insert new edge data
    ret.writeSet.push_back(key);
    // for why use a copy not move here:
    // previously, we use atomicOp(a kind of raft log, raft send this log in sync)
    //             this import an implicit constraint
    //             that all atomicOp will execute only once
    //             (because all atomicOp may fail or succeed, won't retry)
    // but in mergeable mode of atomic:
    //             an atomicOp may fail because of conflict
    //             then it will retry after the prev batch commit
    //             this mean now atomicOp may execute twice
    //             (won't be more than twice)
    //             but if we move the key out,
    //             then the second run will core.
    batchHolder->put(std::string(key), std::string(value));
  }

  if (consistOp_) {
    (*consistOp_)(*batchHolder, nullptr);
  }

  ret.code = nebula::cpp2::ErrorCode::SUCCEEDED;
  ret.batch = encodeBatchValue(batchHolder->getBatch());
  return ret;
}

ErrorOr<nebula::cpp2::ErrorCode, std::string> AddEdgesProcessor::findOldValue(
    PartitionID partId, const folly::StringPiece& rawKey) {
  auto key = NebulaKeyUtils::edgeKey(spaceVidLen_,
                                     partId,
                                     NebulaKeyUtils::getSrcId(spaceVidLen_, rawKey).str(),
                                     NebulaKeyUtils::getEdgeType(spaceVidLen_, rawKey),
                                     NebulaKeyUtils::getRank(spaceVidLen_, rawKey),
                                     NebulaKeyUtils::getDstId(spaceVidLen_, rawKey).str());
  std::string val;
  auto ret = env_->kvstore_->get(spaceId_, partId, key, &val);
  if (ret == nebula::cpp2::ErrorCode::SUCCEEDED) {
    return val;
  } else if (ret == nebula::cpp2::ErrorCode::E_KEY_NOT_FOUND) {
    return std::string();
  } else {
    LOG(ERROR) << "Error! ret = " << apache::thrift::util::enumNameSafe(ret) << ", spaceId "
               << spaceId_;
    return ret;
  }
}

std::vector<std::string> AddEdgesProcessor::indexKeys(
    PartitionID partId,
    RowReaderWrapper* reader,
    const folly::StringPiece& rawKey,
    std::shared_ptr<nebula::meta::cpp2::IndexItem> index,
    const meta::SchemaProviderIf* latestSchema) {
  auto values = IndexKeyUtils::collectIndexValues(reader, index.get(), latestSchema);
  if (!values.ok()) {
    return {};
  }
  return IndexKeyUtils::edgeIndexKeys(spaceVidLen_,
                                      partId,
                                      index->get_index_id(),
                                      NebulaKeyUtils::getSrcId(spaceVidLen_, rawKey).str(),
                                      NebulaKeyUtils::getRank(spaceVidLen_, rawKey),
                                      NebulaKeyUtils::getDstId(spaceVidLen_, rawKey).str(),
                                      std::move(values).value());
}

/*
 * Batch insert
 * ifNotExist_ is true. Only keep the first one when edgeKey is same
 * ifNotExist_ is false. Only keep the last one when edgeKey is same
 */
nebula::cpp2::ErrorCode AddEdgesProcessor::deleteDupEdge(std::vector<cpp2::NewEdge>& edges) {
  std::unordered_set<std::string> visited;
  visited.reserve(edges.size());
  if (ifNotExists_) {
    auto iter = edges.begin();
    while (iter != edges.end()) {
      auto edgeKeyRef = iter->key_ref();
      if (!NebulaKeyUtils::isValidVidLen(
              spaceVidLen_, edgeKeyRef->src_ref()->getStr(), edgeKeyRef->dst_ref()->getStr())) {
        LOG(ERROR) << "Space " << spaceId_ << " vertex length invalid, "
                   << "space vid len: " << spaceVidLen_
                   << ", edge srcVid: " << *edgeKeyRef->src_ref()
                   << ", dstVid: " << *edgeKeyRef->dst_ref() << ", ifNotExists_: " << std::boolalpha
                   << ifNotExists_;
        return nebula::cpp2::ErrorCode::E_INVALID_VID;
      }
      auto key = NebulaKeyUtils::edgeKey(spaceVidLen_,
                                         0,  // it's ok, just distinguish between different edgekey
                                         edgeKeyRef->src_ref()->getStr(),
                                         edgeKeyRef->get_edge_type(),
                                         edgeKeyRef->get_ranking(),
                                         edgeKeyRef->dst_ref()->getStr());
      if (!visited.emplace(key).second) {
        iter = edges.erase(iter);
      } else {
        ++iter;
      }
    }
  } else {
    auto iter = edges.rbegin();
    while (iter != edges.rend()) {
      auto edgeKeyRef = iter->key_ref();
      if (!NebulaKeyUtils::isValidVidLen(
              spaceVidLen_, edgeKeyRef->src_ref()->getStr(), edgeKeyRef->dst_ref()->getStr())) {
        LOG(ERROR) << "Space " << spaceId_ << " vertex length invalid, "
                   << "space vid len: " << spaceVidLen_
                   << ", edge srcVid: " << *edgeKeyRef->src_ref()
                   << ", dstVid: " << *edgeKeyRef->dst_ref() << ", ifNotExists_: " << std::boolalpha
                   << ifNotExists_;
        return nebula::cpp2::ErrorCode::E_INVALID_VID;
      }
      auto key = NebulaKeyUtils::edgeKey(spaceVidLen_,
                                         0,  // it's ok, just distinguish between different edgekey
                                         edgeKeyRef->src_ref()->getStr(),
                                         edgeKeyRef->get_edge_type(),
                                         edgeKeyRef->get_ranking(),
                                         edgeKeyRef->dst_ref()->getStr());
      if (!visited.emplace(key).second) {
        iter = decltype(iter)(edges.erase(std::next(iter).base()));
      } else {
        ++iter;
      }
    }
  }
  return nebula::cpp2::ErrorCode::SUCCEEDED;
}

}  // namespace storage
}  // namespace nebula
