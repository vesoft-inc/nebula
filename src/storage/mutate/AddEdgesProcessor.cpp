/* Copyright (c) 2018 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#include "storage/mutate/AddEdgesProcessor.h"

#include <algorithm>

#include "codec/RowWriterV2.h"
#include "common/stats/StatsManager.h"
#include "common/time/WallClock.h"
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
  for (auto& part : partEdges) {
    IndexCountWrapper wrapper(env_);
    std::unique_ptr<kvstore::BatchHolder> batchHolder = std::make_unique<kvstore::BatchHolder>();
    auto partId = part.first;
    const auto& newEdges = part.second;
    std::vector<EMLI> dummyLock;
    dummyLock.reserve(newEdges.size());
    auto code = nebula::cpp2::ErrorCode::SUCCEEDED;

    deleteDupEdge(const_cast<std::vector<cpp2::NewEdge>&>(newEdges));
    for (auto& newEdge : newEdges) {
      auto edgeKey = *newEdge.key_ref();
      auto l = std::make_tuple(spaceId_,
                               partId,
                               edgeKey.src_ref()->getStr(),
                               *edgeKey.edge_type_ref(),
                               *edgeKey.ranking_ref(),
                               edgeKey.dst_ref()->getStr());
      if (std::find(dummyLock.begin(), dummyLock.end(), l) == dummyLock.end()) {
        if (!env_->edgesML_->try_lock(l)) {
          LOG(ERROR) << folly::sformat("edge locked : src {}, type {}, rank {}, dst {}",
                                       edgeKey.src_ref()->getStr(),
                                       *edgeKey.edge_type_ref(),
                                       *edgeKey.ranking_ref(),
                                       edgeKey.dst_ref()->getStr());
          code = nebula::cpp2::ErrorCode::E_DATA_CONFLICT_ERROR;
          break;
        }
        dummyLock.emplace_back(std::move(l));
      }
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
      }
      if (*edgeKey.edge_type_ref() > 0) {
        std::string oldVal;
        RowReaderWrapper nReader;
        RowReaderWrapper oReader;
        if (!ignoreExistedIndex_) {
          auto obsIdx = findOldValue(partId, key);
          if (nebula::ok(obsIdx)) {
            // already exists in kvstore
            if (ifNotExists_ && !nebula::value(obsIdx).empty()) {
              continue;
            }
            if (!nebula::value(obsIdx).empty()) {
              oldVal = std::move(value(obsIdx));
              oReader = RowReaderWrapper::getEdgePropReader(
                  env_->schemaMan_, spaceId_, *edgeKey.edge_type_ref(), oldVal);
            }
          } else {
            code = nebula::error(obsIdx);
            break;
          }
        }
        if (!retEnc.value().empty()) {
          nReader = RowReaderWrapper::getEdgePropReader(
              env_->schemaMan_, spaceId_, *edgeKey.edge_type_ref(), retEnc.value());
        }
        for (auto& index : indexes_) {
          if (*edgeKey.edge_type_ref() == index->get_schema_id().get_edge_type()) {
            /*
             * step 1 , Delete old version index if exists.
             */
            if (oReader != nullptr) {
              auto ois = indexKeys(partId, oReader.get(), key, index, schema.get());
              if (!ois.empty()) {
                // Check the index is building for the specified partition or not.
                auto indexState = env_->getIndexState(spaceId_, partId);
                if (env_->checkRebuilding(indexState)) {
                  auto delOpKey = OperationKeyUtils::deleteOperationKey(partId);
                  for (auto& oi : ois) {
                    batchHolder->put(std::string(delOpKey), std::move(oi));
                  }
                } else if (env_->checkIndexLocked(indexState)) {
                  LOG(ERROR) << "The index has been locked: " << index->get_index_name();
                  code = nebula::cpp2::ErrorCode::E_DATA_CONFLICT_ERROR;
                  break;
                } else {
                  for (auto& oi : ois) {
                    batchHolder->remove(std::move(oi));
                  }
                }
              }
            }
            /*
             * step 2 , Insert new edge index
             */
            if (nReader != nullptr) {
              auto niks = indexKeys(partId, nReader.get(), key, index, schema.get());
              if (!niks.empty()) {
                auto v = CommonUtils::ttlValue(schema.get(), nReader.get());
                auto niv = v.ok() ? IndexKeyUtils::indexVal(std::move(v).value()) : "";
                // Check the index is building for the specified partition or not.
                auto indexState = env_->getIndexState(spaceId_, partId);
                if (env_->checkRebuilding(indexState)) {
                  for (auto& nik : niks) {
                    auto opKey = OperationKeyUtils::modifyOperationKey(partId, std::move(nik));
                    batchHolder->put(std::move(opKey), std::string(niv));
                  }
                } else if (env_->checkIndexLocked(indexState)) {
                  LOG(ERROR) << "The index has been locked: " << index->get_index_name();
                  code = nebula::cpp2::ErrorCode::E_DATA_CONFLICT_ERROR;
                  break;
                } else {
                  for (auto& nik : niks) {
                    batchHolder->put(std::move(nik), std::string(niv));
                  }
                }
              }
            }
          }
        }
      }
      if (code != nebula::cpp2::ErrorCode::SUCCEEDED) {
        break;
      }
      batchHolder->put(std::move(key), std::move(retEnc.value()));
      stats::StatsManager::addValue(kNumEdgesInserted);
    }
    if (code != nebula::cpp2::ErrorCode::SUCCEEDED) {
      env_->edgesML_->unlockBatch(dummyLock);
      handleAsync(spaceId_, partId, code);
      continue;
    }
    if (consistOp_) {
      (*consistOp_)(*batchHolder, nullptr);
    }
    auto batch = encodeBatchValue(batchHolder->getBatch());
    DCHECK(!batch.empty());
    nebula::MemoryLockGuard<EMLI> lg(env_->edgesML_.get(), std::move(dummyLock), false, false);
    env_->kvstore_->asyncAppendBatch(spaceId_,
                                     partId,
                                     std::move(batch),
                                     [l = std::move(lg), icw = std::move(wrapper), partId, this](
                                         nebula::cpp2::ErrorCode retCode) {
                                       UNUSED(l);
                                       UNUSED(icw);
                                       handleAsync(spaceId_, partId, retCode);
                                     });
  }
}

ErrorOr<nebula::cpp2::ErrorCode, std::string> AddEdgesProcessor::addEdges(
    PartitionID partId, const std::vector<kvstore::KV>& edges) {
  IndexCountWrapper wrapper(env_);
  std::unique_ptr<kvstore::BatchHolder> batchHolder = std::make_unique<kvstore::BatchHolder>();

  /*
   * Define the map newEdges to avoid inserting duplicate edge.
   * This map means :
   * map<edge_unique_key, prop_value> ,
   * -- edge_unique_key is only used as the unique key , for example:
   * insert below edges in the same request:
   *     kv(part1_src1_edgeType1_rank1_dst1 , v1)
   *     kv(part1_src1_edgeType1_rank1_dst1 , v2)
   *     kv(part1_src1_edgeType1_rank1_dst1 , v3)
   *     kv(part1_src1_edgeType1_rank1_dst1 , v4)
   *
   * Ultimately, kv(part1_src1_edgeType1_rank1_dst1 , v4) . It's just what I need.
   */
  std::unordered_map<std::string, std::string> newEdges;
  std::for_each(
      edges.begin(), edges.end(), [&newEdges](const auto& e) { newEdges[e.first] = e.second; });

  for (auto& e : newEdges) {
    std::string val;
    RowReaderWrapper oReader;
    RowReaderWrapper nReader;
    auto edgeType = NebulaKeyUtils::getEdgeType(spaceVidLen_, e.first);
    auto schema = env_->schemaMan_->getEdgeSchema(spaceId_, std::abs(edgeType));
    if (!schema) {
      LOG(ERROR) << "Space " << spaceId_ << ", Edge " << edgeType << " invalid";
      return nebula::cpp2::ErrorCode::E_EDGE_NOT_FOUND;
    }
    for (auto& index : indexes_) {
      if (edgeType == index->get_schema_id().get_edge_type()) {
        /*
         * step 1 , Delete old version index if exists.
         */
        if (!ignoreExistedIndex_ && val.empty()) {
          auto obsIdx = findOldValue(partId, e.first);
          if (!nebula::ok(obsIdx)) {
            return nebula::error(obsIdx);
          }
          val = std::move(nebula::value(obsIdx));
          if (!val.empty()) {
            oReader =
                RowReaderWrapper::getEdgePropReader(env_->schemaMan_, spaceId_, edgeType, val);
            if (oReader == nullptr) {
              LOG(ERROR) << "Bad format row";
              return nebula::cpp2::ErrorCode::E_INVALID_DATA;
            }
          }
        }

        if (!val.empty()) {
          auto ois = indexKeys(partId, oReader.get(), e.first, index, schema.get());
          if (!ois.empty()) {
            // Check the index is building for the specified partition or not.
            auto indexState = env_->getIndexState(spaceId_, partId);
            if (env_->checkRebuilding(indexState)) {
              auto deleteOpKey = OperationKeyUtils::deleteOperationKey(partId);
              for (auto& oi : ois) {
                batchHolder->put(std::string(deleteOpKey), std::move(oi));
              }
            } else if (env_->checkIndexLocked(indexState)) {
              LOG(ERROR) << "The index has been locked: " << index->get_index_name();
              return nebula::cpp2::ErrorCode::E_DATA_CONFLICT_ERROR;
            } else {
              for (auto& oi : ois) {
                batchHolder->remove(std::move(oi));
              }
            }
          }
        }

        /*
         * step 2 , Insert new edge index
         */
        if (nReader == nullptr) {
          nReader =
              RowReaderWrapper::getEdgePropReader(env_->schemaMan_, spaceId_, edgeType, e.second);
          if (nReader == nullptr) {
            LOG(ERROR) << "Bad format row";
            return nebula::cpp2::ErrorCode::E_INVALID_DATA;
          }
        }

        auto niks = indexKeys(partId, nReader.get(), e.first, index, schema.get());
        if (!niks.empty()) {
          auto v = CommonUtils::ttlValue(schema.get(), nReader.get());
          auto niv = v.ok() ? IndexKeyUtils::indexVal(std::move(v).value()) : "";
          // Check the index is building for the specified partition or not.
          auto indexState = env_->getIndexState(spaceId_, partId);
          if (env_->checkRebuilding(indexState)) {
            for (auto& nik : niks) {
              auto modifyOpKey = OperationKeyUtils::modifyOperationKey(partId, std::move(nik));
              batchHolder->put(std::move(modifyOpKey), std::string(niv));
            }
          } else if (env_->checkIndexLocked(indexState)) {
            LOG(ERROR) << "The index has been locked: " << index->get_index_name();
            return nebula::cpp2::ErrorCode::E_DATA_CONFLICT_ERROR;
          } else {
            for (auto& nik : niks) {
              batchHolder->put(std::move(nik), std::string(niv));
            }
          }
        }
      }
    }
    /*
     * step 3 , Insert new edge data
     */
    auto key = e.first;
    auto prop = e.second;
    batchHolder->put(std::move(key), std::move(prop));
  }
  return encodeBatchValue(batchHolder->getBatch());
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
    RowReader* reader,
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
void AddEdgesProcessor::deleteDupEdge(std::vector<cpp2::NewEdge>& edges) {
  std::unordered_set<std::string> visited;
  visited.reserve(edges.size());
  if (ifNotExists_) {
    auto iter = edges.begin();
    while (iter != edges.end()) {
      auto edgeKeyRef = iter->key_ref();
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
}

}  // namespace storage
}  // namespace nebula
