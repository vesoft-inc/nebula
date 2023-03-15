/* Copyright (c) 2018 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#include "storage/mutate/AddVerticesProcessor.h"

#include <algorithm>

#include "codec/RowWriterV2.h"
#include "common/memory/MemoryTracker.h"
#include "common/stats/StatsManager.h"
#include "common/utils/IndexKeyUtils.h"
#include "common/utils/NebulaKeyUtils.h"
#include "common/utils/OperationKeyUtils.h"
#include "storage/StorageFlags.h"
#include "storage/stats/StorageStats.h"

namespace nebula {
namespace storage {

ProcessorCounters kAddVerticesCounters;

void AddVerticesProcessor::process(const cpp2::AddVerticesRequest& req) {
  spaceId_ = req.get_space_id();
  const auto& partVertices = req.get_parts();
  ifNotExists_ = req.get_if_not_exists();
  CHECK_NOTNULL(env_->schemaMan_);
  auto ret = env_->schemaMan_->getSpaceVidLen(spaceId_);
  if (!ret.ok()) {
    LOG(ERROR) << ret.status();
    for (auto& part : partVertices) {
      pushResultCode(nebula::cpp2::ErrorCode::E_INVALID_SPACEVIDLEN, part.first);
    }
    onFinished();
    return;
  }

  auto schema = env_->schemaMan_->getAllLatestVerTagSchema(spaceId_);
  if (!schema.ok()) {
    LOG(ERROR) << "Load schema failed";
    for (auto& part : partVertices) {
      pushResultCode(nebula::cpp2::ErrorCode::E_TAG_NOT_FOUND, part.first);
    }
    onFinished();
    return;
  }
  tagSchema_ = schema.value();

  spaceVidLen_ = ret.value();
  callingNum_ = partVertices.size();

  CHECK_NOTNULL(env_->indexMan_);
  auto iRet = env_->indexMan_->getTagIndexes(spaceId_);
  if (!iRet.ok()) {
    LOG(ERROR) << iRet.status();
    for (auto& part : partVertices) {
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

void AddVerticesProcessor::doProcess(const cpp2::AddVerticesRequest& req) {
  const auto& partVertices = req.get_parts();
  const auto& propNamesMap = req.get_prop_names();
  for (auto& part : partVertices) {
    auto partId = part.first;
    const auto& vertices = part.second;

    std::vector<kvstore::KV> data;
    data.reserve(32);
    auto code = nebula::cpp2::ErrorCode::SUCCEEDED;
    std::unordered_set<std::string> visited;
    visited.reserve(vertices.size());
    for (auto& vertex : vertices) {
      auto vid = vertex.get_id().getStr();
      const auto& newTags = vertex.get_tags();

      if (!NebulaKeyUtils::isValidVidLen(spaceVidLen_, vid)) {
        LOG(ERROR) << "Space " << spaceId_ << ", vertex length invalid, "
                   << " space vid len: " << spaceVidLen_ << ",  vid is " << vid;
        code = nebula::cpp2::ErrorCode::E_INVALID_VID;
        break;
      }
      if (FLAGS_use_vertex_key) {
        data.emplace_back(NebulaKeyUtils::vertexKey(spaceVidLen_, partId, vid), "");
      }
      for (auto& newTag : newTags) {
        auto tagId = newTag.get_tag_id();
        VLOG(3) << "PartitionID: " << partId << ", VertexID: " << vid << ", TagID: " << tagId;

        auto schemaIter = tagSchema_.find(tagId);
        if (schemaIter == tagSchema_.end()) {
          LOG(ERROR) << "Space " << spaceId_ << ", Tag " << tagId << " invalid";
          code = nebula::cpp2::ErrorCode::E_TAG_NOT_FOUND;
          break;
        }
        auto schema = schemaIter->second.get();

        auto key = NebulaKeyUtils::tagKey(spaceVidLen_, partId, vid, tagId);
        if (ifNotExists_) {
          if (!visited.emplace(key).second) {
            continue;
          }
          auto obsIdx = findOldValue(partId, vid, tagId);
          if (nebula::ok(obsIdx)) {
            if (!nebula::value(obsIdx).empty()) {
              continue;
            }
          } else {
            code = nebula::error(obsIdx);
            break;
          }
        }
        auto props = newTag.get_props();
        auto iter = propNamesMap.find(tagId);
        std::vector<std::string> propNames;
        if (iter != propNamesMap.end()) {
          propNames = iter->second;
        }

        WriteResult wRet;
        auto retEnc = encodeRowVal(schema, propNames, props, wRet);
        if (!retEnc.ok()) {
          LOG(ERROR) << retEnc.status();
          code = writeResultTo(wRet, false);
          break;
        }
        data.emplace_back(std::move(key), std::move(retEnc.value()));
      }
    }
    if (code != nebula::cpp2::ErrorCode::SUCCEEDED) {
      handleAsync(spaceId_, partId, code);
    } else {
      doPut(spaceId_, partId, std::move(data));
      stats::StatsManager::addValue(kNumVerticesInserted, data.size());
    }
  }
}

void AddVerticesProcessor::doProcessWithIndex(const cpp2::AddVerticesRequest& req) {
  const auto& partVertices = req.get_parts();
  const auto& propNamesMap = req.get_prop_names();

  for (const auto& part : partVertices) {
    auto partId = part.first;
    const auto& vertices = part.second;
    std::vector<kvstore::KV> tags;
    tags.reserve(vertices.size());

    std::vector<std::string> verticeData;
    verticeData.reserve(vertices.size());

    auto code = nebula::cpp2::ErrorCode::SUCCEEDED;
    deleteDupVid(const_cast<std::vector<cpp2::NewVertex>&>(vertices));
    for (const auto& vertex : vertices) {
      auto vid = vertex.get_id().getStr();
      const auto& newTags = vertex.get_tags();

      if (!NebulaKeyUtils::isValidVidLen(spaceVidLen_, vid)) {
        LOG(ERROR) << "Space " << spaceId_ << ", vertex length invalid, "
                   << " space vid len: " << spaceVidLen_ << ",  vid is " << vid;
        code = nebula::cpp2::ErrorCode::E_INVALID_VID;
        break;
      }

      if (FLAGS_use_vertex_key) {
        verticeData.emplace_back(NebulaKeyUtils::vertexKey(spaceVidLen_, partId, vid));
      }
      for (const auto& newTag : newTags) {
        auto tagId = newTag.get_tag_id();
        VLOG(3) << "PartitionID: " << partId << ", VertexID: " << vid << ", TagID: " << tagId;

        auto schemaIter = tagSchema_.find(tagId);
        if (schemaIter == tagSchema_.end()) {
          LOG(ERROR) << "Space " << spaceId_ << ", Tag " << tagId << " invalid";
          code = nebula::cpp2::ErrorCode::E_TAG_NOT_FOUND;
          break;
        }
        auto schema = schemaIter->second.get();

        auto key = NebulaKeyUtils::tagKey(spaceVidLen_, partId, vid, tagId);
        // collect values
        const auto& props = newTag.get_props();
        auto iter = propNamesMap.find(tagId);
        std::vector<std::string> propNames;
        if (iter != propNamesMap.end()) {
          propNames = iter->second;
        }

        WriteResult writeResult;
        auto encode = encodeRowVal(schema, propNames, props, writeResult);
        if (!encode.ok()) {
          LOG(ERROR) << encode.status();
          code = writeResultTo(writeResult, false);
          break;
        }
        tags.emplace_back(std::string(key), std::string(encode.value()));
      }
    }

    if (code != nebula::cpp2::ErrorCode::SUCCEEDED) {
      handleAsync(spaceId_, partId, code);
    } else {
      stats::StatsManager::addValue(kNumVerticesInserted, verticeData.size());
      auto atomicOp = [=, tags = std::move(tags), vertices = std::move(verticeData)]() mutable {
        return addVerticesWithIndex(partId, tags, vertices);
      };

      auto cb = [partId, this](nebula::cpp2::ErrorCode ec) { handleAsync(spaceId_, partId, ec); };
      env_->kvstore_->asyncAtomicOp(spaceId_, partId, std::move(atomicOp), std::move(cb));
    }
  }
}

kvstore::MergeableAtomicOpResult AddVerticesProcessor::addVerticesWithIndex(
    PartitionID partId,
    const std::vector<kvstore::KV>& data,
    const std::vector<std::string>& vertices) {
  kvstore::MergeableAtomicOpResult ret;
  ret.code = nebula::cpp2::ErrorCode::E_RAFT_ATOMIC_OP_FAILED;
  IndexCountWrapper wrapper(env_);
  auto batchHolder = std::make_unique<kvstore::BatchHolder>();
  for (auto& vertice : vertices) {
    batchHolder->put(std::string(vertice), "");
  }
  for (auto& [key, value] : data) {
    auto vId = NebulaKeyUtils::getVertexId(spaceVidLen_, key);
    auto tagId = NebulaKeyUtils::getTagId(spaceVidLen_, key);
    RowReaderWrapper oldReader;
    RowReaderWrapper newReader =
        RowReaderWrapper::getTagPropReader(env_->schemaMan_, spaceId_, tagId, value);
    auto schemaIter = tagSchema_.find(tagId);
    if (schemaIter == tagSchema_.end()) {
      ret.code = nebula::cpp2::ErrorCode::E_TAG_NOT_FOUND;
      DLOG(INFO) << "===>>> failed";
      return ret;
    }
    auto schema = schemaIter->second.get();
    std::string oldVal;
    if (!ignoreExistedIndex_) {
      // read the old key value and initialize row reader if exists
      auto result = findOldValue(partId, vId.str(), tagId);
      if (nebula::ok(result)) {
        if (ifNotExists_ && !nebula::value(result).empty()) {
          continue;
        } else if (!nebula::value(result).empty()) {
          oldVal = std::move(nebula::value(result));
          oldReader = RowReaderWrapper::getTagPropReader(env_->schemaMan_, spaceId_, tagId, oldVal);
          ret.readSet.emplace_back(key);
        }
      } else {
        // read old value failed
        DLOG(INFO) << "===>>> failed";
        return ret;
      }
    }
    for (const auto& index : indexes_) {
      if (tagId == index->get_schema_id().get_tag_id()) {
        // step 1, Delete old version index if exists.
        if (oldReader != nullptr) {
          auto oldIndexKeys = indexKeys(partId, vId.str(), oldReader.get(), index, schema);
          if (!oldIndexKeys.empty()) {
            // Check the index is building for the specified partition or
            // not.
            auto indexState = env_->getIndexState(spaceId_, partId);
            if (env_->checkRebuilding(indexState)) {
              auto delOpKey = OperationKeyUtils::deleteOperationKey(partId);
              for (auto& idxKey : oldIndexKeys) {
                ret.writeSet.emplace_back(std::string(delOpKey));
                batchHolder->put(std::string(delOpKey), std::move(idxKey));
              }
            } else if (env_->checkIndexLocked(indexState)) {
              LOG(ERROR) << "The index has been locked: " << index->get_index_name();
              ret.code = nebula::cpp2::ErrorCode::E_DATA_CONFLICT_ERROR;
              return ret;
            } else {
              for (auto& idxKey : oldIndexKeys) {
                ret.writeSet.emplace_back(std::string(idxKey));
                batchHolder->remove(std::move(idxKey));
              }
            }
          }
        }

        // step 2, Insert new vertex index
        if (newReader != nullptr) {
          auto newIndexKeys = indexKeys(partId, vId.str(), newReader.get(), index, schema);
          if (!newIndexKeys.empty()) {
            // check if index has ttl field, write it to index value if exists
            auto field = CommonUtils::ttlValue(schema, newReader.get());
            auto indexVal = field.ok() ? IndexKeyUtils::indexVal(std::move(field).value()) : "";
            auto indexState = env_->getIndexState(spaceId_, partId);
            if (env_->checkRebuilding(indexState)) {
              for (auto& idxKey : newIndexKeys) {
                auto opKey = OperationKeyUtils::modifyOperationKey(partId, idxKey);
                ret.writeSet.emplace_back(std::string(opKey));
                batchHolder->put(std::move(opKey), std::string(indexVal));
              }
            } else if (env_->checkIndexLocked(indexState)) {
              LOG(ERROR) << "The index has been locked: " << index->get_index_name();
              ret.code = nebula::cpp2::ErrorCode::E_DATA_CONFLICT_ERROR;
              return ret;
            } else {
              for (auto& idxKey : newIndexKeys) {
                ret.writeSet.emplace_back(std::string(idxKey));
                batchHolder->put(std::move(idxKey), std::string(indexVal));
              }
            }
          }
        }
      }
    }
    // step 3, Insert new vertex data
    ret.writeSet.emplace_back(key);
    batchHolder->put(std::string(key), std::string(value));
  }
  ret.batch = encodeBatchValue(batchHolder->getBatch());
  ret.code = nebula::cpp2::ErrorCode::SUCCEEDED;
  return ret;
}

ErrorOr<nebula::cpp2::ErrorCode, std::string> AddVerticesProcessor::findOldValue(
    PartitionID partId, const VertexID& vId, TagID tagId) {
  auto key = NebulaKeyUtils::tagKey(spaceVidLen_, partId, vId, tagId);
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

std::vector<std::string> AddVerticesProcessor::indexKeys(
    PartitionID partId,
    const VertexID& vId,
    RowReaderWrapper* reader,
    std::shared_ptr<nebula::meta::cpp2::IndexItem> index,
    const meta::SchemaProviderIf* latestSchema) {
  auto values = IndexKeyUtils::collectIndexValues(reader, index.get(), latestSchema);
  if (!values.ok()) {
    return {};
  }

  return IndexKeyUtils::vertexIndexKeys(
      spaceVidLen_, partId, index->get_index_id(), vId, std::move(values).value());
}

/*
 * Batch insert
 * ifNotExist_ is true. Only keep the first one when vid is same
 * ifNotExist_ is false. Only keep the last one when vid is same
 */
void AddVerticesProcessor::deleteDupVid(std::vector<cpp2::NewVertex>& vertices) {
  std::unordered_set<std::string> visited;
  visited.reserve(vertices.size());
  if (ifNotExists_) {
    auto iter = vertices.begin();
    while (iter != vertices.end()) {
      const auto& vid = iter->get_id().getStr();
      if (!visited.emplace(vid).second) {
        iter = vertices.erase(iter);
      } else {
        ++iter;
      }
    }
  } else {
    auto iter = vertices.rbegin();
    while (iter != vertices.rend()) {
      const auto& vid = iter->get_id().getStr();
      if (!visited.emplace(vid).second) {
        iter = decltype(iter)(vertices.erase(std::next(iter).base()));
      } else {
        ++iter;
      }
    }
  }
}

}  // namespace storage
}  // namespace nebula
