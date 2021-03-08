/* Copyright (c) 2018 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "common/time/WallClock.h"
#include "utils/NebulaKeyUtils.h"
#include "utils/IndexKeyUtils.h"
#include "utils/OperationKeyUtils.h"
#include <algorithm>
#include "codec/RowWriterV2.h"
#include "storage/mutate/AddEdgesProcessor.h"

namespace nebula {
namespace storage {

ProcessorCounters kAddEdgesCounters;

void AddEdgesProcessor::process(const cpp2::AddEdgesRequest& req) {
    spaceId_ = req.get_space_id();
    const auto& partEdges = req.get_parts();

    CHECK_NOTNULL(env_->schemaMan_);
    auto ret = env_->schemaMan_->getSpaceVidLen(spaceId_);
    if (!ret.ok()) {
        LOG(ERROR) << ret.status();
        for (auto& part : partEdges) {
            pushResultCode(cpp2::ErrorCode::E_INVALID_SPACEVIDLEN, part.first);
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
            pushResultCode(cpp2::ErrorCode::E_SPACE_NOT_FOUND, part.first);
        }
        onFinished();
        return;
    }
    indexes_ = std::move(iRet).value();

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
        cpp2::ErrorCode code = cpp2::ErrorCode::SUCCEEDED;
        for (auto& newEdge : newEdges) {
            auto edgeKey = newEdge.key;
            VLOG(3) << "PartitionID: " << partId << ", VertexID: " << edgeKey.src
                    << ", EdgeType: " << edgeKey.edge_type << ", EdgeRanking: "
                    << edgeKey.ranking << ", VertexID: "
                    << edgeKey.dst;

            if (!NebulaKeyUtils::isValidVidLen(
                    spaceVidLen_, edgeKey.src.getStr(), edgeKey.dst.getStr())) {
                LOG(ERROR) << "Space " << spaceId_ << " vertex length invalid, "
                           << "space vid len: " << spaceVidLen_ << ", edge srcVid: " << edgeKey.src
                           << ", dstVid: " << edgeKey.dst;
                code = cpp2::ErrorCode::E_INVALID_VID;
                break;
            }

            auto key = NebulaKeyUtils::edgeKey(spaceVidLen_,
                                               partId,
                                               edgeKey.src.getStr(),
                                               edgeKey.edge_type,
                                               edgeKey.ranking,
                                               edgeKey.dst.getStr());
            auto schema = env_->schemaMan_->getEdgeSchema(spaceId_,
                                                          std::abs(edgeKey.edge_type));
            if (!schema) {
                LOG(ERROR) << "Space " << spaceId_ << ", Edge "
                           << edgeKey.edge_type << " invalid";
                code = cpp2::ErrorCode::E_EDGE_NOT_FOUND;
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
        if (code != cpp2::ErrorCode::SUCCEEDED) {
            handleAsync(spaceId_, partId, code);
        } else {
            doPut(spaceId_, partId, std::move(data));
        }
    }
}

void AddEdgesProcessor::doProcessWithIndex(const cpp2::AddEdgesRequest& req) {
    const auto& partEdges = req.get_parts();
    const auto& propNames = req.get_prop_names();
    for (auto& part : partEdges) {
        IndexCountWrapper wrapper(env_);
        std::unique_ptr<kvstore::BatchHolder> batchHolder =
        std::make_unique<kvstore::BatchHolder>();
        auto partId = part.first;
        const auto& newEdges = part.second;
        std::vector<EMLI> dummyLock;
        dummyLock.reserve(newEdges.size());
        cpp2::ErrorCode code = cpp2::ErrorCode::SUCCEEDED;
        for (auto& newEdge : newEdges) {
            auto edgeKey = newEdge.key;
            VLOG(3) << "PartitionID: " << partId << ", VertexID: " << edgeKey.src
                    << ", EdgeType: " << edgeKey.edge_type << ", EdgeRanking: "
                    << edgeKey.ranking << ", VertexID: "
                    << edgeKey.dst;

            if (!NebulaKeyUtils::isValidVidLen(
                    spaceVidLen_, edgeKey.src.getStr(), edgeKey.dst.getStr())) {
                LOG(ERROR) << "Space " << spaceId_ << " vertex length invalid, "
                           << "space vid len: " << spaceVidLen_ << ", edge srcVid: " << edgeKey.src
                           << ", dstVid: " << edgeKey.dst;
                code = cpp2::ErrorCode::E_INVALID_VID;
                break;
            }

            auto key = NebulaKeyUtils::edgeKey(spaceVidLen_,
                                               partId,
                                               edgeKey.src.getStr(),
                                               edgeKey.edge_type,
                                               edgeKey.ranking,
                                               edgeKey.dst.getStr());
            auto schema = env_->schemaMan_->getEdgeSchema(spaceId_,
                                                          std::abs(edgeKey.edge_type));
            if (!schema) {
                LOG(ERROR) << "Space " << spaceId_ << ", Edge "
                           << edgeKey.edge_type << " invalid";
                code = cpp2::ErrorCode::E_EDGE_NOT_FOUND;
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
            if (edgeKey.edge_type > 0) {
                RowReaderWrapper nReader;
                RowReaderWrapper oReader;
                auto obsIdx = findOldValue(partId, key);
                if (nebula::ok(obsIdx)) {
                    if (!nebula::value(obsIdx).empty()) {
                        oReader = RowReaderWrapper::getEdgePropReader(env_->schemaMan_,
                                                                      spaceId_,
                                                                      edgeKey.edge_type,
                                                                      nebula::value(obsIdx));
                    }
                } else {
                    code = to(nebula::error(obsIdx));
                    break;
                }
                if (!retEnc.value().empty()) {
                    nReader = RowReaderWrapper::getEdgePropReader(env_->schemaMan_,
                                                                  spaceId_,
                                                                  edgeKey.edge_type,
                                                                  retEnc.value());
                }
                for (auto& index : indexes_) {
                    if (edgeKey.edge_type == index->get_schema_id().get_edge_type()) {
                        /*
                        * step 1 , Delete old version index if exists.
                        */
                        if (oReader != nullptr) {
                            auto oi = indexKey(partId, oReader.get(), key, index);
                            if (!oi.empty()) {
                                // Check the index is building for the specified partition or not.
                                auto indexState = env_->getIndexState(spaceId_, partId);
                                if (env_->checkRebuilding(indexState)) {
                                    auto delOpKey = OperationKeyUtils::deleteOperationKey(partId);
                                    batchHolder->put(std::move(delOpKey), std::move(oi));
                                } else if (env_->checkIndexLocked(indexState)) {
                                    LOG(ERROR) << "The index has been locked: "
                                               << index->get_index_name();
                                    code = cpp2::ErrorCode::E_DATA_CONFLICT_ERROR;
                                    break;
                                } else {
                                    batchHolder->remove(std::move(oi));
                                }
                            }
                        }
                        /*
                        * step 2 , Insert new edge index
                        */
                        if (nReader != nullptr) {
                            auto ni = indexKey(partId, nReader.get(), key, index);
                            if (!ni.empty()) {
                                // Check the index is building for the specified partition or not.
                                auto indexState = env_->getIndexState(spaceId_, partId);
                                if (env_->checkRebuilding(indexState)) {
                                    auto opKey = OperationKeyUtils::modifyOperationKey(
                                        partId, std::move(ni));
                                    batchHolder->put(std::move(opKey), "");
                                } else if (env_->checkIndexLocked(indexState)) {
                                    LOG(ERROR) << "The index has been locked: "
                                               << index->get_index_name();
                                    code = cpp2::ErrorCode::E_DATA_CONFLICT_ERROR;
                                    break;
                                } else {
                                    batchHolder->put(std::move(ni), "");
                                }
                            }
                        }
                    }
                }
            }
            if (code != cpp2::ErrorCode::SUCCEEDED) {
                break;
            }
            batchHolder->put(std::move(key), std::move(retEnc.value()));
            dummyLock.emplace_back(std::make_tuple(spaceId_,
                                                   partId,
                                                   edgeKey.src.getStr(),
                                                   edgeKey.edge_type,
                                                   edgeKey.ranking,
                                                   edgeKey.dst.getStr()));
        }
        if (code != cpp2::ErrorCode::SUCCEEDED) {
            handleAsync(spaceId_, partId, code);
            continue;
        }
        auto batch = encodeBatchValue(std::move(batchHolder)->getBatch());
        DCHECK(!batch.empty());
        nebula::MemoryLockGuard<EMLI> lg(env_->edgesML_.get(), std::move(dummyLock), true);
        if (!lg) {
            auto conflict = lg.conflictKey();
            LOG(ERROR) << "edge conflict "
                        << std::get<0>(conflict) << ":"
                        << std::get<1>(conflict) << ":"
                        << std::get<2>(conflict) << ":"
                        << std::get<3>(conflict) << ":"
                        << std::get<4>(conflict) << ":"
                        << std::get<5>(conflict);
            handleAsync(spaceId_, partId, cpp2::ErrorCode::E_DATA_CONFLICT_ERROR);
            continue;
        }
        env_->kvstore_->asyncAppendBatch(spaceId_, partId, std::move(batch),
            [l = std::move(lg), partId, this](kvstore::ResultCode kvRet) {
                UNUSED(l);
                handleAsync(spaceId_, partId, kvRet);
            });
    }
}

ErrorOr<kvstore::ResultCode, std::string>
AddEdgesProcessor::addEdges(PartitionID partId, const std::vector<kvstore::KV>& edges) {
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
    std::for_each(edges.begin(), edges.end(),
                  [&newEdges](const auto& e) {
                      newEdges[e.first] = e.second;
                  });

    for (auto& e : newEdges) {
        std::string val;
        RowReaderWrapper oReader;
        RowReaderWrapper nReader;
        auto edgeType = NebulaKeyUtils::getEdgeType(spaceVidLen_, e.first);
        for (auto& index : indexes_) {
            if (edgeType == index->get_schema_id().get_edge_type()) {
                /*
                 * step 1 , Delete old version index if exists.
                 */
                if (val.empty()) {
                    auto obsIdx = findOldValue(partId, e.first);
                    if (!nebula::ok(obsIdx)) {
                        return nebula::error(obsIdx);
                    }
                    val = std::move(nebula::value(obsIdx));
                    if (!val.empty()) {
                        oReader = RowReaderWrapper::getEdgePropReader(env_->schemaMan_,
                                                                      spaceId_,
                                                                      edgeType,
                                                                      val);
                        if (oReader == nullptr) {
                            LOG(ERROR) << "Bad format row";
                            return kvstore::ResultCode::ERR_INVALID_DATA;
                        }
                    }
                }

                if (!val.empty()) {
                    auto oi = indexKey(partId, oReader.get(), e.first, index);
                    if (!oi.empty()) {
                        // Check the index is building for the specified partition or not.
                        auto indexState = env_->getIndexState(spaceId_, partId);
                        if (env_->checkRebuilding(indexState)) {
                            auto deleteOpKey = OperationKeyUtils::deleteOperationKey(partId);
                            batchHolder->put(std::move(deleteOpKey), std::move(oi));
                        } else if (env_->checkIndexLocked(indexState)) {
                            LOG(ERROR) << "The index has been locked: " << index->get_index_name();
                            return kvstore::ResultCode::ERR_DATA_CONFLICT_ERROR;
                        } else {
                            batchHolder->remove(std::move(oi));
                        }
                    }
                }

                /*
                 * step 2 , Insert new edge index
                 */
                if (nReader == nullptr) {
                    nReader = RowReaderWrapper::getEdgePropReader(env_->schemaMan_,
                                                                  spaceId_,
                                                                  edgeType,
                                                                  e.second);
                    if (nReader == nullptr) {
                        LOG(ERROR) << "Bad format row";
                        return kvstore::ResultCode::ERR_INVALID_DATA;
                    }
                }

                auto ni = indexKey(partId, nReader.get(), e.first, index);
                if (!ni.empty()) {
                    // Check the index is building for the specified partition or not.
                    auto indexState = env_->getIndexState(spaceId_, partId);
                    if (env_->checkRebuilding(indexState)) {
                        auto modifyOpKey = OperationKeyUtils::modifyOperationKey(partId,
                                                                                 std::move(ni));
                        batchHolder->put(std::move(modifyOpKey), "");
                    } else if (env_->checkIndexLocked(indexState)) {
                        LOG(ERROR) << "The index has been locked: " << index->get_index_name();
                        return kvstore::ResultCode::ERR_DATA_CONFLICT_ERROR;
                    } else {
                        batchHolder->put(std::move(ni), "");
                    }
                }
            }
        }
        /*
         * step 3 , Insert new vertex data
         */
        auto key = e.first;
        auto prop = e.second;
        batchHolder->put(std::move(key), std::move(prop));
    }
    return encodeBatchValue(batchHolder->getBatch());
}

ErrorOr<kvstore::ResultCode, std::string>
AddEdgesProcessor::findOldValue(PartitionID partId, const folly::StringPiece& rawKey) {
    auto prefix = NebulaKeyUtils::edgePrefix(spaceVidLen_,
                                             partId,
                                             NebulaKeyUtils::getSrcId(spaceVidLen_, rawKey).str(),
                                             NebulaKeyUtils::getEdgeType(spaceVidLen_, rawKey),
                                             NebulaKeyUtils::getRank(spaceVidLen_, rawKey),
                                             NebulaKeyUtils::getDstId(spaceVidLen_, rawKey).str());
    std::unique_ptr<kvstore::KVIterator> iter;
    auto ret = env_->kvstore_->prefix(spaceId_, partId, prefix, &iter);
    if (ret != kvstore::ResultCode::SUCCEEDED) {
        LOG(ERROR) << "Error! ret = " << static_cast<int32_t>(ret)
                   << ", spaceId " << spaceId_;
        return ret;
    }
    if (iter && iter->valid()) {
        return iter->val().str();
    }
    return std::string();
}

std::string AddEdgesProcessor::indexKey(PartitionID partId,
                                        RowReader* reader,
                                        const folly::StringPiece& rawKey,
                                        std::shared_ptr<nebula::meta::cpp2::IndexItem> index) {
    auto values = IndexKeyUtils::collectIndexValues(reader, index->get_fields());
    if (!values.ok()) {
        return "";
    }
    return IndexKeyUtils::edgeIndexKey(spaceVidLen_, partId,
                                       index->get_index_id(),
                                       NebulaKeyUtils::getSrcId(spaceVidLen_, rawKey).str(),
                                       NebulaKeyUtils::getRank(spaceVidLen_, rawKey),
                                       NebulaKeyUtils::getDstId(spaceVidLen_, rawKey).str(),
                                       std::move(values).value());
}

}  // namespace storage
}  // namespace nebula
