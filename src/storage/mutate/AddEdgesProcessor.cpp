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

void AddEdgesProcessor::process(const cpp2::AddEdgesRequest& req) {
    spaceId_ = req.get_space_id();
    const auto& partEdges = req.get_parts();
    const auto& propNames = req.get_prop_names();

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
    if (iRet.ok()) {
        indexes_ = std::move(iRet).value();
    }

    CHECK_NOTNULL(env_->kvstore_);
    for (auto& part : partEdges) {
        auto partId = part.first;
        const auto& newEdges = part.second;

        std::vector<kvstore::KV> data;
        data.reserve(32);
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
                pushResultCode(cpp2::ErrorCode::E_INVALID_VID, partId);
                onFinished();
                return;
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
                pushResultCode(cpp2::ErrorCode::E_EDGE_NOT_FOUND, partId);
                onFinished();
                return;
            }

            auto props = newEdge.get_props();
            WriteResult wRet;
            auto retEnc = encodeRowVal(schema.get(), propNames, props, wRet);
            if (!retEnc.ok()) {
                LOG(ERROR) << retEnc.status();
                pushResultCode(writeResultTo(wRet, true), partId);
                onFinished();
                return;
            }

            data.emplace_back(std::move(key), std::move(retEnc.value()));
        }
        if (indexes_.empty()) {
            doPut(spaceId_, partId, std::move(data));
        } else {
            auto atomic = [partId, edges = std::move(data), this]()
                          -> folly::Optional<std::string> {
                return addEdges(partId, edges);
            };

            auto callback = [partId, this](kvstore::ResultCode code) {
                handleAsync(spaceId_, partId, code);
            };
            env_->kvstore_->asyncAtomicOp(spaceId_, partId, atomic, callback);
        }
    }
}

folly::Optional<std::string>
AddEdgesProcessor::addEdges(PartitionID partId,
                            const std::vector<kvstore::KV>& edges) {
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
                    if (obsIdx == folly::none) {
                        return folly::none;
                    }
                    val = std::move(obsIdx).value();
                    if (!val.empty()) {
                        oReader = RowReaderWrapper::getEdgePropReader(env_->schemaMan_,
                                                                      spaceId_,
                                                                      edgeType,
                                                                      val);
                        if (oReader == nullptr) {
                            LOG(ERROR) << "Bad format row";
                            return folly::none;
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
                            return folly::none;
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
                        return folly::none;
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
                        return folly::none;
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

folly::Optional<std::string>
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
        return folly::none;
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
