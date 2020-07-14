/* Copyright (c) 2018 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */
#include "storage/mutate/AddEdgesProcessor.h"
#include "utils/NebulaKeyUtils.h"
#include <algorithm>
#include <limits>
#include "time/WallClock.h"

namespace nebula {
namespace storage {

void AddEdgesProcessor::process(const cpp2::AddEdgesRequest& req) {
    spaceId_ = req.get_space_id();
    auto version = FLAGS_enable_multi_versions ?
        std::numeric_limits<int64_t>::max() - time::WallClock::fastNowInMicroSec() : 0L;
    // Switch version to big-endian, make sure the key is in ordered.
    version = folly::Endian::big(version);

    callingNum_ = req.parts.size();
    auto iRet = indexMan_->getEdgeIndexes(spaceId_);
    if (iRet.ok()) {
        indexes_ = std::move(iRet).value();
    }
    CHECK_NOTNULL(kvstore_);
    if (indexes_.empty()) {
        std::for_each(req.parts.begin(), req.parts.end(), [&](auto& partEdges) {
            auto partId = partEdges.first;
            std::vector<kvstore::KV> data;
            std::for_each(partEdges.second.begin(), partEdges.second.end(), [&](auto& edge) {
                VLOG(3) << "PartitionID: " << partId << ", VertexID: " << edge.key.src
                        << ", EdgeType: " << edge.key.edge_type << ", EdgeRanking: "
                        << edge.key.ranking << ", VertexID: "
                        << edge.key.dst << ", EdgeVersion: " << version;
                auto key = NebulaKeyUtils::edgeKey(partId, edge.key.src, edge.key.edge_type,
                                                   edge.key.ranking, edge.key.dst, version);
                data.emplace_back(std::move(key), std::move(edge.get_props()));
            });
            doPut(spaceId_, partId, std::move(data));
        });
    } else {
        std::for_each(req.parts.begin(), req.parts.end(), [&](auto& partEdges) {
            auto partId = partEdges.first;
            auto atomic = [version, partId, edges = std::move(partEdges.second), this]()
                          -> folly::Optional<std::string> {
                return addEdges(version, partId, edges);
            };
            auto callback = [partId, this](kvstore::ResultCode code) {
                handleAsync(spaceId_, partId, code);
            };
            this->kvstore_->asyncAtomicOp(spaceId_, partId, atomic, callback);
        });
    }
}

std::string AddEdgesProcessor::addEdges(int64_t version, PartitionID partId,
                                        const std::vector<cpp2::Edge>& edges) {
    std::unique_ptr<kvstore::BatchHolder> batchHolder = std::make_unique<kvstore::BatchHolder>();

    /*
     * Define the map newIndexes to avoid inserting duplicate edge.
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
    std::map<std::string, std::string> newEdges;
    std::for_each(edges.begin(), edges.end(), [&](auto& edge) {
        auto prop = edge.get_props();
        auto type = edge.key.edge_type;
        auto srcId = edge.key.src;
        auto rank = edge.key.ranking;
        auto dstId = edge.key.dst;
        VLOG(3) << "PartitionID: " << partId << ", VertexID: " << srcId
                << ", EdgeType: " << type << ", EdgeRanking: " << rank
                << ", VertexID: " << dstId << ", EdgeVersion: " << version;
        auto key = NebulaKeyUtils::edgeKey(partId, srcId, type, rank, dstId, version);
        newEdges[key] = std::move(prop);
    });
    for (auto& e : newEdges) {
        std::string val;
        std::unique_ptr<RowReader> nReader;
        auto edgeType = NebulaKeyUtils::getEdgeType(e.first);
        for (auto& index : indexes_) {
            if (edgeType == index->get_schema_id().get_edge_type()) {
                /*
                 * step 1 , Delete old version index if exists.
                 */
                if (val.empty()) {
                    val = findObsoleteIndex(partId, e.first);
                }
                if (!val.empty()) {
                    auto reader = RowReader::getEdgePropReader(this->schemaMan_,
                                                               val,
                                                               spaceId_,
                                                               edgeType);
                    if (reader == nullptr) {
                        LOG(WARNING) << "Bad format row";
                        return "";
                    }
                    auto oi = indexKey(partId, reader.get(), e.first, index);
                    if (!oi.empty()) {
                        batchHolder->remove(std::move(oi));
                    }
                }
                /*
                 * step 2 , Insert new edge index
                 */
                if (nReader == nullptr) {
                    nReader = RowReader::getEdgePropReader(this->schemaMan_,
                                                           e.second,
                                                           spaceId_,
                                                           edgeType);
                    if (nReader == nullptr) {
                        LOG(WARNING) << "Bad format row";
                        return "";
                    }
                }
                auto ni = indexKey(partId, nReader.get(), e.first, index);
                if (!ni.empty()) {
                    batchHolder->put(std::move(ni), "");
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

std::string AddEdgesProcessor::findObsoleteIndex(PartitionID partId,
                                                 const folly::StringPiece& rawKey) {
    auto prefix = NebulaKeyUtils::edgePrefix(partId,
                                             NebulaKeyUtils::getSrcId(rawKey),
                                             NebulaKeyUtils::getEdgeType(rawKey),
                                             NebulaKeyUtils::getRank(rawKey),
                                             NebulaKeyUtils::getDstId(rawKey));
    std::unique_ptr<kvstore::KVIterator> iter;
    auto ret = kvstore_->prefix(this->spaceId_, partId, prefix, &iter);
    if (ret != kvstore::ResultCode::SUCCEEDED) {
        LOG(ERROR) << "Error! ret = " << static_cast<int32_t>(ret)
                   << ", spaceId " << this->spaceId_;
        return "";
    }
    if (iter && iter->valid()) {
        return iter->val().str();
    }
    return "";
}

std::string AddEdgesProcessor::indexKey(PartitionID partId,
                                        RowReader* reader,
                                        const folly::StringPiece& rawKey,
                                        std::shared_ptr<nebula::cpp2::IndexItem> index) {
    auto values = collectIndexValues(reader, index->get_fields());
    if (!values.ok()) {
        return "";
    }
    return NebulaKeyUtils::edgeIndexKey(partId,
                                        index->get_index_id(),
                                        NebulaKeyUtils::getSrcId(rawKey),
                                        NebulaKeyUtils::getRank(rawKey),
                                        NebulaKeyUtils::getDstId(rawKey),
                                        values.value());
}

}  // namespace storage
}  // namespace nebula
