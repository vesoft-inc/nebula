/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef STORAGE_EXEC_EDGENODE_H_
#define STORAGE_EXEC_EDGENODE_H_

#include "common/base/Base.h"
#include "storage/exec/RelNode.h"

namespace nebula {
namespace storage {

// used to save stat value of each vertex
struct PropStat {
    PropStat() = default;

    explicit PropStat(const cpp2::StatType& statType) : statType_(statType) {}

    cpp2::StatType statType_;
    mutable Value sum_ = 0L;
    mutable int32_t count_ = 0;
};

using EdgeProcessor = std::function<kvstore::ResultCode(std::unique_ptr<RowReader>* reader,
                                                        folly::StringPiece key,
                                                        folly::StringPiece val)>;

class EdgeNode : public RelNode {
public:
    EdgeNode(EdgeContext* ctx,
             StorageEnv* env,
             GraphSpaceID spaceId,
             PartitionID partId,
             size_t vIdLen,
             const VertexID& vId,
             const Expression* exp,
             const FilterNode* filter,
             nebula::Row* row)
        : edgeContext_(ctx)
        , env_(env)
        , spaceId_(spaceId)
        , partId_(partId)
        , vIdLen_(vIdLen)
        , vId_(vId)
        , exp_(exp)
        , filter_(filter)
        , resultRow_(row) {}

protected:
    kvstore::ResultCode collectEdgeProps(EdgeType edgeType,
                                         RowReader* reader,
                                         folly::StringPiece key,
                                         const std::vector<PropContext>& props,
                                         nebula::DataSet& dataSet,
                                         std::vector<PropStat>* stats) {
        nebula::Row row;
        for (size_t i = 0; i < props.size(); i++) {
            auto& prop = props[i];
            VLOG(2) << "Collect prop " << prop.name_ << ", type " << edgeType;
            nebula::Value value;
            switch (prop.propInKeyType_) {
                // prop in value
                case PropContext::PropInKeyType::NONE: {
                    if (reader != nullptr) {
                        auto status = readValue(reader, prop);
                        if (!status.ok()) {
                            return kvstore::ResultCode::ERR_EDGE_PROP_NOT_FOUND;
                        }
                        value = std::move(status).value();
                    }
                    break;
                }
                case PropContext::PropInKeyType::SRC: {
                    value = NebulaKeyUtils::getSrcId(vIdLen_, key);
                    break;
                }
                case PropContext::PropInKeyType::DST: {
                    value = NebulaKeyUtils::getDstId(vIdLen_, key);
                    break;
                }
                case PropContext::PropInKeyType::TYPE: {
                    value = NebulaKeyUtils::getEdgeType(vIdLen_, key);
                    break;
                }
                case PropContext::PropInKeyType::RANK: {
                    value = NebulaKeyUtils::getRank(vIdLen_, key);
                    break;
                }
            }
            if (prop.hasStat_ && stats != nullptr) {
                addStatValue(value, (*stats)[prop.statIndex_]);
            }
            if (prop.returned_) {
                row.columns.emplace_back(std::move(value));
            }
        }
        dataSet.rows.emplace_back(std::move(row));
        return kvstore::ResultCode::SUCCEEDED;
    }

    std::vector<PropStat> initStatValue() {
        if (statCount_ <= 0) {
            return {};
        }

        // initialize all stat value of all edgeTypes
        std::vector<PropStat> stats;
        stats.resize(statCount_);
        for (const auto& ec : edgeContext_->propContexts_) {
            for (const auto& ctx : ec.second) {
                if (ctx.hasStat_) {
                    PropStat stat(ctx.statType_);
                    stats[ctx.statIndex_] = std::move(stat);
                }
            }
        }
        return stats;
    }

    void addStatValue(const Value& value, PropStat& stat) {
        if (value.type() == Value::Type::INT || value.type() == Value::Type::FLOAT) {
            stat.sum_ = stat.sum_ + value;
            ++stat.count_;
        }
    }

    nebula::Value calculateStat(const std::vector<PropStat>& stats) {
        if (statCount_ <= 0) {
            return NullType::__NULL__;
        }
        nebula::List result;
        result.values.reserve(statCount_);
        for (const auto& stat : stats) {
            if (stat.statType_ == cpp2::StatType::SUM) {
                result.values.emplace_back(stat.sum_);
            } else if (stat.statType_ == cpp2::StatType::COUNT) {
                result.values.emplace_back(stat.count_);
            } else if (stat.statType_ == cpp2::StatType::AVG) {
                if (stat.count_ > 0) {
                    result.values.emplace_back(stat.sum_ / stat.count_);
                } else {
                    result.values.emplace_back(NullType::NaN);
                }
            }
        }
        return result;
    }

    folly::Optional<std::pair<std::string, int64_t>> getEdgeTTLInfo(EdgeType edgeType) {
        folly::Optional<std::pair<std::string, int64_t>> ret;
        auto edgeFound = edgeContext_->ttlInfo_.find(std::abs(edgeType));
        if (edgeFound != edgeContext_->ttlInfo_.end()) {
            ret.emplace(edgeFound->second.first, edgeFound->second.second);
        }
        return ret;
    }

protected:
    EdgeContext* edgeContext_;
    StorageEnv* env_;
    GraphSpaceID spaceId_;
    PartitionID partId_;
    size_t vIdLen_;
    VertexID vId_;
    const Expression* exp_;
    const FilterNode* filter_;
    nebula::Row* resultRow_;

    // todo(doodle): stat
    size_t statCount_ = 0;
    static constexpr size_t kStatReturnIndex_ = 1;
};

class EdgeTypePrefixScanNode final : public EdgeNode {
    FRIEND_TEST(ScanEdgePropBench, ProcessEdgeProps);
    FRIEND_TEST(ScanEdgePropBench, ScanEdgesVsProcessEdgeProps);

public:
    EdgeTypePrefixScanNode(EdgeContext* ctx,
                           StorageEnv* env,
                           GraphSpaceID spaceId,
                           PartitionID partId,
                           size_t vIdLen,
                           const VertexID& vId,
                           const Expression* exp,
                           const FilterNode* filter,
                           nebula::Row* row)
        : EdgeNode(ctx, env, spaceId, partId, vIdLen, vId, exp, filter, row) {}

    folly::Future<kvstore::ResultCode> execute() override {
        auto stats = initStatValue();
        int64_t edgeRowCount = 0;
        for (const auto& ec : edgeContext_->propContexts_) {
            VLOG(1) << "partId " << partId_ << ", vId " << vId_ << ", edgeType " << ec.first
                    << ", prop size " << ec.second.size();
            if (edgeRowCount >= FLAGS_max_edge_returned_per_vertex) {
                // add a null field if return size exceeds, make sure the resp has correct columns
                resultRow_->columns.emplace_back(NullType::__NULL__);
                continue;
            }

            nebula::DataSet dataSet;
            auto edgeType = ec.first;
            auto& returnProps = ec.second;

            auto schemaIter = edgeContext_->schemas_.find(std::abs(edgeType));
            CHECK(schemaIter != edgeContext_->schemas_.end());
            CHECK(!schemaIter->second.empty());
            // all version schemas of a edgeType
            const auto& schemas = schemaIter->second;

            auto ttl = getEdgeTTLInfo(edgeType);
            auto ret = processEdgeProps(edgeType, edgeRowCount,
                    [this, edgeType, &ttl, &schemas, &returnProps, &dataSet, &stats]
                    (std::unique_ptr<RowReader>* reader,
                     folly::StringPiece key,
                     folly::StringPiece val)
                    -> kvstore::ResultCode {
                if (reader->get() == nullptr) {
                    *reader = RowReader::getRowReader(schemas, val);
                    if (!reader) {
                        return kvstore::ResultCode::ERR_EDGE_NOT_FOUND;
                    }
                } else if (!(*reader)->reset(schemas, val)) {
                    return kvstore::ResultCode::ERR_EDGE_NOT_FOUND;
                }

                const auto& latestSchema = schemas.back();
                if (ttl.has_value() &&
                    CommonUtils::checkDataExpiredForTTL(latestSchema.get(), reader->get(),
                                        ttl.value().first, ttl.value().second)) {
                    return kvstore::ResultCode::ERR_RESULT_EXPIRED;
                }

                // todo(doodle): filter
                if (exp_ != nullptr && !filter_->eval(exp_)) {
                    return kvstore::ResultCode::ERR_RESULT_FILTERED;
                }

                return collectEdgeProps(edgeType, reader->get(), key, returnProps, dataSet, &stats);
            });

            if (ret == kvstore::ResultCode::SUCCEEDED ||
                ret == kvstore::ResultCode::ERR_RESULT_OVERFLOW) {
                resultRow_->columns.emplace_back(std::move(dataSet));
                continue;
            } else if (ret == kvstore::ResultCode::ERR_KEY_NOT_FOUND) {
                // just add a null field if there is no corresponding edge
                resultRow_->columns.emplace_back(NullType::__NULL__);
                continue;
            } else if (ret != kvstore::ResultCode::SUCCEEDED) {
                return ret;
            }
        }

        auto statsValueList = calculateStat(stats);
        if (!statsValueList.empty()) {
            resultRow_->columns[kStatReturnIndex_] = std::move(statsValueList);
        }
        return kvstore::ResultCode::SUCCEEDED;
    }

private:
    kvstore::ResultCode processEdgeProps(EdgeType edgeType,
                                         int64_t& edgeRowCount,
                                         EdgeProcessor proc) {
        auto prefix = NebulaKeyUtils::edgePrefix(vIdLen_, partId_, vId_, edgeType);
        std::unique_ptr<kvstore::KVIterator> iter;
        auto ret = env_->kvstore_->prefix(spaceId_, partId_, prefix, &iter);
        if (ret != kvstore::ResultCode::SUCCEEDED) {
            return ret;
        }
        if (!iter || !iter->valid()) {
            return kvstore::ResultCode::ERR_KEY_NOT_FOUND;
        }

        EdgeRanking lastRank = 0;
        VertexID lastDstId = "";
        lastDstId.reserve(vIdLen_);
        bool firstLoop = true;
        int64_t count = edgeRowCount;

        auto reader = std::unique_ptr<RowReader>();
        for (; iter->valid(); iter->next()) {
            if (edgeRowCount >= FLAGS_max_edge_returned_per_vertex) {
                return kvstore::ResultCode::ERR_RESULT_OVERFLOW;
            }

            auto key = iter->key();
            auto val = iter->val();
            auto rank = NebulaKeyUtils::getRank(vIdLen_, key);
            auto dstId = NebulaKeyUtils::getDstId(vIdLen_, key);
            if (!firstLoop && rank == lastRank && lastDstId == dstId.str()) {
                VLOG(1) << "Only get the latest version for each edge.";
                continue;
            }
            lastRank = rank;
            lastDstId = dstId.str();

            ret = proc(&reader, key, val);
            if (ret == kvstore::ResultCode::ERR_RESULT_EXPIRED ||
                ret == kvstore::ResultCode::ERR_RESULT_FILTERED) {
                continue;
            } else if (ret != kvstore::ResultCode::SUCCEEDED) {
                return ret;
            }

            edgeRowCount++;
            if (firstLoop) {
                firstLoop = false;
            }
        }

        // return not found if none of the edges is valid, so we can add a null field to resp row
        return edgeRowCount == count ? kvstore::ResultCode::ERR_KEY_NOT_FOUND :
                                       kvstore::ResultCode::SUCCEEDED;
    }
};

class VertexPrefixScanNode final : public EdgeNode {
    FRIEND_TEST(ScanEdgePropBench, ScanEdgesVsProcessEdgeProps);

public:
    VertexPrefixScanNode(EdgeContext* ctx,
                         StorageEnv* env,
                         GraphSpaceID spaceId,
                         PartitionID partId,
                         size_t vIdLen,
                         const VertexID& vId,
                         const Expression* exp,
                         const FilterNode* filter,
                         nebula::Row* row)
        : EdgeNode(ctx, env, spaceId, partId, vIdLen, vId, exp, filter, row) {}

    folly::Future<kvstore::ResultCode> execute() override {
        auto prefix = NebulaKeyUtils::vertexPrefix(vIdLen_, partId_, vId_);
        std::unique_ptr<kvstore::KVIterator> iter;
        auto ret = env_->kvstore_->prefix(spaceId_, partId_, prefix, &iter);
        if (ret != kvstore::ResultCode::SUCCEEDED) {
            return ret;
        }
        if (!iter || !iter->valid()) {
            return kvstore::ResultCode::ERR_KEY_NOT_FOUND;
        }

        size_t edgePropsStartIdx = resultRow_->columns.size();
        resultRow_->columns.resize(edgePropsStartIdx + edgeContext_->propContexts_.size(),
                                   NullType::__NULL__);

        auto stats = initStatValue();
        EdgeType lastType = 0;
        EdgeRanking lastRank = 0;
        VertexID lastDstId = "";
        // all version schemas of a edgeType
        std::vector<std::shared_ptr<const meta::NebulaSchemaProvider>>* schemas = nullptr;
        // save the column num in response
        size_t lastEdgeTypeIdx;
        folly::Optional<std::pair<std::string, int64_t>> ttl;
        auto reader = std::unique_ptr<RowReader>();
        nebula::DataSet dataSet;
        int64_t edgeRowCount = 0;

        for (; iter->valid(); iter->next()) {
            if (edgeRowCount >= FLAGS_max_edge_returned_per_vertex) {
                break;
            }
            auto key = iter->key();
            if (!NebulaKeyUtils::isEdge(vIdLen_, key)) {
                continue;
            }

            auto edgeType = NebulaKeyUtils::getEdgeType(vIdLen_, key);
            auto rank = NebulaKeyUtils::getRank(vIdLen_, key);
            auto dstId = NebulaKeyUtils::getDstId(vIdLen_, key);
            if (edgeType == lastType && rank == lastRank && lastDstId == dstId.str()) {
                VLOG(1) << "Only get the latest version for each edge.";
                continue;
            }

            if (schemas == nullptr || edgeType != lastType) {
                // add dataSet of lastType to row
                if (lastType != 0) {
                    auto idx = lastEdgeTypeIdx + edgePropsStartIdx;
                    resultRow_->columns[idx] = std::move(dataSet);
                }

                auto idxIter = edgeContext_->indexMap_.find(edgeType);
                if (idxIter == edgeContext_->indexMap_.end()) {
                    // skip the edges should not return
                    continue;
                }

                auto schemaIter = edgeContext_->schemas_.find(std::abs(edgeType));
                if (schemaIter == edgeContext_->schemas_.end()) {
                    // skip the edges which is obsolete
                    continue;
                }
                lastType = edgeType;
                lastEdgeTypeIdx = idxIter->second;
                CHECK(!schemaIter->second.empty());
                schemas = &(schemaIter->second);
                ttl = getEdgeTTLInfo(edgeType);
            }

            lastRank = rank;
            lastDstId = dstId.str();
            auto val = iter->val();
            if (reader.get() == nullptr) {
                reader = RowReader::getRowReader(*schemas, val);
                if (reader.get() == nullptr) {
                    return kvstore::ResultCode::ERR_EDGE_NOT_FOUND;
                }
            } else if (!reader->reset(*schemas, val)) {
                return kvstore::ResultCode::ERR_EDGE_NOT_FOUND;
            }

            if (ttl.has_value() &&
                CommonUtils::checkDataExpiredForTTL(schemas->back().get(), reader.get(),
                                                    ttl.value().first, ttl.value().second)) {
                continue;
            }

            // todo(doodle): filter
            if (exp_ != nullptr && !filter_->eval(exp_)) {
                continue;
            }

            ret = collectEdgeProps(edgeType, reader.get(), key,
                                   edgeContext_->propContexts_[lastEdgeTypeIdx].second,
                                   dataSet, &stats);
            if (ret == kvstore::ResultCode::SUCCEEDED) {
                ++edgeRowCount;
            }
        }

        if (lastType != 0) {
            // set the result of last edgeType
            auto idx = lastEdgeTypeIdx + edgePropsStartIdx;
            resultRow_->columns[idx] = std::move(dataSet);
        }

        auto statsValueList = calculateStat(stats);
        if (!statsValueList.empty()) {
            resultRow_->columns[kStatReturnIndex_] = std::move(statsValueList);
        }
        return kvstore::ResultCode::SUCCEEDED;
    }
};

}  // namespace storage
}  // namespace nebula

#endif  // STORAGE_EXEC_EDGENODE_H_
