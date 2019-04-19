/* Copyright (c) 2018 - present, VE Software Inc. All rights reserved
 *
 * This source code is licensed under Apache 2.0 License
 *  (found in the LICENSE.Apache file in the root directory)
 */
#include "storage/QueryBaseProcessor.h"
#include <algorithm>
#include "storage/KeyUtils.h"
#include "dataman/RowReader.h"
#include "dataman/RowWriter.h"

DECLARE_int32(max_handlers_per_req);
DECLARE_int32(min_vertices_num_mp);

namespace nebula {
namespace storage {

template<typename REQ, typename RESP>
bool QueryBaseProcessor<REQ, RESP>::validOperation(nebula::cpp2::SupportedType vType,
                                                   cpp2::StatType statType) {
    switch (statType) {
        case cpp2::StatType::SUM:
        case cpp2::StatType::AVG: {
            return vType == nebula::cpp2::SupportedType::INT
                    || vType == nebula::cpp2::SupportedType::VID
                    || vType == nebula::cpp2::SupportedType::TIMESTAMP
                    || vType == nebula::cpp2::SupportedType::FLOAT
                    || vType == nebula::cpp2::SupportedType::DOUBLE;
        }
        case cpp2::StatType::COUNT: {
             break;
        }
    }
    return true;
}


template<typename REQ, typename RESP>
void QueryBaseProcessor<REQ, RESP>::collectProps(RowReader* reader,
                                                 folly::StringPiece key,
                                                 const std::vector<PropContext>& props,
                                                 Collector* collector) {
    for (const auto& prop : props) {
        switch (prop.pikType_) {
            case PropContext::PropInKeyType::NONE:
                break;
            case PropContext::PropInKeyType::SRC:
                VLOG(3) << "collect _src, value = " << KeyUtils::getSrcId(key);
                collector->collectInt64(ResultType::SUCCEEDED, KeyUtils::getSrcId(key), prop);
                continue;
            case PropContext::PropInKeyType::DST:
                VLOG(3) << "collect _dst, value = " << KeyUtils::getDstId(key);
                collector->collectInt64(ResultType::SUCCEEDED, KeyUtils::getDstId(key), prop);
                continue;
            case PropContext::PropInKeyType::TYPE:
                VLOG(3) << "collect _type, value = " << KeyUtils::getEdgeType(key);
                collector->collectInt32(ResultType::SUCCEEDED, KeyUtils::getEdgeType(key), prop);
                continue;
            case PropContext::PropInKeyType::RANK:
                VLOG(3) << "collect _rank, value = " << KeyUtils::getRank(key);
                collector->collectInt64(ResultType::SUCCEEDED, KeyUtils::getRank(key), prop);
                continue;
        }
        if (reader != nullptr) {
            const auto& name = prop.prop_.get_name();
            switch (prop.type_.type) {
                case nebula::cpp2::SupportedType::INT: {
                    int64_t v;
                    auto ret = reader->getInt<int64_t>(name, v);
                    VLOG(3) << "collect " << name << ", value = " << v;
                    collector->collectInt64(ret, v, prop);
                    break;
                }
                case nebula::cpp2::SupportedType::VID: {
                    int64_t v;
                    auto ret = reader->getVid(name, v);
                    collector->collectInt64(ret, v, prop);
                    VLOG(3) << "collect " << name << ", value = " << v;
                    break;
                }
                case nebula::cpp2::SupportedType::TIMESTAMP: {
                    int64_t v;
                    auto ret = reader->getTimestamp(name, v);
                    collector->collectInt64(ret, v, prop);
                    VLOG(3) << "collect " << name << ", value = " << v;
                    break;
                }
                case nebula::cpp2::SupportedType::FLOAT: {
                    float v;
                    auto ret = reader->getFloat(name, v);
                    collector->collectFloat(ret, v, prop);
                    VLOG(3) << "collect " << name << ", value = " << v;
                    break;
                }
                case nebula::cpp2::SupportedType::DOUBLE: {
                    double v;
                    auto ret = reader->getDouble(name, v);
                    collector->collectDouble(ret, v, prop);
                    VLOG(3) << "collect " << name << ", value = " << v;
                    break;
                }
                case nebula::cpp2::SupportedType::STRING: {
                    folly::StringPiece v;
                    auto ret = reader->getString(name, v);
                    collector->collectString(ret, v, prop);
                    VLOG(3) << "collect " << name << ", value = " << v;
                    break;
                }
                default: {
                    VLOG(1) << "Unsupport stats!";
                    break;
                }
            }  // switch
        }  // if
    }  // for
}


template<typename REQ, typename RESP>
cpp2::ErrorCode QueryBaseProcessor<REQ, RESP>::checkAndBuildContexts(const REQ& req) {
    if (req.__isset.edge_type) {
        edgeContext_.edgeType_ = req.edge_type;
    }
    // Handle the case for query edges which should return some columns by default.
    int32_t index = edgeContext_.props_.size();
    std::unordered_map<TagID, int32_t> tagIndex;
    for (auto& col : req.get_return_columns()) {
        PropContext prop;
        switch (col.owner) {
            case cpp2::PropOwner::SOURCE:
            case cpp2::PropOwner::DEST: {
                auto tagId = col.tag_id;
                auto schema = this->schemaMan_->getTagSchema(spaceId_, tagId);
                if (!schema) {
                    VLOG(3) << "Can't find spaceId " << spaceId_ << ", tagId " << tagId;
                    return cpp2::ErrorCode::E_TAG_PROP_NOT_FOUND;
                }
                const auto& ftype = schema->getFieldType(col.name);
                prop.type_ = ftype;
                prop.retIndex_ = index++;
                if (col.__isset.stat && !validOperation(ftype.type, col.stat)) {
                    return cpp2::ErrorCode::E_IMPROPER_DATA_TYPE;
                }
                VLOG(3) << "tagId " << tagId << ", prop " << col.name;
                prop.prop_ = std::move(col);
                auto it = tagIndex.find(tagId);
                if (it == tagIndex.end()) {
                    TagContext tc;
                    tc.tagId_ = tagId;
                    tc.props_.emplace_back(std::move(prop));
                    tagContexts_.emplace_back(std::move(tc));
                    tagIndex.emplace(tagId, tagContexts_.size() - 1);
                } else {
                    tagContexts_[it->second].props_.emplace_back(std::move(prop));
                }
                break;
            }
            case cpp2::PropOwner::EDGE: {
                auto it = kPropsInKey_.find(col.name);
                if (it != kPropsInKey_.end()) {
                    prop.pikType_ = it->second;
                    prop.type_.type = nebula::cpp2::SupportedType::INT;
                } else if (type_ == BoundType::OUT_BOUND) {
                    // Only outBound have properties on edge.
                    auto schema = this->schemaMan_->getEdgeSchema(spaceId_,
                                                                  edgeContext_.edgeType_);
                    if (!schema) {
                        return cpp2::ErrorCode::E_EDGE_PROP_NOT_FOUND;
                    }
                    const auto& ftype = schema->getFieldType(col.name);
                    prop.type_ = ftype;
                } else {
                    VLOG(3) << "InBound has none props, skip it!";
                    break;
                }
                if (col.__isset.stat && !validOperation(prop.type_.type, col.stat)) {
                    return cpp2::ErrorCode::E_IMPROPER_DATA_TYPE;
                }
                prop.retIndex_ = index++;
                prop.prop_ = std::move(col);
                edgeContext_.props_.emplace_back(std::move(prop));
                break;
            }
        }
    }
    return cpp2::ErrorCode::SUCCEEDED;
}

template<typename REQ, typename RESP>
kvstore::ResultCode QueryBaseProcessor<REQ, RESP>::collectVertexProps(
                            PartitionID partId,
                            VertexID vId,
                            TagID tagId,
                            const std::vector<PropContext>& props,
                            Collector* collector) {
    auto prefix = KeyUtils::prefix(partId, vId, tagId);
    std::unique_ptr<kvstore::KVIterator> iter;
    auto ret = this->kvstore_->prefix(spaceId_, partId, prefix, &iter);
    if (ret != kvstore::ResultCode::SUCCEEDED) {
        VLOG(3) << "Error! ret = " << static_cast<int32_t>(ret) << ", spaceId " << spaceId_;
        return ret;
    }
    // Will decode the properties according to the schema version
    // stored along with the properties
    if (iter && iter->valid()) {
        auto reader = RowReader::getTagPropReader(this->schemaMan_, iter->val(), spaceId_, tagId);
        this->collectProps(reader.get(), iter->key(), props, collector);
    } else {
        VLOG(3) << "Missed partId " << partId << ", vId " << vId << ", tagId " << tagId;
    }
    return ret;
}

template<typename REQ, typename RESP>
kvstore::ResultCode QueryBaseProcessor<REQ, RESP>::collectEdgeProps(
                                               PartitionID partId,
                                               VertexID vId,
                                               EdgeType edgeType,
                                               const std::vector<PropContext>& props,
                                               EdgeProcessor proc) {
    auto prefix = KeyUtils::prefix(partId, vId, edgeType);
    std::unique_ptr<kvstore::KVIterator> iter;
    auto ret = this->kvstore_->prefix(spaceId_, partId, prefix, &iter);
    if (ret != kvstore::ResultCode::SUCCEEDED || !iter) {
        return ret;
    }
    EdgeRanking lastRank  = -1;
    VertexID    lastDstId = 0;
    bool        firstLoop = true;
    for (; iter->valid(); iter->next()) {
        auto key = iter->key();
        auto val = iter->val();
        auto rank = KeyUtils::getRank(key);
        auto dstId = KeyUtils::getDstId(key);
        if (!firstLoop && rank == lastRank && lastDstId == dstId) {
            VLOG(3) << "Only get the latest version for each edge.";
            continue;
        }
        lastRank = rank;
        lastDstId = dstId;
        std::unique_ptr<RowReader> reader;
        if (type_ == BoundType::OUT_BOUND && !val.empty()) {
            reader = RowReader::getEdgePropReader(this->schemaMan_, val, spaceId_, edgeType);
        }
        proc(reader.get(), key, props);
        if (firstLoop) {
            firstLoop = false;
        }
    }
    return ret;
}

template<typename REQ, typename RESP>
folly::Future<std::vector<OneVertexResp>>
QueryBaseProcessor<REQ, RESP>::asyncProcessBucket(Bucket bucket) {
    folly::Promise<std::vector<OneVertexResp>> pro;
    auto f = pro.getFuture();
    executor_->add([this, p = std::move(pro), b = std::move(bucket)] () mutable {
        std::vector<OneVertexResp> codes;
        codes.reserve(b.vertices_.size());
        for (auto& pv : b.vertices_) {
            codes.emplace_back(pv.first, pv.second, processVertex(pv.first, pv.second));
        }
        p.setValue(std::move(codes));
    });
    return f;
}

template<typename REQ, typename RESP>
int32_t QueryBaseProcessor<REQ, RESP>::getBucketsNum(int32_t verticesNum, int handlerNum) {
    if (verticesNum < FLAGS_min_vertices_num_mp) {
        return 1;
    }
    return std::min(verticesNum, handlerNum);
}

template<typename REQ, typename RESP>
std::vector<Bucket> QueryBaseProcessor<REQ, RESP>::genBuckets(
                                                    const cpp2::GetNeighborsRequest& req) {
    std::vector<Bucket> buckets;
    int32_t verticesNum = 0;
    for (auto& pv : req.get_parts()) {
        verticesNum += pv.second.size();
    }
    auto bucketsNum = getBucketsNum(verticesNum, FLAGS_max_handlers_per_req);
    buckets.resize(bucketsNum);
    auto vNumPerBucket = verticesNum / bucketsNum;
    int32_t bucketIndex = -1;
    for (auto& pv : req.get_parts()) {
        for (auto& vId : pv.second) {
            if (bucketIndex == -1
                    || (bucketIndex < bucketsNum - 1
                            && (int64_t)buckets[bucketIndex].vertices_.size() >= vNumPerBucket)) {
                ++bucketIndex;
                buckets[bucketIndex].vertices_.reserve(vNumPerBucket << 1);
            }
            CHECK_LT(bucketIndex, bucketsNum);
            buckets[bucketIndex].vertices_.emplace_back(pv.first, vId);
        }
    }
    return buckets;
}

template<typename REQ, typename RESP>
void QueryBaseProcessor<REQ, RESP>::process(const cpp2::GetNeighborsRequest& req) {
    CHECK_NOTNULL(executor_);
    spaceId_ = req.get_space_id();
    int32_t returnColumnsNum = req.get_return_columns().size();
    VLOG(3) << "Receive request, spaceId " << spaceId_ << ", return cols " << returnColumnsNum;
    tagContexts_.reserve(returnColumnsNum);

    auto retCode = checkAndBuildContexts(req);
    if (retCode != cpp2::ErrorCode::SUCCEEDED) {
        for (auto& p : req.get_parts()) {
            this->pushResultCode(retCode, p.first);
        }
        this->onFinished();
        return;
    }

    // const auto& filter = req.get_filter();
    auto buckets = genBuckets(req);
    std::vector<folly::Future<std::vector<OneVertexResp>>> results;
    for (auto& bucket : buckets) {
        results.emplace_back(asyncProcessBucket(std::move(bucket)));
    }
    folly::collectAll(results).via(executor_).thenTry([
                     this,
                     returnColumnsNum] (auto&& t) mutable {
        CHECK(!t.hasException());
        std::unordered_set<PartitionID> failedParts;
        for (auto& bucketTry : t.value()) {
            CHECK(!bucketTry.hasException());
            for (auto& r : bucketTry.value()) {
                auto& partId = std::get<0>(r);
                auto& ret = std::get<2>(r);
                if (ret != kvstore::ResultCode::SUCCEEDED
                      && failedParts.find(partId) == failedParts.end()) {
                    failedParts.emplace(partId);
                    this->pushResultCode(this->to(ret), partId);
                }
            }
        }
        this->onProcessFinished(returnColumnsNum);
        this->onFinished();
    });
}

}  // namespace storage
}  // namespace nebula
