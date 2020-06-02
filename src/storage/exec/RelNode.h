/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef STORAGE_EXEC_RELNODE_H_
#define STORAGE_EXEC_RELNODE_H_

#include "common/base/Base.h"
#include "utils/NebulaKeyUtils.h"
#include "storage/CommonUtils.h"
#include "storage/query/QueryBaseProcessor.h"
#include "storage/exec/FilterContext.h"

namespace nebula {
namespace storage {

using NullHandler = std::function<kvstore::ResultCode(const std::vector<PropContext>*)>;

using TagPropHandler = std::function<kvstore::ResultCode(TagID,
                                                         RowReader*,
                                                         const std::vector<PropContext>* props)>;

using EdgePropHandler = std::function<kvstore::ResultCode(EdgeType,
                                                          folly::StringPiece,
                                                          RowReader*,
                                                          const std::vector<PropContext>* props)>;

template<typename T> class StoragePlan;

// RelNode is shortcut for relational algebra node, each RelNode has an execute method,
// which will be invoked in dag when all its dependencies have finished
template<typename T>
class RelNode {
    friend class StoragePlan<T>;
public:
    virtual kvstore::ResultCode execute(PartitionID partId, const T& input) {
        DVLOG(1) << name_;
        for (auto* dependency : dependencies_) {
            auto ret = dependency->execute(partId, input);
            if (ret != kvstore::ResultCode::SUCCEEDED) {
                return ret;
            }
        }
        return kvstore::ResultCode::SUCCEEDED;
    }

    void addDependency(RelNode<T>* dep) {
        dependencies_.emplace_back(dep);
        dep->hasDependents_ = true;
    }

    RelNode() = default;

    virtual ~RelNode() = default;

    explicit RelNode(const std::string& name): name_(name) {}

protected:
    folly::Optional<std::pair<std::string, int64_t>>
    getEdgeTTLInfo(EdgeContext* edgeContext, EdgeType edgeType) {
        folly::Optional<std::pair<std::string, int64_t>> ret;
        auto edgeFound = edgeContext->ttlInfo_.find(std::abs(edgeType));
        if (edgeFound != edgeContext->ttlInfo_.end()) {
            ret.emplace(edgeFound->second.first, edgeFound->second.second);
        }
        return ret;
    }

    folly::Optional<std::pair<std::string, int64_t>>
    getTagTTLInfo(TagContext* tagContext, TagID tagId) {
        folly::Optional<std::pair<std::string, int64_t>> ret;
        auto tagFound = tagContext->ttlInfo_.find(tagId);
        if (tagFound != tagContext->ttlInfo_.end()) {
            ret.emplace(tagFound->second.first, tagFound->second.second);
        }
        return ret;
    }

    std::string name_;
    std::vector<RelNode<T>*> dependencies_;
    bool hasDependents_ = false;
};

class QueryUtils final {
public:
    static StatusOr<nebula::Value> readValue(RowReader* reader, const PropContext& ctx) {
        auto value = reader->getValueByName(ctx.name_);
        if (value.type() == Value::Type::NULLVALUE) {
            // read null value
            auto nullType = value.getNull();
            if (nullType == NullType::BAD_DATA ||
                nullType == NullType::BAD_TYPE ||
                nullType == NullType::UNKNOWN_PROP) {
                VLOG(1) << "Fail to read prop " << ctx.name_;
                if (ctx.field_ != nullptr) {
                    if (ctx.field_->hasDefault()) {
                        return ctx.field_->defaultValue();
                    } else if (ctx.field_->nullable()) {
                        return NullType::__NULL__;
                    }
                }
            } else if (nullType == NullType::__NULL__ || nullType == NullType::NaN) {
                return value;
            }
            return Status::Error(folly::stringPrintf("Fail to read prop %s ", ctx.name_.c_str()));
        }
        return value;
    }

    static nebula::Value readEdgeProp(VertexIDSlice srcId,
                                      EdgeType edgeType,
                                      EdgeRanking edgeRank,
                                      VertexIDSlice dstId,
                                      RowReader* reader,
                                      const PropContext& prop) {
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
                value = srcId.str();
                break;
            }
            case PropContext::PropInKeyType::TYPE: {
                value = edgeType;
                break;
            }
            case PropContext::PropInKeyType::RANK: {
                value = edgeRank;
                break;
            }
            case PropContext::PropInKeyType::DST: {
                value = dstId.str();
                break;
            }
        }
        return value;
    }
};

// QueryNode is the node which would read data from kvstore, it usually generate a row in response
// or a cell in a row.
template<typename T>
class QueryNode : public RelNode<T> {
public:
    const Value& result() {
        return result_;
    }

    Value& mutableResult() {
        return result_;
    }

protected:
    kvstore::ResultCode collectEdgeProps(VertexIDSlice srcId,
                                         EdgeType edgeType,
                                         EdgeRanking edgeRank,
                                         VertexIDSlice dstId,
                                         RowReader* reader,
                                         const std::vector<PropContext>* props,
                                         nebula::List& list) {
        for (size_t i = 0; i < props->size(); i++) {
            const auto& prop = (*props)[i];
            if (prop.returned_) {
                VLOG(2) << "Collect prop " << prop.name_ << ", type " << edgeType;
                auto value = QueryUtils::readEdgeProp(srcId, edgeType, edgeRank, dstId,
                                                      reader, prop);
                list.values.emplace_back(std::move(value));
            }
        }
        return kvstore::ResultCode::SUCCEEDED;
    }

    kvstore::ResultCode collectTagProps(TagID tagId,
                                        RowReader* reader,
                                        const std::vector<PropContext>* props,
                                        nebula::List& list,
                                        FilterContext* filter) {
        for (auto& prop : *props) {
            VLOG(2) << "Collect prop " << prop.name_ << ", type " << tagId;
            if (reader != nullptr) {
                auto status = QueryUtils::readValue(reader, prop);
                if (!status.ok()) {
                    return kvstore::ResultCode::ERR_TAG_PROP_NOT_FOUND;
                }
                auto value = std::move(status.value());
                if (filter != nullptr && prop.tagFiltered_) {
                    filter->fillTagProp(tagId, prop.name_, value);
                }
                if (prop.returned_) {
                    list.values.emplace_back(std::move(value));
                }
            }
        }
        return kvstore::ResultCode::SUCCEEDED;
    }

    Value result_;
};

}  // namespace storage
}  // namespace nebula

#endif  // STORAGE_EXEC_RELNODE_H_
