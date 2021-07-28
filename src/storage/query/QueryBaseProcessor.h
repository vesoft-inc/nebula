/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef STORAGE_QUERY_QUERYBASEPROCESSOR_H_
#define STORAGE_QUERY_QUERYBASEPROCESSOR_H_

#include "common/base/Base.h"
#include "common/context/ExpressionContext.h"
#include "common/expression/Expression.h"
#include "common/expression/PropertyExpression.h"
#include "common/expression/ArithmeticExpression.h"
#include "common/expression/ConstantExpression.h"
#include "common/expression/FunctionCallExpression.h"
#include "common/expression/LogicalExpression.h"
#include "common/expression/RelationalExpression.h"
#include "common/expression/TypeCastingExpression.h"
#include "common/expression/UUIDExpression.h"
#include "common/expression/UnaryExpression.h"
#include "common/expression/ContainerExpression.h"
#include "common/expression/CaseExpression.h"
#include "common/expression/VariableExpression.h"
#include "common/expression/ListComprehensionExpression.h"
#include "common/expression/PredicateExpression.h"
#include "common/expression/ReduceExpression.h"
#include "storage/BaseProcessor.h"

namespace nebula {
namespace storage {

// The PropContext stores the info about property to be returned or filtered
struct PropContext {
public:
    enum class PropInKeyType {
        NONE    = 0x00,
        VID     = 0x01,
        TAG     = 0x02,
        SRC     = 0x03,
        TYPE    = 0x04,
        RANK    = 0x05,
        DST     = 0x06,
    };

    explicit PropContext(const char* name)
        : name_(name) {
        setPropInKey();
    }

    PropContext(const char* name,
                const meta::SchemaProviderIf::Field* field,
                bool returned,
                bool filtered,
                const std::pair<size_t, cpp2::StatType>* statInfo = nullptr)
        : name_(name)
        , field_(field)
        , returned_(returned)
        , filtered_(filtered) {
        setPropInKey();
        if (statInfo != nullptr) {
            addStat(statInfo);
        }
    }

    void setPropInKey() {
        if (name_ == kVid) {
            propInKeyType_ = PropContext::PropInKeyType::VID;
        } else if (name_ == kTag) {
            propInKeyType_ = PropContext::PropInKeyType::TAG;
        } else if (name_ == kSrc) {
            propInKeyType_ = PropContext::PropInKeyType::SRC;
        } else if (name_ == kType) {
            propInKeyType_ = PropContext::PropInKeyType::TYPE;
        } else if (name_ == kRank) {
            propInKeyType_ = PropContext::PropInKeyType::RANK;
        } else if (name_ == kDst) {
            propInKeyType_ = PropContext::PropInKeyType::DST;
        }
    }

    void addStat(const std::pair<size_t, cpp2::StatType>* statInfo) {
        hasStat_ = true;
        statIndex_.emplace_back(statInfo->first);
        statType_.emplace_back(statInfo->second);
    }

    // prop name
    std::string name_;
    // field info, e.g. nullable, default value
    const meta::SchemaProviderIf::Field* field_;
    bool returned_ = false;
    bool filtered_ = false;
    // prop type in edge key, for srcId/dstId/type/rank
    PropInKeyType propInKeyType_ = PropInKeyType::NONE;

    // for edge prop stat, such as count/avg/sum
    bool hasStat_ = false;
    // stat prop index from request
    std::vector<size_t> statIndex_;
    std::vector<cpp2::StatType> statType_;
};


struct TagContext {
    std::vector<std::pair<TagID, std::vector<PropContext>>>             propContexts_;
    // indicates whether TagID is in propContxts_
    std::unordered_map<TagID, size_t>                                   indexMap_;
    // tagId -> tagName
    std::unordered_map<TagID, std::string>                              tagNames_;
    // tagId -> tag schema
    std::unordered_map<TagID,
        std::vector<std::shared_ptr<const meta::NebulaSchemaProvider>>> schemas_;
    // tagId -> tag ttl info
    std::unordered_map<TagID, std::pair<std::string, int64_t>>          ttlInfo_;
};


struct EdgeContext {
    // propContexts_, indexMap_, edgeNames_ will contain both +/- edges
    std::vector<std::pair<EdgeType, std::vector<PropContext>>> propContexts_;
    // indicates whether EdgeType is in propContxts_
    std::unordered_map<EdgeType, size_t>                                indexMap_;
    // EdgeType -> edgeName
    std::unordered_map<EdgeType, std::string> edgeNames_;

    // schemas_ and ttlInfo_ will contains only + edges
    std::unordered_map<EdgeType,
        std::vector<std::shared_ptr<const meta::NebulaSchemaProvider>>> schemas_;
    // EdgeType -> edge ttl info
    std::unordered_map<EdgeType, std::pair<std::string, int64_t>>       ttlInfo_;

    // offset is the start index of first edge type in a response row
    size_t                                                              offset_;
    size_t                                                              statCount_ = 0;
};


template<typename REQ, typename RESP>
class QueryBaseProcessor : public BaseProcessor<RESP> {
public:
    virtual ~QueryBaseProcessor() = default;

    virtual void process(const REQ& req) = 0;

protected:
    explicit QueryBaseProcessor(StorageEnv* env,
                                const ProcessorCounters* counters,
                                folly::Executor* executor = nullptr)
        : BaseProcessor<RESP>(env, counters)
        , executor_(executor) {
    }

    virtual nebula::cpp2::ErrorCode checkAndBuildContexts(const REQ& req) = 0;
    virtual void onProcessFinished() = 0;

    nebula::cpp2::ErrorCode getSpaceVertexSchema();
    nebula::cpp2::ErrorCode getSpaceEdgeSchema();

    // build tagContexts_ according to return props
    nebula::cpp2::ErrorCode handleVertexProps(std::vector<cpp2::VertexProp>& tagProps);
    // build edgeContexts_ according to return props
    nebula::cpp2::ErrorCode handleEdgeProps(std::vector<cpp2::EdgeProp>& edgeProps);

    nebula::cpp2::ErrorCode buildFilter(const REQ& req);
    nebula::cpp2::ErrorCode buildYields(const REQ& req);

    // build ttl info map
    void buildTagTTLInfo();
    void buildEdgeTTLInfo();

    std::vector<cpp2::VertexProp> buildAllTagProps();
    std::vector<cpp2::EdgeProp> buildAllEdgeProps(const cpp2::EdgeDirection& direction);

    nebula::cpp2::ErrorCode
    checkExp(const Expression* exp,
             bool returned,
             bool filtered,
             bool updated = false);

    void addReturnPropContext(std::vector<PropContext>& ctxs,
                              const char* propName,
                              const meta::SchemaProviderIf::Field* field);

    void addPropContextIfNotExists(
            std::vector<std::pair<SchemaID, std::vector<PropContext>>>& props,
            std::unordered_map<SchemaID, size_t>& indexMap,
            std::unordered_map<SchemaID, std::string>& names,
            int32_t entryId,
            const std::string& entryName,
            const std::string& propName,
            const meta::SchemaProviderIf::Field* field,
            bool returned,
            bool filtered,
            const std::pair<size_t, cpp2::StatType>* statInfo = nullptr);

protected:
    GraphSpaceID                                        spaceId_;
    folly::Executor*                                    executor_{nullptr};
    std::unique_ptr<PlanContext>                        planContext_;
    TagContext                                          tagContext_;
    EdgeContext                                         edgeContext_;
    Expression*                                         filter_{nullptr};

    // Collect prop in value expression in upsert set clause
    std::unordered_set<std::string>                     valueProps_;

    nebula::DataSet                                     resultDataSet_;
};

}  // namespace storage
}  // namespace nebula

#include "storage/query/QueryBaseProcessor.inl"

#endif  // STORAGE_QUERY_QUERYBASEPROCESSOR_H_
