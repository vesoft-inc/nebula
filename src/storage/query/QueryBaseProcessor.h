/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef STORAGE_QUERY_QUERYBASEPROCESSOR_H_
#define STORAGE_QUERY_QUERYBASEPROCESSOR_H_

#include "common/base/Base.h"
#include "common/expression/Expression.h"
#include "storage/BaseProcessor.h"

namespace nebula {
namespace storage {

struct ReturnProp {
    int32_t entryId_;                       // tagId or edgeType
    std::vector<std::string> names_;        // property names
};

// The PropContext stores the info about property to be returned or filtered
class PropContext {
public:
    enum class PropInKeyType {
        NONE = 0x00,
        SRC = 0x01,
        DST = 0x02,
        TYPE = 0x03,
        RANK = 0x04,
    };

    explicit PropContext(const char* name)
        : name_(name) {}

    // prop name
    std::string name_;
    // field info, e.g. nullable, default value
    const meta::SchemaProviderIf::Field* field_;
    bool tagFiltered_ = false;
    bool returned_ = false;
    // prop type in edge key, for srcId/dstId/type/rank
    PropInKeyType propInKeyType_ = PropInKeyType::NONE;

    // for edge prop stat, such as count/avg/sum
    bool hasStat_ = false;
    // stat prop index from request
    size_t statIndex_;
    cpp2::StatType statType_;
};

const std::vector<std::pair<std::string, PropContext::PropInKeyType>> kPropsInKey_ = {
    {"_src", PropContext::PropInKeyType::SRC},
    {"_type", PropContext::PropInKeyType::TYPE},
    {"_rank", PropContext::PropInKeyType::RANK},
    {"_dst", PropContext::PropInKeyType::DST},
};

struct TagContext {
    std::vector<std::pair<TagID, std::vector<PropContext>>> propContexts_;
    std::unordered_map<TagID, size_t> indexMap_;
    std::unordered_map<TagID,
                       std::vector<std::shared_ptr<const meta::NebulaSchemaProvider>>> schemas_;
    std::unordered_map<TagID, std::pair<std::string, int64_t>> ttlInfo_;
    VertexCache* vertexCache_ = nullptr;
};

struct EdgeContext {
    std::vector<std::pair<EdgeType, std::vector<PropContext>>> propContexts_;
    std::unordered_map<EdgeType, size_t> indexMap_;
    std::unordered_map<EdgeType,
                       std::vector<std::shared_ptr<const meta::NebulaSchemaProvider>>> schemas_;
    std::unordered_map<EdgeType, std::pair<std::string, int64_t>> ttlInfo_;
    // offset is the start index of first edge type in a response row
    size_t offset_;
    size_t statCount_ = 0;
};

template<typename REQ, typename RESP>
class QueryBaseProcessor : public BaseProcessor<RESP> {
public:
    virtual ~QueryBaseProcessor() = default;

    virtual void process(const REQ& req) = 0;

protected:
    explicit QueryBaseProcessor(StorageEnv* env,
                                stats::Stats* stats = nullptr,
                                VertexCache* cache = nullptr)
        : BaseProcessor<RESP>(env, stats) {
        this->tagContext_.vertexCache_ = cache;
    }

    virtual cpp2::ErrorCode checkAndBuildContexts(const REQ& req) = 0;
    virtual void onProcessFinished() = 0;

    cpp2::ErrorCode getSpaceVertexSchema();
    cpp2::ErrorCode getSpaceEdgeSchema();

    // collect tag props need to return
    cpp2::ErrorCode prepareVertexProps(const std::vector<cpp2::PropExp>& vertexProps,
                                       std::vector<ReturnProp>& returnProps);

    // collect edge props need to return
    cpp2::ErrorCode prepareEdgeProps(const std::vector<cpp2::PropExp>& edgeProps,
                                     std::vector<ReturnProp>& returnProps);

    // build tagContexts_ according to return props
    cpp2::ErrorCode handleVertexProps(const std::vector<ReturnProp>& vertexProps);
    // build edgeContexts_ according to return props
    cpp2::ErrorCode handleEdgeProps(const std::vector<ReturnProp>& edgeProps);

    cpp2::ErrorCode buildFilter(const REQ& req);

    // build ttl info map
    void buildTagTTLInfo();
    void buildEdgeTTLInfo();

    std::vector<ReturnProp> buildAllTagProps();
    std::vector<ReturnProp> buildAllEdgeProps(const cpp2::EdgeDirection& direction);

protected:
    GraphSpaceID spaceId_;

    TagContext tagContext_;
    EdgeContext edgeContext_;
    std::unique_ptr<Expression> exp_;

    nebula::DataSet resultDataSet_;
};


}  // namespace storage
}  // namespace nebula

#include "storage/query/QueryBaseProcessor.inl"

#endif  // STORAGE_QUERY_QUERYBASEPROCESSOR_H_
