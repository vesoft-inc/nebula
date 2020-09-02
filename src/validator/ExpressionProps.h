/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef VALIDATOR_EXPRESSIONPROPS_H_
#define VALIDATOR_EXPRESSIONPROPS_H_

#include <set>
#include <string>
#include <unordered_map>

#include <folly/String.h>

#include "common/thrift/ThriftTypes.h"

namespace nebula {
namespace graph {

class ExpressionProps final {
public:
    using TagIDPropsMap = std::unordered_map<TagID, std::set<folly::StringPiece>>;
    using EdgePropMap = std::unordered_map<EdgeType, std::set<folly::StringPiece>>;
    using VarPropMap = std::unordered_map<std::string, std::set<folly::StringPiece>>;

    ExpressionProps() = default;
    ~ExpressionProps() = default;

    const std::set<folly::StringPiece>& inputProps() const {
        return inputProps_;
    }
    const TagIDPropsMap& srcTagProps() const {
        return srcTagProps_;
    }
    const TagIDPropsMap& dstTagProps() const {
        return dstTagProps_;
    }
    const TagIDPropsMap& tagProps() const {
        return tagProps_;
    }
    const EdgePropMap& edgeProps() const {
        return edgeProps_;
    }
    const VarPropMap& varProps() const {
        return varProps_;
    }

    void insertInputProp(folly::StringPiece prop);
    void insertVarProp(const std::string& varName, folly::StringPiece prop);
    void insertSrcTagProp(TagID tagId, folly::StringPiece prop);
    void insertDstTagProp(TagID tagId, folly::StringPiece prop);
    void insertEdgeProp(EdgeType edgeType, folly::StringPiece prop);
    void insertTagProp(TagID tagId, folly::StringPiece prop);
    bool isSubsetOfInput(const std::set<folly::StringPiece>& props);
    bool isSubsetOfVar(const VarPropMap& props);
    void unionProps(ExpressionProps exprProps);

private:
    std::set<folly::StringPiece> inputProps_;
    VarPropMap varProps_;
    TagIDPropsMap srcTagProps_;
    TagIDPropsMap dstTagProps_;
    EdgePropMap edgeProps_;
    TagIDPropsMap tagProps_;
};

}   // namespace graph
}   // namespace nebula

#endif   // VALIDATOR_EXPRESSIONPROPS_H_
