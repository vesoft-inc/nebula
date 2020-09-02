/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "validator/ExpressionProps.h"

namespace nebula {
namespace graph {

void ExpressionProps::insertVarProp(const std::string& varName, folly::StringPiece prop) {
    auto& props = varProps_[varName];
    props.emplace(prop);
}

void ExpressionProps::insertInputProp(folly::StringPiece prop) {
    inputProps_.emplace(prop);
}

void ExpressionProps::insertSrcTagProp(TagID tagId, folly::StringPiece prop) {
    auto& props = srcTagProps_[tagId];
    props.emplace(prop);
}

void ExpressionProps::insertDstTagProp(TagID tagId, folly::StringPiece prop) {
    auto& props = dstTagProps_[tagId];
    props.emplace(prop);
}

void ExpressionProps::insertEdgeProp(EdgeType edgeType, folly::StringPiece prop) {
    auto& props = edgeProps_[edgeType];
    props.emplace(prop);
}

void ExpressionProps::insertTagProp(TagID tagId, folly::StringPiece prop) {
    auto& props = tagProps_[tagId];
    props.emplace(prop);
}

bool ExpressionProps::isSubsetOfInput(const std::set<folly::StringPiece>& props) {
    for (auto& prop : props) {
        if (inputProps_.find(prop) == inputProps_.end()) {
            return false;
        }
    }
    return true;
}

bool ExpressionProps::isSubsetOfVar(const VarPropMap& props) {
    for (auto& iter : props) {
        if (varProps_.find(iter.first) == varProps_.end()) {
            return false;
        }
        for (auto& prop : iter.second) {
            if (varProps_[iter.first].find(prop) == varProps_[iter.first].end()) {
                return false;
            }
        }
    }
    return true;
}

void ExpressionProps::unionProps(ExpressionProps exprProps) {
    if (!exprProps.inputProps().empty()) {
        inputProps_.insert(std::make_move_iterator(exprProps.inputProps().begin()),
                           std::make_move_iterator(exprProps.inputProps().end()));
    }
    if (!exprProps.srcTagProps().empty()) {
        for (auto& iter : exprProps.srcTagProps()) {
            srcTagProps_[iter.first].insert(std::make_move_iterator(iter.second.begin()),
                                            std::make_move_iterator(iter.second.end()));
        }
    }
    if (!exprProps.dstTagProps().empty()) {
        for (auto& iter : exprProps.dstTagProps()) {
            dstTagProps_[iter.first].insert(std::make_move_iterator(iter.second.begin()),
                                            std::make_move_iterator(iter.second.end()));
        }
    }
    if (!exprProps.tagProps().empty()) {
        for (auto& iter : exprProps.tagProps()) {
            tagProps_[iter.first].insert(std::make_move_iterator(iter.second.begin()),
                                         std::make_move_iterator(iter.second.end()));
        }
    }
    if (!exprProps.varProps().empty()) {
        for (auto& iter : exprProps.varProps()) {
            varProps_[iter.first].insert(std::make_move_iterator(iter.second.begin()),
                                         std::make_move_iterator(iter.second.end()));
        }
    }
    if (!exprProps.edgeProps().empty()) {
        for (auto& iter : exprProps.edgeProps()) {
            edgeProps_[iter.first].insert(std::make_move_iterator(iter.second.begin()),
                                          std::make_move_iterator(iter.second.end()));
        }
    }
}

}   // namespace graph
}   // namespace nebula
