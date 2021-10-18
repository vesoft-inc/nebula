/* Copyright (c) 2021 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef GRAPH_CONTEXT_AST_QUERYASTCONTEXT_H_
#define GRAPH_CONTEXT_AST_QUERYASTCONTEXT_H_

#include "common/base/Base.h"
#include "common/expression/Expression.h"
#include "graph/context/ast/AstContext.h"
#include "graph/visitor/DeducePropsVisitor.h"

namespace nebula {
namespace graph {

enum FromType {
  kInstantExpr,
  kVariable,
  kPipe,
};

struct Starts {
  FromType fromType{kInstantExpr};
  Expression* src{nullptr};
  Expression* originalSrc{nullptr};
  std::string userDefinedVarName;
  std::string runtimeVidName;
  std::vector<Value> vids;
};

struct Over {
  bool isOverAll{false};
  std::vector<EdgeType> edgeTypes;
  storage::cpp2::EdgeDirection direction;
  std::vector<std::string> allEdges;
};

// path context
struct PathContext final : AstContext {
  Starts from;
  Starts to;
  StepClause steps;
  Over over;
  Expression* filter{nullptr};

  /*
   * find path from A to B OR find path from $-.src to $-.dst
   * fromVidsVar's DataSet save A OR $-.src
   * toVidsVar's DataSet save B OR $-.dst
   */
  std::string fromVidsVar;
  std::string toVidsVar;

  bool isShortest{false};
  bool isWeight{false};
  bool noLoop{false};
  bool withProp{false};

  /*
   * runtime
   * find path from $-.src to $-.dst
   * project($-.src)<- dedup($-.src)
   * runtimeFromProject is project($-.src)
   * runtimeFromDedup is dedup($-.src)
   */
  PlanNode* runtimeFromProject{nullptr};
  PlanNode* runtimeFromDedup{nullptr};
  PlanNode* runtimeToProject{nullptr};
  PlanNode* runtimeToDedup{nullptr};
  // just for pipe sentence,
  // store the result of the previous sentence
  std::string inputVarName;
  ExpressionProps exprProps;
};

struct GoContext final : AstContext {
  Starts from;
  StepClause steps;
  Over over;
  Expression* filter{nullptr};
  YieldColumns* yieldExpr;
  bool distinct{false};
  // true: sample, false: limit
  bool random{false};
  // step limit value
  std::vector<int64_t> limits;
  std::vector<std::string> colNames;

  std::string vidsVar;
  // true when pipe or multi-sentence
  bool joinInput{false};
  // true when $$.tag.prop exist
  bool joinDst{false};

  ExpressionProps exprProps;

  // save dst prop
  YieldColumns* dstPropsExpr;
  // save src and edge prop
  YieldColumns* srcEdgePropsExpr;
  // for track vid in Nsteps
  std::string srcVidColName;
  std::string dstVidColName;

  // store the result of the previous sentence
  std::string inputVarName;
};

struct LookupContext final : public AstContext {
  bool isEdge{false};
  bool dedup{false};
  bool isEmptyResultSet{false};
  int32_t schemaId{-1};
  int64_t limit{-1};
  Expression* filter{nullptr};
  YieldColumns* yieldExpr{nullptr};
  std::vector<std::string> idxReturnCols;
  std::vector<std::string> idxColNames;
  // order by
};

struct SubgraphContext final : public AstContext {
  Starts from;
  StepClause steps;
  std::string loopSteps;
  YieldColumns* yieldExpr;
  std::vector<std::string> colNames;
  std::unordered_set<EdgeType> edgeTypes;
  bool withProp{false};
  bool getVertexProp{false};
  bool getEdgeProp{false};
};

struct FetchVerticesContext final : public AstContext {
  Starts from;
  bool distinct{false};
  YieldColumns* yieldExpr{nullptr};
  ExpressionProps exprProps;

  // store the result of the previous sentence
  std::string inputVarName;
};

struct FetchEdgesContext final : public AstContext {
  Expression* src{nullptr};
  Expression* dst{nullptr};
  Expression* rank{nullptr};
  Expression* type{nullptr};

  ExpressionProps exprProps;
  YieldColumns* yieldExpr{nullptr};
  std::string edgeName;
  bool distinct{false};
  // store the result of the previous sentence
  std::string inputVarName;
};

struct AlterSchemaContext final : public AstContext {
  std::vector<meta::cpp2::AlterSchemaItem> schemaItems;
  meta::cpp2::SchemaProp schemaProps;
};

struct CreateSchemaContext final : public AstContext {
  bool ifNotExist{false};
  std::string name;
  meta::cpp2::Schema schema;
};

}  // namespace graph
}  // namespace nebula
#endif  // GRAPH_CONTEXT_AST_QUERYASTCONTEXT_H_
