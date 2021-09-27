/* Copyright (c) 2021 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef GRAPH_PLANNER_PLAN_SCAN_H_
#define GRAPH_PLANNER_PLAN_SCAN_H_

#include "graph/planner/plan/Query.h"

namespace nebula {
namespace graph {

// Logical Plan
class EdgeIndexScan : public IndexScan {
 public:
  const std::string& edgeType() const { return edgeType_; }

 protected:
  EdgeIndexScan(QueryContext* qctx,
                PlanNode* input,
                const std::string& edgeType,
                GraphSpaceID space,
                std::vector<IndexQueryContext>&& contexts,
                std::vector<std::string> returnCols,
                int32_t schemaId,
                bool isEmptyResultSet,
                bool dedup,
                std::vector<storage::cpp2::OrderBy> orderBy,
                int64_t limit,
                Expression* filter,
                Kind kind)
      : IndexScan(qctx,
                  input,
                  space,
                  std::move(contexts),
                  std::move(returnCols),
                  true,
                  schemaId,
                  isEmptyResultSet,
                  dedup,
                  std::move(orderBy),
                  limit,
                  filter,
                  kind),
        edgeType_(edgeType) {}

  void cloneMembers(const EdgeIndexScan& es) {
    IndexScan::cloneMembers(es);
    edgeType_ = es.edgeType_;
  }

  std::string edgeType_;
};

class EdgeIndexPrefixScan : public EdgeIndexScan {
 public:
  static EdgeIndexPrefixScan* make(QueryContext* qctx,
                                   PlanNode* input,
                                   const std::string& edgeType,
                                   GraphSpaceID space = -1,  //  TBD: -1 is inValid spaceID?
                                   std::vector<IndexQueryContext>&& contexts = {},
                                   std::vector<std::string> returnCols = {},
                                   int32_t schemaId = -1,
                                   bool isEmptyResultSet = false,
                                   bool dedup = false,
                                   std::vector<storage::cpp2::OrderBy> orderBy = {},
                                   int64_t limit = std::numeric_limits<int64_t>::max(),
                                   Expression* filter = nullptr) {
    return qctx->objPool()->add(new EdgeIndexPrefixScan(qctx,
                                                        input,
                                                        edgeType,
                                                        space,
                                                        std::move(contexts),
                                                        std::move(returnCols),
                                                        schemaId,
                                                        isEmptyResultSet,
                                                        dedup,
                                                        std::move(orderBy),
                                                        limit,
                                                        filter));
  }

  PlanNode* clone() const override {
    auto* newEdgeIndexPrefixScan = EdgeIndexPrefixScan::make(qctx_, nullptr, "");
    newEdgeIndexPrefixScan->cloneMembers(*this);
    return newEdgeIndexPrefixScan;
  }

 private:
  EdgeIndexPrefixScan(QueryContext* qctx,
                      PlanNode* input,
                      const std::string& edgeType,
                      GraphSpaceID space,
                      std::vector<IndexQueryContext>&& contexts,
                      std::vector<std::string> returnCols,
                      int32_t schemaId,
                      bool isEmptyResultSet,
                      bool dedup,
                      std::vector<storage::cpp2::OrderBy> orderBy,
                      int64_t limit,
                      Expression* filter)
      : EdgeIndexScan(qctx,
                      input,
                      edgeType,
                      space,
                      std::move(contexts),
                      std::move(returnCols),
                      schemaId,
                      isEmptyResultSet,
                      dedup,
                      std::move(orderBy),
                      limit,
                      filter,
                      Kind::kEdgeIndexPrefixScan) {}
};

class EdgeIndexRangeScan : public EdgeIndexScan {
 public:
  static EdgeIndexRangeScan* make(QueryContext* qctx,
                                  PlanNode* input,
                                  const std::string& edgeType,
                                  GraphSpaceID space = -1,  //  TBD: -1 is inValid spaceID?
                                  std::vector<IndexQueryContext>&& contexts = {},
                                  std::vector<std::string> returnCols = {},
                                  int32_t schemaId = -1,
                                  bool isEmptyResultSet = false,
                                  bool dedup = false,
                                  std::vector<storage::cpp2::OrderBy> orderBy = {},
                                  int64_t limit = std::numeric_limits<int64_t>::max(),
                                  Expression* filter = nullptr) {
    return qctx->objPool()->add(new EdgeIndexRangeScan(qctx,
                                                       input,
                                                       edgeType,
                                                       space,
                                                       std::move(contexts),
                                                       std::move(returnCols),
                                                       schemaId,
                                                       isEmptyResultSet,
                                                       dedup,
                                                       std::move(orderBy),
                                                       limit,
                                                       filter));
  }

  PlanNode* clone() const override {
    auto* newEdgeIndexRangeScan = EdgeIndexRangeScan::make(qctx_, nullptr, "");
    newEdgeIndexRangeScan->cloneMembers(*this);
    return newEdgeIndexRangeScan;
  }

 private:
  EdgeIndexRangeScan(QueryContext* qctx,
                     PlanNode* input,
                     const std::string& edgeType,
                     GraphSpaceID space,
                     std::vector<IndexQueryContext>&& contexts,
                     std::vector<std::string> returnCols,
                     int32_t schemaId,
                     bool isEmptyResultSet,
                     bool dedup,
                     std::vector<storage::cpp2::OrderBy> orderBy,
                     int64_t limit,
                     Expression* filter)
      : EdgeIndexScan(qctx,
                      input,
                      edgeType,
                      space,
                      std::move(contexts),
                      std::move(returnCols),
                      schemaId,
                      isEmptyResultSet,
                      dedup,
                      std::move(orderBy),
                      limit,
                      filter,
                      Kind::kEdgeIndexRangeScan) {}
};

class EdgeIndexFullScan final : public EdgeIndexScan {
 public:
  static EdgeIndexFullScan* make(QueryContext* qctx,
                                 PlanNode* input,
                                 const std::string& edgeType,
                                 GraphSpaceID space = -1,  //  TBD: -1 is inValid spaceID?
                                 std::vector<IndexQueryContext>&& contexts = {},
                                 std::vector<std::string> returnCols = {},
                                 int32_t schemaId = -1,
                                 bool isEmptyResultSet = false,
                                 bool dedup = false,
                                 std::vector<storage::cpp2::OrderBy> orderBy = {},
                                 int64_t limit = std::numeric_limits<int64_t>::max(),
                                 Expression* filter = nullptr) {
    return qctx->objPool()->add(new EdgeIndexFullScan(qctx,
                                                      input,
                                                      edgeType,
                                                      space,
                                                      std::move(contexts),
                                                      std::move(returnCols),
                                                      schemaId,
                                                      isEmptyResultSet,
                                                      dedup,
                                                      std::move(orderBy),
                                                      limit,
                                                      filter));
  }

  PlanNode* clone() const override {
    auto* newEdgeIndexFullScan = EdgeIndexFullScan::make(qctx_, nullptr, "");
    newEdgeIndexFullScan->cloneMembers(*this);
    return newEdgeIndexFullScan;
  }

 private:
  EdgeIndexFullScan(QueryContext* qctx,
                    PlanNode* input,
                    const std::string& edgeType,
                    GraphSpaceID space,
                    std::vector<IndexQueryContext>&& contexts,
                    std::vector<std::string> returnCols,
                    int32_t schemaId,
                    bool isEmptyResultSet,
                    bool dedup,
                    std::vector<storage::cpp2::OrderBy> orderBy,
                    int64_t limit,
                    Expression* filter)
      : EdgeIndexScan(qctx,
                      input,
                      edgeType,
                      space,
                      std::move(contexts),
                      std::move(returnCols),
                      schemaId,
                      isEmptyResultSet,
                      dedup,
                      std::move(orderBy),
                      limit,
                      filter,
                      Kind::kEdgeIndexFullScan) {}
};

// class EdgeFullTextIndexScan : public EdgeIndexScan {};

class TagIndexScan : public IndexScan {
 public:
  const std::string& tagName() const { return tagName_; }

 protected:
  TagIndexScan(QueryContext* qctx,
               PlanNode* input,
               const std::string& tagName,
               GraphSpaceID space,
               std::vector<IndexQueryContext>&& contexts,
               std::vector<std::string> returnCols,
               int32_t schemaId,
               bool isEmptyResultSet,
               bool dedup,
               std::vector<storage::cpp2::OrderBy> orderBy,
               int64_t limit,
               Expression* filter,
               Kind kind)
      : IndexScan(qctx,
                  input,
                  space,
                  std::move(contexts),
                  std::move(returnCols),
                  false,
                  schemaId,
                  isEmptyResultSet,
                  dedup,
                  std::move(orderBy),
                  limit,
                  filter,
                  kind),
        tagName_(tagName) {}

  void cloneMembers(const TagIndexScan& ts) {
    IndexScan::cloneMembers(ts);
    tagName_ = ts.tagName_;
  }

  std::string tagName_;
};

class TagIndexPrefixScan : public TagIndexScan {
 public:
  static TagIndexPrefixScan* make(QueryContext* qctx,
                                  PlanNode* input,
                                  const std::string& tagName,
                                  GraphSpaceID space = -1,  //  TBD: -1 is inValid spaceID?
                                  std::vector<IndexQueryContext>&& contexts = {},
                                  std::vector<std::string> returnCols = {},
                                  int32_t schemaId = -1,
                                  bool isEmptyResultSet = false,
                                  bool dedup = false,
                                  std::vector<storage::cpp2::OrderBy> orderBy = {},
                                  int64_t limit = std::numeric_limits<int64_t>::max(),
                                  Expression* filter = nullptr) {
    return qctx->objPool()->add(new TagIndexPrefixScan(qctx,
                                                       input,
                                                       tagName,
                                                       space,
                                                       std::move(contexts),
                                                       std::move(returnCols),
                                                       schemaId,
                                                       isEmptyResultSet,
                                                       dedup,
                                                       std::move(orderBy),
                                                       limit,
                                                       filter));
  }

  PlanNode* clone() const {
    auto* newTagIndexPrefixScan = TagIndexPrefixScan::make(qctx_, nullptr, "");
    newTagIndexPrefixScan->cloneMembers(*this);
    return newTagIndexPrefixScan;
  }

 private:
  TagIndexPrefixScan(QueryContext* qctx,
                     PlanNode* input,
                     const std::string& tagName,
                     GraphSpaceID space,
                     std::vector<IndexQueryContext>&& contexts,
                     std::vector<std::string> returnCols,
                     int32_t schemaId,
                     bool isEmptyResultSet,
                     bool dedup,
                     std::vector<storage::cpp2::OrderBy> orderBy,
                     int64_t limit,
                     Expression* filter)
      : TagIndexScan(qctx,
                     input,
                     tagName,
                     space,
                     std::move(contexts),
                     std::move(returnCols),
                     schemaId,
                     isEmptyResultSet,
                     dedup,
                     std::move(orderBy),
                     limit,
                     filter,
                     Kind::kTagIndexPrefixScan) {}
};

class TagIndexRangeScan : public TagIndexScan {
 public:
  static TagIndexRangeScan* make(QueryContext* qctx,
                                 PlanNode* input,
                                 const std::string& tagName,
                                 GraphSpaceID space = -1,  //  TBD: -1 is inValid spaceID?
                                 std::vector<IndexQueryContext>&& contexts = {},
                                 std::vector<std::string> returnCols = {},
                                 int32_t schemaId = -1,
                                 bool isEmptyResultSet = false,
                                 bool dedup = false,
                                 std::vector<storage::cpp2::OrderBy> orderBy = {},
                                 int64_t limit = std::numeric_limits<int64_t>::max(),
                                 Expression* filter = nullptr) {
    return qctx->objPool()->add(new TagIndexRangeScan(qctx,
                                                      input,
                                                      tagName,
                                                      space,
                                                      std::move(contexts),
                                                      std::move(returnCols),
                                                      schemaId,
                                                      isEmptyResultSet,
                                                      dedup,
                                                      std::move(orderBy),
                                                      limit,
                                                      filter));
  }

  PlanNode* clone() const {
    auto* newTagIndexRangeScan = TagIndexRangeScan::make(qctx_, nullptr, "");
    newTagIndexRangeScan->cloneMembers(*this);
    return newTagIndexRangeScan;
  }

 private:
  TagIndexRangeScan(QueryContext* qctx,
                    PlanNode* input,
                    const std::string& tagName,
                    GraphSpaceID space,
                    std::vector<IndexQueryContext>&& contexts,
                    std::vector<std::string> returnCols,
                    int32_t schemaId,
                    bool isEmptyResultSet,
                    bool dedup,
                    std::vector<storage::cpp2::OrderBy> orderBy,
                    int64_t limit,
                    Expression* filter)
      : TagIndexScan(qctx,
                     input,
                     tagName,
                     space,
                     std::move(contexts),
                     std::move(returnCols),
                     schemaId,
                     isEmptyResultSet,
                     dedup,
                     std::move(orderBy),
                     limit,
                     filter,
                     Kind::kTagIndexRangeScan) {}
};

class TagIndexFullScan final : public TagIndexScan {
 public:
  static TagIndexFullScan* make(QueryContext* qctx,
                                PlanNode* input,
                                const std::string& tagName,
                                GraphSpaceID space = -1,  //  TBD: -1 is inValid spaceID?
                                std::vector<IndexQueryContext>&& contexts = {},
                                std::vector<std::string> returnCols = {},
                                int32_t schemaId = -1,
                                bool isEmptyResultSet = false,
                                bool dedup = false,
                                std::vector<storage::cpp2::OrderBy> orderBy = {},
                                int64_t limit = std::numeric_limits<int64_t>::max(),
                                Expression* filter = nullptr) {
    return qctx->objPool()->add(new TagIndexFullScan(qctx,
                                                     input,
                                                     tagName,
                                                     space,
                                                     std::move(contexts),
                                                     std::move(returnCols),
                                                     schemaId,
                                                     isEmptyResultSet,
                                                     dedup,
                                                     std::move(orderBy),
                                                     limit,
                                                     filter));
  }

  PlanNode* clone() const {
    auto* newTagIndexFullScan = TagIndexFullScan::make(qctx_, nullptr, "");
    newTagIndexFullScan->cloneMembers(*this);
    return newTagIndexFullScan;
  }

 private:
  TagIndexFullScan(QueryContext* qctx,
                   PlanNode* input,
                   const std::string& tagName,
                   GraphSpaceID space,
                   std::vector<IndexQueryContext>&& contexts,
                   std::vector<std::string> returnCols,
                   int32_t schemaId,
                   bool isEmptyResultSet,
                   bool dedup,
                   std::vector<storage::cpp2::OrderBy> orderBy,
                   int64_t limit,
                   Expression* filter)
      : TagIndexScan(qctx,
                     input,
                     tagName,
                     space,
                     std::move(contexts),
                     std::move(returnCols),
                     schemaId,
                     isEmptyResultSet,
                     dedup,
                     std::move(orderBy),
                     limit,
                     filter,
                     Kind::kTagIndexFullScan) {}
};

// class TagFullTextIndexScan : public TagIndexScan {};

}  // namespace graph
}  // namespace nebula

#endif  // GRAPH_PLANNER_PLAN_SCAN_H_
