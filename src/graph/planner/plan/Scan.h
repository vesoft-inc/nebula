/* Copyright (c) 2021 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef PLANNER_PLAN_SCAN_H_
#define PLANNER_PLAN_SCAN_H_

#include "planner/plan/Query.h"

namespace nebula {
namespace graph {

// Logical Plan
class EdgeIndexScan : public IndexScan {
public:
    const std::string& edgeType() const {
        return edgeType_;
    }

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
                  std::string filter,
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
                    std::move(filter),
                    kind),
          edgeType_(edgeType) {}

    std::string edgeType_;
};

class EdgeIndexPrefixScan : public EdgeIndexScan {
public:
    static EdgeIndexPrefixScan* make(QueryContext* qctx,
                                     PlanNode* input,
                                     const std::string& edgeType,
                                     GraphSpaceID space = -1,   //  TBD: -1 is inValid spaceID?
                                     std::vector<IndexQueryContext>&& contexts = {},
                                     std::vector<std::string> returnCols = {},
                                     int32_t schemaId = -1,
                                     bool isEmptyResultSet = false,
                                     bool dedup = false,
                                     std::vector<storage::cpp2::OrderBy> orderBy = {},
                                     int64_t limit = std::numeric_limits<int64_t>::max(),
                                     std::string filter = "") {
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
                                                            std::move(filter)));
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
                        std::string filter)
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
                        std::move(filter),
                        Kind::kEdgeIndexPrefixScan) {}
};

class EdgeIndexRangeScan : public EdgeIndexScan {
public:
    static EdgeIndexRangeScan* make(QueryContext* qctx,
                                    PlanNode* input,
                                    const std::string& edgeType,
                                    GraphSpaceID space = -1,   //  TBD: -1 is inValid spaceID?
                                    std::vector<IndexQueryContext>&& contexts = {},
                                    std::vector<std::string> returnCols = {},
                                    int32_t schemaId = -1,
                                    bool isEmptyResultSet = false,
                                    bool dedup = false,
                                    std::vector<storage::cpp2::OrderBy> orderBy = {},
                                    int64_t limit = std::numeric_limits<int64_t>::max(),
                                    std::string filter = "") {
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
                                                           std::move(filter)));
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
                       std::string filter)
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
                        std::move(filter),
                        Kind::kEdgeIndexRangeScan) {}
};

class EdgeIndexFullScan final : public EdgeIndexScan {
public:
    static EdgeIndexFullScan* make(QueryContext* qctx,
                                   PlanNode* input,
                                   const std::string& edgeType,
                                   GraphSpaceID space = -1,   //  TBD: -1 is inValid spaceID?
                                   std::vector<IndexQueryContext>&& contexts = {},
                                   std::vector<std::string> returnCols = {},
                                   int32_t schemaId = -1,
                                   bool isEmptyResultSet = false,
                                   bool dedup = false,
                                   std::vector<storage::cpp2::OrderBy> orderBy = {},
                                   int64_t limit = std::numeric_limits<int64_t>::max(),
                                   std::string filter = "") {
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
                                                          std::move(filter)));
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
                      std::string filter)
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
                        std::move(filter),
                        Kind::kEdgeIndexFullScan) {}
};

// class EdgeFullTextIndexScan : public EdgeIndexScan {};

class TagIndexScan : public IndexScan {
public:
    const std::string& tagName() const {
        return tagName_;
    }

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
                 std::string filter,
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
                    std::move(filter),
                    kind),
          tagName_(tagName) {}

    std::string tagName_;
};

class TagIndexPrefixScan : public TagIndexScan {
public:
    static TagIndexPrefixScan* make(QueryContext* qctx,
                                    PlanNode* input,
                                    const std::string& tagName,
                                    GraphSpaceID space = -1,   //  TBD: -1 is inValid spaceID?
                                    std::vector<IndexQueryContext>&& contexts = {},
                                    std::vector<std::string> returnCols = {},
                                    int32_t schemaId = -1,
                                    bool isEmptyResultSet = false,
                                    bool dedup = false,
                                    std::vector<storage::cpp2::OrderBy> orderBy = {},
                                    int64_t limit = std::numeric_limits<int64_t>::max(),
                                    std::string filter = "") {
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
                                                           std::move(filter)));
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
                       std::string filter)
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
                       std::move(filter),
                       Kind::kTagIndexPrefixScan) {}
};

class TagIndexRangeScan : public TagIndexScan {
public:
    static TagIndexRangeScan* make(QueryContext* qctx,
                                   PlanNode* input,
                                   const std::string& tagName,
                                   GraphSpaceID space = -1,   //  TBD: -1 is inValid spaceID?
                                   std::vector<IndexQueryContext>&& contexts = {},
                                   std::vector<std::string> returnCols = {},
                                   int32_t schemaId = -1,
                                   bool isEmptyResultSet = false,
                                   bool dedup = false,
                                   std::vector<storage::cpp2::OrderBy> orderBy = {},
                                   int64_t limit = std::numeric_limits<int64_t>::max(),
                                   std::string filter = "") {
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
                                                          std::move(filter)));
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
                      std::string filter)
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
                       std::move(filter),
                       Kind::kTagIndexRangeScan) {}
};

class TagIndexFullScan final : public TagIndexScan {
public:
    static TagIndexFullScan* make(QueryContext* qctx,
                                  PlanNode* input,
                                  const std::string& tagName,
                                  GraphSpaceID space = -1,   //  TBD: -1 is inValid spaceID?
                                  std::vector<IndexQueryContext>&& contexts = {},
                                  std::vector<std::string> returnCols = {},
                                  int32_t schemaId = -1,
                                  bool isEmptyResultSet = false,
                                  bool dedup = false,
                                  std::vector<storage::cpp2::OrderBy> orderBy = {},
                                  int64_t limit = std::numeric_limits<int64_t>::max(),
                                  std::string filter = "") {
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
                                                         std::move(filter)));
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
                     std::string filter)
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
                       std::move(filter),
                       Kind::kTagIndexFullScan) {}
};

// class TagFullTextIndexScan : public TagIndexScan {};

}   // namespace graph
}   // namespace nebula

#endif   // PLANNER_PLAN_SCAN_H_
