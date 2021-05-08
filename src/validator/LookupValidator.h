/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */
#ifndef _VALIDATOR_INDEXSCAN_VALIDATOR_H_
#define _VALIDATOR_INDEXSCAN_VALIDATOR_H_

#include "planner/plan/Query.h"
#include "common/base/Base.h"
#include "common/interface/gen-cpp2/storage_types.h"
#include "common/plugin/fulltext/elasticsearch/ESGraphAdapter.h"
#include "parser/TraverseSentences.h"
#include "validator/Validator.h"

namespace nebula {
namespace graph {

class LookupValidator final : public Validator {
public:
    LookupValidator(Sentence* sentence, QueryContext* context)
        : Validator(sentence, context) {}

private:
    Status validateImpl() override;

    Status toPlan() override;

    Status prepareFrom();

    Status prepareYield();

    Status prepareFilter();

    StatusOr<std::string> rewriteTSFilter(Expression* expr);

    StatusOr<std::vector<std::string>> textSearch(TextSearchExpression* expr);

    bool needTextSearch(Expression* expr);

    StatusOr<Expression*> checkFilter(Expression* expr);

    StatusOr<Expression*> checkRelExpr(RelationalExpression* expr);

    StatusOr<Expression*> rewriteRelExpr(RelationalExpression* expr);

    StatusOr<Value> checkConstExpr(Expression* expr,
                                   const std::string& prop,
                                   const Expression::Kind kind);

    std::unique_ptr<Expression> reverseRelKind(RelationalExpression* expr);

    Status checkTSService();

    Status checkTSIndex();

    const nebula::plugin::HttpClient& randomFTClient() const;

private:
    static constexpr char kSrcVID[] = "SrcVID";
    static constexpr char kDstVID[] = "DstVID";
    static constexpr char kRanking[] = "Ranking";

    static constexpr char kVertexID[] = "VertexID";

    GraphSpaceID                      spaceId_{0};
    IndexScan::IndexQueryCtx          contexts_{};
    IndexScan::IndexReturnCols        returnCols_{};
    bool                              isEdge_{false};
    int32_t                           schemaId_;
    bool                              isEmptyResultSet_{false};
    bool                              textSearchReady_{false};
    std::string                       from_;
    std::vector<nebula::plugin::HttpClient> esClients_;
    std::vector<std::string>          idxScanColNames_;
    std::vector<std::string>          colNames_;
    bool                              withProject_{false};
    bool                              dedup_{false};
    YieldColumns                     *newYieldColumns_{nullptr};
};

}   // namespace graph
}   // namespace nebula


#endif   // _VALIDATOR_INDEXSCAN_VALIDATOR_H_
