/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef _VALIDATOR_FETCH_VERTICES_VALIDATOR_H_
#define _VALIDATOR_FETCH_VERTICES_VALIDATOR_H_

#include "common/interface/gen-cpp2/storage_types.h"
#include "parser/TraverseSentences.h"
#include "validator/Validator.h"

namespace nebula {
namespace graph {

class FetchVerticesValidator final : public Validator {
public:
    using VertexProp = nebula::storage::cpp2::VertexProp;
    using Expr = nebula::storage::cpp2::Expr;
    FetchVerticesValidator(Sentence* sentence, QueryContext* context)
        : Validator(sentence, context) {}

private:
    Status validateImpl() override;

    Status toPlan() override;

    Status check();

    Status prepareVertices();

    Status preparePropertiesWithYield(const YieldClause *yield);
    Status preparePropertiesWithoutYield();
    Status prepareProperties();

    // TODO(shylock) merge the code
    std::string buildConstantInput();
    std::string buildRuntimeInput();

private:
    // src from constant
    DataSet srcVids_{{kVid}};
    // src from runtime
    Expression* srcRef_{nullptr};
    Expression* src_{nullptr};
    bool onStar_{false};
    std::unordered_map<std::string, TagID> tags_;
    std::map<TagID, std::shared_ptr<const meta::SchemaProviderIf>> tagsSchema_;
    std::unique_ptr<std::vector<VertexProp>> props_;
    std::unique_ptr<std::vector<Expr>> exprs_;
    bool dedup_{false};
    std::vector<storage::cpp2::OrderBy> orderBy_{};
    int64_t limit_{std::numeric_limits<int64_t>::max()};
    std::string filter_{};
    // valid when yield expression not require storage
    // So expression like these will be evaluate in Project Executor
    bool withYield_{false};
    // outputs
    std::vector<std::string> gvColNames_;
    // new yield to inject reserved properties for compatible with 1.0
    YieldColumns* newYieldColumns_{nullptr};
    // input
    std::string inputVar_;   // empty when pipe or no input in fact
};

}   // namespace graph
}   // namespace nebula

#endif  // _VALIDATOR_FETCH_VERTICES_VALIDATOR_H_
