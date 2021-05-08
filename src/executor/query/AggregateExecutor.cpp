/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "executor/query/AggregateExecutor.h"

#include "common/datatypes/List.h"
#include "common/expression/AggregateExpression.h"
#include "context/QueryExpressionContext.h"
#include "context/Result.h"
#include "planner/plan/PlanNode.h"
#include "planner/plan/Query.h"
#include "util/ScopedTimer.h"

namespace nebula {
namespace graph {

folly::Future<Status> AggregateExecutor::execute() {
    SCOPED_TIMER(&execTime_);
    auto* agg = asNode<Aggregate>(node());
    auto groupKeys = agg->groupKeys();
    auto groupItems = agg->groupItems();
    auto iter = ectx_->getResult(agg->inputVar()).iter();
    DCHECK(!!iter);
    QueryExpressionContext ctx(ectx_);

    std::unordered_map<List, std::vector<std::unique_ptr<AggData>>, std::hash<nebula::List>> result;

    // generate default result when input dataset is empty
    if (UNLIKELY(!iter->valid())) {
        List defaultValues;
        bool allAggItems = true;
        for (size_t i = 0; i < groupItems.size(); ++i) {
            auto* item = groupItems[i];
            if (UNLIKELY(item->kind() != Expression::Kind::kAggregate)) {
                allAggItems = false;
                break;
            }
            AggData aggData;
            static_cast<AggregateExpression*>(item)->apply(&aggData, Value::kNullValue);
            defaultValues.emplace_back(aggData.result());
        }
        if (allAggItems) {
            List dummyKey;
            std::vector<std::unique_ptr<AggData>> cols;
            for (size_t i = 0; i < groupItems.size(); ++i) {
                cols.emplace_back(new AggData());
                cols[i]->setResult(defaultValues[i]);
            }
            result.emplace(std::make_pair(dummyKey, std::move(cols)));
        }
    }

    for (; iter->valid(); iter->next()) {
        List list;
        for (auto* key : groupKeys) {
            list.values.emplace_back(key->eval(ctx(iter.get())));
        }

        auto it = result.find(list);
        if (it == result.end()) {
           std::vector<std::unique_ptr<AggData>> cols;
           for (size_t i = 0; i < groupItems.size(); ++i) {
               cols.emplace_back(new AggData());
           }
           result.emplace(std::make_pair(list, std::move(cols)));
        } else {
            DCHECK_EQ(it->second.size(), groupItems.size());
        }

        for (size_t i = 0; i < groupItems.size(); ++i) {
            auto* item = groupItems[i];
            if (item->kind() == Expression::Kind::kAggregate) {
                static_cast<AggregateExpression*>(item)->setAggData(result[list][i].get());
                item->eval(ctx(iter.get()));
            } else {
                result[list][i]->setResult(item->eval(ctx(iter.get())));
            }
        }
    }

    DataSet ds;
    ds.colNames = agg->colNames();
    ds.rows.reserve(result.size());
    for (auto& kv : result) {
        Row row;
        for (auto& v : kv.second) {
            row.values.emplace_back(v->result());
        }
        ds.rows.emplace_back(std::move(row));
    }
    return finish(ResultBuilder().value(Value(std::move(ds))).finish());
}

}   // namespace graph
}   // namespace nebula
