/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef EXECUTOR_QUERY_TESTQUERYBASE_H
#define EXECUTOR_QUERY_TESTQUERYBASE_H
#include "common/base/Base.h"

#include <gtest/gtest.h>
#include "parser/GQLParser.h"

namespace nebula {
namespace graph {

class QueryTestBase : public testing::Test {
protected:
    void SetUp() override {
        qctx_ = std::make_unique<QueryContext>();
        // GetNeighbors
        {
            DataSet dataset({kVid, "_stats", "_tag:person:name:age",
                             "_edge:+study:_dst:start_year:end_year", "_expr"});
            {
                Row row;
                // vid
                row.values.emplace_back("Ann");

                // _stats
                row.values.emplace_back(Value::kEmpty);

                // vertices props
                List vertices({"Ann", 18});
                row.values.emplace_back(std::move(vertices));

                // edge props
                List edges;
                List edge1({"School1", 2010, 2014});
                List edge2({"School2", 2014, 2017});
                edges.values.emplace_back(std::move(edge1));
                edges.values.emplace_back(std::move(edge2));
                row.values.emplace_back(std::move(edges));

                // _expr
                row.values.emplace_back(Value::kEmpty);
                dataset.emplace_back(std::move(row));
            }
            {
                Row row;
                // vid
                row.values.emplace_back("Tom");

                // _stats
                row.values.emplace_back(Value::kEmpty);

                // vertices props
                List vertices({"Tom", 18});
                row.values.emplace_back(std::move(vertices));

                // edges props
                List edge1({"School1", 2008, 2012});
                List edge2({"School2", 2012, 2015});

                List edges;
                edges.values.emplace_back(std::move(edge1));
                edges.values.emplace_back(std::move(edge2));
                row.values.emplace_back(std::move(edges));

                // _expr
                row.values.emplace_back(Value::kEmpty);
                dataset.emplace_back(std::move(row));
            }
            List datasetList;
            datasetList.values.emplace_back(std::move(dataset));
            auto result = ResultBuilder()
                              .value(Value(datasetList))
                              .iter(Iterator::Kind::kGetNeighbors)
                              .finish();
            qctx_->symTable()->newVariable("input_neighbor");
            qctx_->ectx()->setResult("input_neighbor", std::move(result));
        }
        // sequential
        {
            DataSet dataset({"vid", "v_name", "v_age", "v_dst", "e_start_year", "e_end_year"});
            dataset.emplace_back(Row({"Ann", "Ann", 18, "School1", 2010, 2014}));
            dataset.emplace_back(Row({"Joy", "Joy", Value::kNullValue, "School2", 2009, 2012}));
            dataset.emplace_back(Row({"Tom", "Tom", 20, "School2", 2008, 2012}));
            dataset.emplace_back(Row({"Kate", "Kate", 19, "School2", 2009, 2013}));
            dataset.emplace_back(Row({"Ann", "Ann", 18, "School1", 2010, 2014}));
            dataset.emplace_back(Row({"Lily", "Lily", 20, "School2", 2009, 2012}));
            qctx_->symTable()->newVariable("input_sequential");
            qctx_->ectx()->setResult("input_sequential",
                                     ResultBuilder().value(Value(dataset)).finish());
        }
        // sequential init by two sequentialIters
        {
            DataSet lds({"vid", "v_name", "v_age", "v_dst", "e_start_year", "e_end_year"});
            lds.emplace_back(Row({"Ann", "Ann", 18, "School1", 2010, 2014}));
            lds.emplace_back(Row({"Joy", "Joy", Value::kNullValue, "School2", 2009, 2012}));
            lds.emplace_back(Row({"Tom", "Tom", 20, "School2", 2008, 2012}));
            lds.emplace_back(Row({"Kate", "Kate", 19, "School2", 2009, 2013}));
            qctx_->symTable()->newVariable("left_neighbor");
            qctx_->ectx()->setResult("left_sequential",
                                     ResultBuilder().value(Value(lds)).finish());

            DataSet rds({"vid", "v_name", "v_age", "v_dst", "e_start_year", "e_end_year"});
            rds.emplace_back(Row({"Ann", "Ann", 18, "School1", 2010, 2014}));
            rds.emplace_back(Row({"Lily", "Lily", 20, "School2", 2009, 2012}));
            qctx_->ectx()->setResult("right_sequential",
                                     ResultBuilder().value(Value(rds)).finish());

            auto lIter = qctx_->ectx()->getResult("left_sequential").iter();
            auto rIter = qctx_->ectx()->getResult("right_sequential").iter();
            ResultBuilder builder;
            builder.value(lIter->valuePtr())
                .iter(std::make_unique<SequentialIter>(std::move(lIter), std::move(rIter)));
            qctx_->symTable()->newVariable("union_sequential");
            qctx_->ectx()->setResult("union_sequential", builder.finish());
        }
        // empty
        {
            DataSet dataset({kVid, "_stats", "_tag:person:name:age",
                             "_edge:+study:_dst:start_year:end_year", "_expr"});
            qctx_->symTable()->newVariable("empty");
            qctx_->ectx()->setResult("empty",
                                     ResultBuilder().value(Value(dataset)).finish());
        }
    }

    YieldSentence* getYieldSentence(const std::string& query, QueryContext* qctx) {
        auto ret = GQLParser(qctx).parse(query);
        CHECK(ret.ok()) << ret.status();
        sentences_ = std::move(ret).value();
        CHECK_EQ(sentences_->kind(), Sentence::Kind::kSequential);
        auto sens = static_cast<const SequentialSentences*>(sentences_.get())->sentences();
        CHECK_EQ(sens.size(), 1);
        CHECK_EQ(sens[0]->kind(), Sentence::Kind::kYield);
        return static_cast<YieldSentence*>(sens[0]);
    }

    YieldColumns* getYieldColumns(const std::string& query, QueryContext* qctx) {
        auto yieldSentence = getYieldSentence(query, qctx);
        CHECK(yieldSentence);
        return yieldSentence->yieldColumns();
    }

    Expression* getYieldFilter(const std::string& query, QueryContext* qctx) {
        auto yieldSentence = getYieldSentence(query, qctx);
        CHECK(yieldSentence);
        auto where = yieldSentence->where();
        CHECK(where);
        return where->filter();
    }

protected:
    std::unique_ptr<QueryContext>         qctx_;
    std::unique_ptr<Sentence>             sentences_;
};
}  // namespace graph
}  // namespace nebula
#endif  // EXECUTOR_QUERY_TESTQUERYBASE_H
