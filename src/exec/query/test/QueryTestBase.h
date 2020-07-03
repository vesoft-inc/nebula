/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef EXEC_QUERY_TESTQUERYBASE_H
#define EXEC_QUERY_TESTQUERYBASE_H
#include "common/base/Base.h"

#include <gtest/gtest.h>
#include "parser/GQLParser.h"

namespace nebula {
namespace graph {

class QueryTestBase : public testing::Test {
protected:
    void SetUp() override {
        qctx_ = std::make_unique<QueryContext>();

        {
            DataSet dataset({"_vid", "_stats", "_tag:person:name:age",
                             "_edge:+study:_dst:start_year:end_year", "_expr"});
            {
                Row row;
                // vid
                row.values.emplace_back("Ann");

                // _stats
                row.values.emplace_back(Value::kEmpty);

                // vertices props
                List vertices;
                vertices.values.emplace_back("Ann");
                vertices.values.emplace_back(18);
                row.values.emplace_back(vertices);

                // edge props
                List edges;
                List edge1;
                edge1.values.emplace_back("School1");
                edge1.values.emplace_back(2010);
                edge1.values.emplace_back(2014);
                edges.values.emplace_back(std::move(edge1));
                List edge2;
                edge2.values.emplace_back("School2");
                edge2.values.emplace_back(2014);
                edge2.values.emplace_back(2017);
                edges.values.emplace_back(std::move(edge2));
                row.values.emplace_back(edges);

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
                List vertices;
                vertices.values.emplace_back("Tom");
                vertices.values.emplace_back(18);
                row.values.emplace_back(vertices);
                // edges props
                List edge1;
                edge1.values.emplace_back("School1");
                edge1.values.emplace_back(2008);
                edge1.values.emplace_back(2012);

                List edge2;
                edge2.values.emplace_back("School2");
                edge2.values.emplace_back(2012);
                edge2.values.emplace_back(2015);

                List edges;
                edges.values.emplace_back(std::move(edge1));
                edges.values.emplace_back(std::move(edge2));

                row.values.emplace_back(edges);

                // _expr
                row.values.emplace_back(Value::kEmpty);
                dataset.emplace_back(std::move(row));
            }
            List datasetList;
            datasetList.values.emplace_back(std::move(dataset));
            qctx_->ectx()->setResult("input_neighbor",
                                     ExecResult::buildGetNeighbors(Value(datasetList)));
        }

        {
            DataSet dataset({"vid", "v_name", "v_age", "v_dst", "e_start_year", "e_end_year"});
            {
                Row row;
                row.values = {"Ann", "Ann", 18, "School1", 2010, 2014};
                dataset.emplace_back(row);
                row.values = {"Tom", "Tom", 20, "School1", 2008, 2012};
                dataset.emplace_back(row);
                row.values = {"Joy", "Joy", 20, "School2", 2008, 2012};
                dataset.emplace_back(row);
                row.values = {"Ann", "Ann", 18, "School1", 2010, 2014};
                dataset.emplace_back(row);
            }
            qctx_->ectx()->setResult("input_sequential",
                                     ExecResult::buildSequential(Value(dataset)));
        }
        {
            DataSet dataset({"_vid", "_stats", "_tag:person:name:age",
                             "_edge:+study:_dst:start_year:end_year", "_expr"});
            qctx_->ectx()->setResult("empty",
                                     ExecResult::buildSequential(Value(dataset)));
        }
    }

    YieldSentence* getYieldSentence(const std::string &query) {
        auto ret = GQLParser().parse(query);
        CHECK(ret.ok()) << ret.status();
        sentences_ = std::move(ret).value();
        auto sens = sentences_->sentences();
        CHECK_EQ(sens.size(), 1);
        CHECK_EQ(sens[0]->kind(), Sentence::Kind::kYield);
        return static_cast<YieldSentence*>(sens[0]);
    }

    YieldColumns* getYieldColumns(const std::string &query) {
        auto yieldSentence = getYieldSentence(query);
        CHECK(yieldSentence);
        return yieldSentence->yieldColumns();
    }

    std::vector<Expression*> getExprs(const YieldColumns *yieldColumns) {
        std::vector<Expression*> exprs;
        for (auto &col : yieldColumns->columns()) {
            exprs.emplace_back(col->expr());
        }
        return exprs;
    }

    Expression* getYieldFilter(const std::string &query) {
        auto yieldSentence = getYieldSentence(query);
        CHECK(yieldSentence);
        auto where = yieldSentence->where();
        CHECK(where);
        return where->filter();
    }


protected:
    std::unique_ptr<QueryContext>         qctx_;
    std::unique_ptr<SequentialSentences>  sentences_;
};
}  // namespace graph
}  // namespace nebula
#endif  // EXEC_QUERY_TESTQUERYBASE_H

