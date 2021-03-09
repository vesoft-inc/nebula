/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "planner/Logic.h"
#include "planner/Query.h"
#include "common/base/ObjectPool.h"
#include "validator/FetchVerticesValidator.h"
#include "validator/test/ValidatorTestBase.h"

namespace nebula {
namespace graph {

class FetchVerticesValidatorTest : public ValidatorTestBase {
protected:
    QueryContext *getQCtx(const std::string &query) {
        auto result = validate(query);
        EXPECT_TRUE(result.ok()) << result.status();
        return std::move(result).value();
    }
};

TEST_F(FetchVerticesValidatorTest, FetchVerticesProp) {
    auto src = std::make_unique<VariablePropertyExpression>(new std::string("_VARNAME_"),
                                                            new std::string("VertexID"));
    {
        auto qctx = getQCtx("FETCH PROP ON person \"1\"");

        auto *start = StartNode::make(qctx);

        auto tagIdResult = schemaMng_->toTagID(1, "person");
        ASSERT_TRUE(tagIdResult.ok());
        auto tagId = tagIdResult.value();
        storage::cpp2::VertexProp prop;
        prop.set_tag(tagId);
        auto *gv = GetVertices::make(
            qctx, start, 1, src.get(), std::vector<storage::cpp2::VertexProp>{std::move(prop)}, {});
        gv->setColNames({nebula::kVid, "person.name", "person.age"});
        // project
        auto yieldColumns = std::make_unique<YieldColumns>();
        yieldColumns->addColumn(
            new YieldColumn(new VertexExpression(), new std::string("vertices_")));
        auto *project = Project::make(qctx, gv, yieldColumns.get());
        project->setColNames({"vertices_"});
        auto result = Eq(qctx->plan()->root(), project);
        ASSERT_TRUE(result.ok()) << result;
    }
    // multi-tags
    {
        auto qctx = getQCtx("FETCH PROP ON person, book \"1\"");

        auto *start = StartNode::make(qctx);

        // person
        auto tagIdResult = schemaMng_->toTagID(1, "person");
        ASSERT_TRUE(tagIdResult.ok());
        auto tagId = tagIdResult.value();
        storage::cpp2::VertexProp personProp;
        personProp.set_tag(tagId);
        // book
        tagIdResult = schemaMng_->toTagID(1, "book");
        ASSERT_TRUE(tagIdResult.ok());
        tagId = tagIdResult.value();
        storage::cpp2::VertexProp bookProp;
        bookProp.set_tag(tagId);
        auto *gv = GetVertices::make(
            qctx,
            start,
            1,
            src.get(),
            std::vector<storage::cpp2::VertexProp>{std::move(personProp), std::move(bookProp)},
            {});
        gv->setColNames({nebula::kVid, "person.name", "person.age", "book.name"});
        // project
        auto yieldColumns = std::make_unique<YieldColumns>();
        yieldColumns->addColumn(
            new YieldColumn(new VertexExpression(), new std::string("vertices_")));
        auto *project = Project::make(qctx, gv, yieldColumns.get());
        project->setColNames({"vertices_"});
        auto result = Eq(qctx->plan()->root(), project);
        ASSERT_TRUE(result.ok()) << result;
    }
    // With YIELD
    {
        auto qctx = getQCtx("FETCH PROP ON person \"1\" YIELD person.name, person.age");

        auto *start = StartNode::make(qctx);

        auto tagIdResult = schemaMng_->toTagID(1, "person");
        ASSERT_TRUE(tagIdResult.ok());
        auto tagId = tagIdResult.value();
        storage::cpp2::VertexProp prop;
        prop.set_tag(tagId);
        prop.set_props(std::vector<std::string>{"name", "age"});
        storage::cpp2::Expr expr1;
        expr1.set_expr(
            TagPropertyExpression(new std::string("person"), new std::string("name")).encode());
        storage::cpp2::Expr expr2;
        expr2.set_expr(
            TagPropertyExpression(new std::string("person"), new std::string("age")).encode());
        auto *gv =
            GetVertices::make(qctx,
                              start,
                              1,
                              src.get(),
                              std::vector<storage::cpp2::VertexProp>{std::move(prop)},
                              std::vector<storage::cpp2::Expr>{std::move(expr1), std::move(expr2)});
        gv->setColNames({nebula::kVid, "person.name", "person.age"});

        // project
        auto yieldColumns = std::make_unique<YieldColumns>();
        yieldColumns->addColumn(
            new YieldColumn(new InputPropertyExpression(new std::string(nebula::kVid)),
                            new std::string("VertexID")));
        yieldColumns->addColumn(new YieldColumn(
            new TagPropertyExpression(new std::string("person"), new std::string("name"))));
        yieldColumns->addColumn(new YieldColumn(
            new TagPropertyExpression(new std::string("person"), new std::string("age"))));
        auto *project = Project::make(qctx, gv, yieldColumns.get());
        project->setColNames({"VertexID", "person.name", "person.age"});

        auto result = Eq(qctx->plan()->root(), project);
        ASSERT_TRUE(result.ok()) << result;
    }
    // multi-tags With YIELD
    {
        auto qctx =
            getQCtx("FETCH PROP ON person,book \"1\" YIELD person.name, person.age, book.name");

        auto *start = StartNode::make(qctx);

        // person
        auto tagIdResult = schemaMng_->toTagID(1, "person");
        ASSERT_TRUE(tagIdResult.ok());
        auto tagId = tagIdResult.value();
        storage::cpp2::VertexProp personProp;
        personProp.set_tag(tagId);
        personProp.set_props(std::vector<std::string>{"name", "age"});
        // book
        tagIdResult = schemaMng_->toTagID(1, "book");
        ASSERT_TRUE(tagIdResult.ok());
        tagId = tagIdResult.value();
        storage::cpp2::VertexProp bookProp;
        bookProp.set_tag(tagId);
        bookProp.set_props(std::vector<std::string>{"name"});
        storage::cpp2::Expr expr1;
        expr1.set_expr(
            TagPropertyExpression(new std::string("person"), new std::string("name")).encode());
        storage::cpp2::Expr expr2;
        expr2.set_expr(
            TagPropertyExpression(new std::string("person"), new std::string("age")).encode());
        storage::cpp2::Expr expr3;
        expr3.set_expr(
            TagPropertyExpression(new std::string("book"), new std::string("name")).encode());
        auto *gv = GetVertices::make(
            qctx,
            start,
            1,
            src.get(),
            std::vector<storage::cpp2::VertexProp>{std::move(personProp), std::move(bookProp)},
            std::vector<storage::cpp2::Expr>{std::move(expr1), std::move(expr2), std::move(expr3)});
        gv->setColNames({nebula::kVid, "person.name", "person.age", "book.name"});

        // project
        auto yieldColumns = std::make_unique<YieldColumns>();
        yieldColumns->addColumn(
            new YieldColumn(new InputPropertyExpression(new std::string(nebula::kVid)),
                            new std::string("VertexID")));
        yieldColumns->addColumn(new YieldColumn(
            new TagPropertyExpression(new std::string("person"), new std::string("name"))));
        yieldColumns->addColumn(new YieldColumn(
            new TagPropertyExpression(new std::string("person"), new std::string("age"))));
        yieldColumns->addColumn(new YieldColumn(
            new TagPropertyExpression(new std::string("book"), new std::string("name"))));
        auto *project = Project::make(qctx, gv, yieldColumns.get());
        project->setColNames({"VertexID", "person.name", "person.age", "book.name"});

        auto result = Eq(qctx->plan()->root(), project);
        ASSERT_TRUE(result.ok()) << result;
    }
    // With YIELD const expression
    {
        auto qctx = getQCtx("FETCH PROP ON person \"1\" YIELD person.name, 1 > 1, person.age");

        auto *start = StartNode::make(qctx);

        // get vertices
        auto tagIdResult = schemaMng_->toTagID(1, "person");
        ASSERT_TRUE(tagIdResult.ok());
        auto tagId = tagIdResult.value();
        storage::cpp2::VertexProp prop;
        prop.set_tag(tagId);
        prop.set_props(std::vector<std::string>{"name", "age"});
        storage::cpp2::Expr expr1;
        expr1.set_expr(
            TagPropertyExpression(new std::string("person"), new std::string("name")).encode());
        storage::cpp2::Expr expr2;
        expr2.set_expr(
            TagPropertyExpression(new std::string("person"), new std::string("age")).encode());
        auto *gv =
            GetVertices::make(qctx,
                              start,
                              1,
                              src.get(),
                              std::vector<storage::cpp2::VertexProp>{std::move(prop)},
                              std::vector<storage::cpp2::Expr>{std::move(expr1), std::move(expr2)});
        gv->setColNames({nebula::kVid, "person.name", "person.age"});

        // project
        auto yieldColumns = std::make_unique<YieldColumns>();
        yieldColumns->addColumn(
            new YieldColumn(new InputPropertyExpression(new std::string(nebula::kVid)),
                            new std::string("VertexID")));
        yieldColumns->addColumn(new YieldColumn(
            new TagPropertyExpression(new std::string("person"), new std::string("name"))));
        yieldColumns->addColumn(new YieldColumn(new RelationalExpression(
            Expression::Kind::kRelGT, new ConstantExpression(1), new ConstantExpression(1))));
        yieldColumns->addColumn(new YieldColumn(
            new TagPropertyExpression(new std::string("person"), new std::string("age"))));
        auto *project = Project::make(qctx, gv, yieldColumns.get());
        project->setColNames({"VertexID", "person.name", "(1>1)", "person.age"});

        auto result = Eq(qctx->plan()->root(), project);
        ASSERT_TRUE(result.ok()) << result;
    }
    // multi-tags With YIELD const expression
    {
        auto qctx = getQCtx(
            "FETCH PROP ON person, book \"1\" YIELD person.name, 1 > 1, book.name, person.age");

        auto *start = StartNode::make(qctx);

        // get vertices
        // person
        auto tagIdResult = schemaMng_->toTagID(1, "person");
        ASSERT_TRUE(tagIdResult.ok());
        auto tagId = tagIdResult.value();
        storage::cpp2::VertexProp personProp;
        personProp.set_tag(tagId);
        personProp.set_props(std::vector<std::string>{"name", "age"});
        // book
        tagIdResult = schemaMng_->toTagID(1, "book");
        ASSERT_TRUE(tagIdResult.ok());
        tagId = tagIdResult.value();
        storage::cpp2::VertexProp bookProp;
        bookProp.set_tag(tagId);
        bookProp.set_props(std::vector<std::string>{"name"});

        storage::cpp2::Expr expr1;
        expr1.set_expr(
            TagPropertyExpression(new std::string("person"), new std::string("name")).encode());
        storage::cpp2::Expr expr2;
        expr2.set_expr(
            TagPropertyExpression(new std::string("book"), new std::string("name")).encode());
        storage::cpp2::Expr expr3;
        expr3.set_expr(
            TagPropertyExpression(new std::string("person"), new std::string("age")).encode());
        auto *gv = GetVertices::make(
            qctx,
            start,
            1,
            src.get(),
            std::vector<storage::cpp2::VertexProp>{std::move(personProp), std::move(bookProp)},
            std::vector<storage::cpp2::Expr>{std::move(expr1), std::move(expr2), std::move(expr3)});
        gv->setColNames({nebula::kVid, "person.name", "book.name", "person.age"});

        // project
        auto yieldColumns = std::make_unique<YieldColumns>();
        yieldColumns->addColumn(
            new YieldColumn(new InputPropertyExpression(new std::string(nebula::kVid)),
                            new std::string("VertexID")));
        yieldColumns->addColumn(new YieldColumn(
            new TagPropertyExpression(new std::string("person"), new std::string("name"))));
        yieldColumns->addColumn(new YieldColumn(new RelationalExpression(
            Expression::Kind::kRelGT, new ConstantExpression(1), new ConstantExpression(1))));
        yieldColumns->addColumn(new YieldColumn(
            new TagPropertyExpression(new std::string("book"), new std::string("name"))));
        yieldColumns->addColumn(new YieldColumn(
            new TagPropertyExpression(new std::string("person"), new std::string("age"))));
        auto *project = Project::make(qctx, gv, yieldColumns.get());
        project->setColNames({"VertexID", "person.name", "(1>1)", "book.name", "person.age"});

        auto result = Eq(qctx->plan()->root(), project);
        ASSERT_TRUE(result.ok()) << result;
    }
    // With YIELD combine properties
    {
        auto qctx = getQCtx("FETCH PROP ON person \"1\" YIELD person.name + person.age");

        auto *start = StartNode::make(qctx);

        auto tagIdResult = schemaMng_->toTagID(1, "person");
        ASSERT_TRUE(tagIdResult.ok());
        auto tagId = tagIdResult.value();
        storage::cpp2::VertexProp prop;
        prop.set_tag(tagId);
        prop.set_props(std::vector<std::string>{"name", "age"});
        storage::cpp2::Expr expr1;
        expr1.set_expr(
            ArithmeticExpression(
                Expression::Kind::kAdd,
                new TagPropertyExpression(new std::string("person"), new std::string("name")),
                new TagPropertyExpression(new std::string("person"), new std::string("age")))
                .encode());

        auto *gv = GetVertices::make(qctx,
                                     start,
                                     1,
                                     src.get(),
                                     std::vector<storage::cpp2::VertexProp>{std::move(prop)},
                                     std::vector<storage::cpp2::Expr>{std::move(expr1)});
        gv->setColNames({nebula::kVid, "person.name", "person.age"});

        // project, TODO(shylock) could push down to storage is it supported
        auto yieldColumns = std::make_unique<YieldColumns>();
        yieldColumns->addColumn(
            new YieldColumn(new InputPropertyExpression(new std::string(nebula::kVid)),
                            new std::string("VertexID")));
        yieldColumns->addColumn(new YieldColumn(new ArithmeticExpression(
            Expression::Kind::kAdd,
            new TagPropertyExpression(new std::string("person"), new std::string("name")),
            new TagPropertyExpression(new std::string("person"), new std::string("age")))));
        auto *project = Project::make(qctx, gv, yieldColumns.get());
        project->setColNames({"VertexID", "(person.name+person.age)"});

        auto result = Eq(qctx->plan()->root(), project);
        ASSERT_TRUE(result.ok()) << result;
    }
    // multi-tags With YIELD combine properties
    {
        auto qctx = getQCtx("FETCH PROP ON book,person \"1\" YIELD person.name + book.name");

        auto *start = StartNode::make(qctx);

        // person
        auto tagIdResult = schemaMng_->toTagID(1, "person");
        ASSERT_TRUE(tagIdResult.ok());
        auto tagId = tagIdResult.value();
        storage::cpp2::VertexProp personProp;
        personProp.set_tag(tagId);
        personProp.set_props(std::vector<std::string>{"name"});
        // book
        tagIdResult = schemaMng_->toTagID(1, "book");
        ASSERT_TRUE(tagIdResult.ok());
        tagId = tagIdResult.value();
        storage::cpp2::VertexProp bookProp;
        bookProp.set_tag(tagId);
        bookProp.set_props(std::vector<std::string>{"name"});

        storage::cpp2::Expr expr1;
        expr1.set_expr(
            ArithmeticExpression(
                Expression::Kind::kAdd,
                new TagPropertyExpression(new std::string("person"), new std::string("name")),
                new TagPropertyExpression(new std::string("book"), new std::string("name")))
                .encode());

        auto *gv = GetVertices::make(
            qctx,
            start,
            1,
            src.get(),
            std::vector<storage::cpp2::VertexProp>{std::move(personProp), std::move(bookProp)},
            std::vector<storage::cpp2::Expr>{std::move(expr1)});
        gv->setColNames({nebula::kVid, "person.name", "book.name"});

        // project, TODO(shylock) could push down to storage is it supported
        auto yieldColumns = std::make_unique<YieldColumns>();
        yieldColumns->addColumn(
            new YieldColumn(new InputPropertyExpression(new std::string(nebula::kVid)),
                            new std::string("VertexID")));
        yieldColumns->addColumn(new YieldColumn(new ArithmeticExpression(
            Expression::Kind::kAdd,
            new TagPropertyExpression(new std::string("person"), new std::string("name")),
            new TagPropertyExpression(new std::string("book"), new std::string("name")))));
        auto *project = Project::make(qctx, gv, yieldColumns.get());
        project->setColNames({"VertexID", "(person.name+book.name)"});

        auto result = Eq(qctx->plan()->root(), project);
        ASSERT_TRUE(result.ok()) << result;
    }
    // With YIELD distinct
    {
        auto qctx = getQCtx("FETCH PROP ON person \"1\" YIELD distinct person.name, person.age");

        auto *start = StartNode::make(qctx);

        auto tagIdResult = schemaMng_->toTagID(1, "person");
        ASSERT_TRUE(tagIdResult.ok());
        auto tagId = tagIdResult.value();
        storage::cpp2::VertexProp prop;
        prop.set_tag(tagId);
        prop.set_props(std::vector<std::string>{"name", "age"});
        storage::cpp2::Expr expr1;
        expr1.set_expr(
            TagPropertyExpression(new std::string("person"), new std::string("name")).encode());
        storage::cpp2::Expr expr2;
        expr2.set_expr(
            TagPropertyExpression(new std::string("person"), new std::string("age")).encode());
        auto *gv =
            GetVertices::make(qctx,
                              start,
                              1,
                              src.get(),
                              std::vector<storage::cpp2::VertexProp>{std::move(prop)},
                              std::vector<storage::cpp2::Expr>{std::move(expr1), std::move(expr2)});

        std::vector<std::string> colNames{"VertexID", "person.name", "person.age"};
        gv->setColNames(colNames);

        // project
        auto yieldColumns = std::make_unique<YieldColumns>();
        yieldColumns->addColumn(
            new YieldColumn(new InputPropertyExpression(new std::string(nebula::kVid)),
                            new std::string("VertexID")));
        yieldColumns->addColumn(new YieldColumn(
            new TagPropertyExpression(new std::string("person"), new std::string("name"))));
        yieldColumns->addColumn(new YieldColumn(
            new TagPropertyExpression(new std::string("person"), new std::string("age"))));
        auto *project = Project::make(qctx, gv, yieldColumns.get());
        project->setColNames(colNames);

        // dedup
        auto *dedup = Dedup::make(qctx, project);
        dedup->setColNames(colNames);

        // data collect
        auto *dataCollect = DataCollect::make(
            qctx, dedup, DataCollect::CollectKind::kRowBasedMove, {dedup->outputVar()});
        dataCollect->setColNames(colNames);

        auto result = Eq(qctx->plan()->root(), dataCollect);
        ASSERT_TRUE(result.ok()) << result;
    }
    // ON *
    {
        auto qctx = getQCtx("FETCH PROP ON * \"1\"");

        auto *start = StartNode::make(qctx);

        auto *gv = GetVertices::make(qctx, start, 1, src.get(), {}, {});
        gv->setColNames({nebula::kVid, "person.name", "person.age"});
        // project
        auto yieldColumns = std::make_unique<YieldColumns>();
        yieldColumns->addColumn(
            new YieldColumn(new VertexExpression(), new std::string("vertices_")));
        auto *project = Project::make(qctx, gv, yieldColumns.get());
        project->setColNames({"vertices_"});
        auto result = Eq(qctx->plan()->root(), project);
        ASSERT_TRUE(result.ok()) << result;
    }
    {
        auto qctx = getQCtx("FETCH PROP ON * \"1\", \"2\"");

        auto *start = StartNode::make(qctx);

        auto *gv = GetVertices::make(qctx, start, 1, src.get(), {}, {});
        gv->setColNames({nebula::kVid, "person.name", "person.age"});
        // project
        auto yieldColumns = std::make_unique<YieldColumns>();
        yieldColumns->addColumn(
            new YieldColumn(new VertexExpression(), new std::string("vertices_")));
        auto *project = Project::make(qctx, gv, yieldColumns.get());
        project->setColNames({"vertices_"});
        auto result = Eq(qctx->plan()->root(), project);
        ASSERT_TRUE(result.ok()) << result;
    }
    // ON * with yield
    {
        auto qctx = getQCtx("FETCH PROP ON * \"1\", \"2\" YIELD person.name");

        auto *start = StartNode::make(qctx);

        std::vector<std::string> colNames{"VertexID", "person.name"};
        // Get vertices
        auto *gv = GetVertices::make(qctx, start, 1, src.get(), {}, {});
        gv->setColNames(colNames);

        // project
        auto yieldColumns = std::make_unique<YieldColumns>();
        yieldColumns->addColumn(
            new YieldColumn(new InputPropertyExpression(new std::string(nebula::kVid)),
                            new std::string("VertexID")));
        yieldColumns->addColumn(new YieldColumn(
            new TagPropertyExpression(new std::string("person"), new std::string("name"))));
        auto *project = Project::make(qctx, gv, yieldColumns.get());
        project->setColNames(colNames);

        auto result = Eq(qctx->plan()->root(), project);
        ASSERT_TRUE(result.ok()) << result;
    }
    {
        auto qctx = getQCtx("FETCH PROP ON * \"1\", \"2\" YIELD person.name, person.age");

        auto *start = StartNode::make(qctx);

        std::vector<std::string> colNames{"VertexID", "person.name", "person.age"};
        // Get vertices
        auto *gv = GetVertices::make(qctx, start, 1, src.get(), {}, {});
        gv->setColNames(colNames);

        // project
        auto yieldColumns = std::make_unique<YieldColumns>();
        yieldColumns->addColumn(
            new YieldColumn(new InputPropertyExpression(new std::string(nebula::kVid)),
                            new std::string("VertexID")));
        yieldColumns->addColumn(new YieldColumn(
            new TagPropertyExpression(new std::string("person"), new std::string("name"))));
        yieldColumns->addColumn(new YieldColumn(
            new TagPropertyExpression(new std::string("person"), new std::string("age"))));
        auto *project = Project::make(qctx, gv, yieldColumns.get());
        project->setColNames(colNames);

        auto result = Eq(qctx->plan()->root(), project);
        ASSERT_TRUE(result.ok()) << result;
    }
    {
        auto qctx = getQCtx("FETCH PROP ON * \"1\", \"2\" YIELD 1+1, person.name, person.age");

        auto *start = StartNode::make(qctx);

        // Get vertices
        auto *gv = GetVertices::make(qctx, start, 1, src.get(), {}, {});
        gv->setColNames({nebula::kVid, "person.name", "person.age"});

        // project
        auto yieldColumns = std::make_unique<YieldColumns>();
        yieldColumns->addColumn(
            new YieldColumn(new InputPropertyExpression(new std::string(nebula::kVid)),
                            new std::string("VertexID")));
        yieldColumns->addColumn(new YieldColumn(new ArithmeticExpression(
            Expression::Kind::kAdd, new ConstantExpression(1), new ConstantExpression(1))));
        yieldColumns->addColumn(new YieldColumn(
            new TagPropertyExpression(new std::string("person"), new std::string("name"))));
        yieldColumns->addColumn(new YieldColumn(
            new TagPropertyExpression(new std::string("person"), new std::string("age"))));
        auto *project = Project::make(qctx, gv, yieldColumns.get());
        project->setColNames({"VertexID", "(1+1)", "person.name", "person.age"});

        auto result = Eq(qctx->plan()->root(), project);
        ASSERT_TRUE(result.ok()) << result;
    }
}

TEST_F(FetchVerticesValidatorTest, FetchVerticesInputOutput) {
    // pipe
    {
        const std::string query = "FETCH PROP ON person \"1\" YIELD person.name AS name"
                                  " | FETCH PROP ON person $-.name";
        EXPECT_TRUE(checkResult(query,
                                {
                                    PlanNode::Kind::kProject,
                                    PlanNode::Kind::kGetVertices,
                                    PlanNode::Kind::kProject,
                                    PlanNode::Kind::kGetVertices,
                                    PlanNode::Kind::kStart,
                                }));
    }
    {
        // with multi-tags
        const std::string query = "FETCH PROP ON person \"1\" YIELD person.name AS name"
                                  " | FETCH PROP ON person, book $-.name";
        EXPECT_TRUE(checkResult(query,
                                {
                                    PlanNode::Kind::kProject,
                                    PlanNode::Kind::kGetVertices,
                                    PlanNode::Kind::kProject,
                                    PlanNode::Kind::kGetVertices,
                                    PlanNode::Kind::kStart,
                                }));
    }
    // Variable
    {
        const std::string query = "$a = FETCH PROP ON person \"1\" YIELD person.name AS name;"
                                  "FETCH PROP ON person $a.name";
        EXPECT_TRUE(checkResult(query,
                                {
                                    PlanNode::Kind::kProject,
                                    PlanNode::Kind::kGetVertices,
                                    PlanNode::Kind::kProject,
                                    PlanNode::Kind::kGetVertices,
                                    PlanNode::Kind::kStart,
                                }));
    }
    {
        // with multi-tags
        const std::string query = "$a = FETCH PROP ON person \"1\" YIELD person.name AS name;"
                                  "FETCH PROP ON book,person $a.name";
        EXPECT_TRUE(checkResult(query,
                                {
                                    PlanNode::Kind::kProject,
                                    PlanNode::Kind::kGetVertices,
                                    PlanNode::Kind::kProject,
                                    PlanNode::Kind::kGetVertices,
                                    PlanNode::Kind::kStart,
                                }));
    }

    // with project
    // pipe
    {
        const std::string query = "FETCH PROP ON person \"1\" YIELD person.name + 1 AS name"
                                  " | FETCH PROP ON person $-.name YIELD person.name + 1";
        EXPECT_TRUE(checkResult(query,
                                {
                                    PlanNode::Kind::kProject,
                                    PlanNode::Kind::kGetVertices,
                                    PlanNode::Kind::kProject,
                                    PlanNode::Kind::kGetVertices,
                                    PlanNode::Kind::kStart,
                                }));
    }
    {
        // with multi-tags
        const std::string query =
            "FETCH PROP ON person \"1\" YIELD person.name + 1 AS name"
            " | FETCH PROP ON person,book $-.name YIELD person.name + 1, book.name";
        EXPECT_TRUE(checkResult(query,
                                {
                                    PlanNode::Kind::kProject,
                                    PlanNode::Kind::kGetVertices,
                                    PlanNode::Kind::kProject,
                                    PlanNode::Kind::kGetVertices,
                                    PlanNode::Kind::kStart,
                                }));
    }
    // Variable
    {
        const std::string query = "$a = FETCH PROP ON person \"1\" YIELD person.name + 1 AS name;"
                                  "FETCH PROP ON person $a.name YIELD person.name + 1 ";
        EXPECT_TRUE(checkResult(query,
                                {
                                    PlanNode::Kind::kProject,
                                    PlanNode::Kind::kGetVertices,
                                    PlanNode::Kind::kProject,
                                    PlanNode::Kind::kGetVertices,
                                    PlanNode::Kind::kStart,
                                }));
    }
    {
        // with multi-tags
        const std::string query =
            "$a = FETCH PROP ON person \"1\" YIELD person.name + 1 AS name;"
            "FETCH PROP ON book,person $a.name YIELD person.name + 1, book.name ";
        EXPECT_TRUE(checkResult(query,
                                {
                                    PlanNode::Kind::kProject,
                                    PlanNode::Kind::kGetVertices,
                                    PlanNode::Kind::kProject,
                                    PlanNode::Kind::kGetVertices,
                                    PlanNode::Kind::kStart,
                                }));
    }
    // on *
    {
        const std::string query = "FETCH PROP ON person \"1\", \"2\" YIELD person.name AS name"
                                  "| FETCH PROP ON * $-.name";
        EXPECT_TRUE(checkResult(query,
                                {
                                    PlanNode::Kind::kProject,
                                    PlanNode::Kind::kGetVertices,
                                    PlanNode::Kind::kProject,
                                    PlanNode::Kind::kGetVertices,
                                    PlanNode::Kind::kStart,
                                }));
    }
    {
        const std::string query =
            "$a = FETCH PROP ON person \"1\", \"2\" YIELD person.name AS name;"
            "FETCH PROP ON * $a.name";
        EXPECT_TRUE(checkResult(query,
                                {
                                    PlanNode::Kind::kProject,
                                    PlanNode::Kind::kGetVertices,
                                    PlanNode::Kind::kProject,
                                    PlanNode::Kind::kGetVertices,
                                    PlanNode::Kind::kStart,
                                }));
    }
    // on * with yield
    {
        const std::string query = "FETCH PROP ON * \"1\", \"2\" YIELD person.name AS name"
                                  "| FETCH PROP ON * $-.name";
        EXPECT_TRUE(checkResult(query,
                                {
                                    PlanNode::Kind::kProject,
                                    PlanNode::Kind::kGetVertices,
                                    PlanNode::Kind::kProject,
                                    PlanNode::Kind::kGetVertices,
                                    PlanNode::Kind::kStart,
                                }));
    }
    {
        const std::string query = "$a = FETCH PROP ON * \"1\", \"2\" YIELD person.name AS name;"
                                  "FETCH PROP ON * $a.name";
        EXPECT_TRUE(checkResult(query,
                                {
                                    PlanNode::Kind::kProject,
                                    PlanNode::Kind::kGetVertices,
                                    PlanNode::Kind::kProject,
                                    PlanNode::Kind::kGetVertices,
                                    PlanNode::Kind::kStart,
                                }));
    }
}

TEST_F(FetchVerticesValidatorTest, FetchVerticesPropFailed) {
    // mismatched tag
    ASSERT_FALSE(validate("FETCH PROP ON person \"1\" YIELD tag2.prop2"));
    ASSERT_FALSE(validate("FETCH PROP ON person, book \"1\" YIELD tag3.prop3"));

    // not exist tag
    ASSERT_FALSE(validate("FETCH PROP ON not_exist_tag \"1\" YIELD not_exist_tag.prop1"));
    ASSERT_FALSE(validate("FETCH PROP ON person, not_exist_tag \"1\""
                          " YIELD not_exist_tag.prop1, person.name"));
    ASSERT_FALSE(validate("FETCH PROP ON * \"1\" YIELD not_exist_tag.prop1"));

    // not exist property
    ASSERT_FALSE(validate("FETCH PROP ON person \"1\" YIELD person.not_exist_property"));
    ASSERT_FALSE(validate("FETCH PROP ON person, book \"1\""
                          " YIELD person.not_exist_property, book.not_exist_property"));
    ASSERT_FALSE(validate("FETCH PROP ON * \"1\" YIELD person.not_exist_property"));

    // invalid yield expression
    ASSERT_FALSE(validate("$a = FETCH PROP ON person \"1\" YIELD person.name AS name;"
                          " FETCH PROP ON person \"1\" YIELD $a.name + 1"));

    ASSERT_FALSE(validate("FETCH PROP ON person \"1\" YIELD $^.person.name"));
    ASSERT_FALSE(validate("FETCH PROP ON person \"1\" YIELD $$.person.name"));
    ASSERT_FALSE(validate("FETCH PROP ON person \"1\" YIELD person.name AS name | "
                          " FETCH PROP ON person \"1\" YIELD $-.name + 1"));
    ASSERT_FALSE(validate("FETCH PROP ON person \"1\" YIELD person._src + 1"));
    ASSERT_FALSE(validate("FETCH PROP ON person \"1\" YIELD person._type"));
    ASSERT_FALSE(validate("FETCH PROP ON person \"1\" YIELD person._rank + 1"));
    ASSERT_FALSE(validate("FETCH PROP ON person \"1\" YIELD person._dst + 1"));

    // invalid yield expression on *
    ASSERT_FALSE(validate("$a = FETCH PROP ON * \"1\" YIELD person.name AS name;"
                          " FETCH PROP ON * \"1\" YIELD $a.name + 1"));

    ASSERT_FALSE(validate("FETCH PROP ON * \"1\" YIELD $^.person.name"));
    ASSERT_FALSE(validate("FETCH PROP ON * \"1\" YIELD $$.person.name"));
    ASSERT_FALSE(validate("FETCH PROP ON * \"1\" YIELD person.name AS name | "
                          " FETCH PROP ON * \"1\" YIELD $-.name + 1"));
    ASSERT_FALSE(validate("FETCH PROP ON * \"1\" YIELD person._src + 1"));
    ASSERT_FALSE(validate("FETCH PROP ON * \"1\" YIELD person._type"));
    ASSERT_FALSE(validate("FETCH PROP ON * \"1\" YIELD person._rank + 1"));
    ASSERT_FALSE(validate("FETCH PROP ON * \"1\" YIELD person._dst + 1"));
}

TEST_F(FetchVerticesValidatorTest, FetchVerticesInputFailed) {
    // mismatched varirable
    ASSERT_FALSE(validate("$a = FETCH PROP ON person \"1\" YIELD person.name AS name;"
                          "FETCH PROP ON person $b.name"));
    ASSERT_FALSE(validate("$a = FETCH PROP ON * \"1\" YIELD person.name AS name;"
                          "FETCH PROP * person $b.name"));

    // mismatched varirable property
    ASSERT_FALSE(validate("$a = FETCH PROP ON person \"1\" YIELD person.name AS name;"
                          "FETCH PROP ON person $a.not_exist_property"));
    ASSERT_FALSE(validate("$a = FETCH PROP * person \"1\" YIELD person.name AS name;"
                          "FETCH PROP * person $a.not_exist_property"));

    // mismatched input property
    ASSERT_FALSE(validate("FETCH PROP ON person \"1\" YIELD person.name AS name | "
                          "FETCH PROP ON person $-.not_exist_property"));
    ASSERT_FALSE(validate("FETCH PROP * person \"1\" YIELD person.name AS name | "
                          "FETCH PROP * person $-.not_exist_property"));
}

}   // namespace graph
}   // namespace nebula
