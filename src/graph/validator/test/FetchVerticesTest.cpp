/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#include "common/base/ObjectPool.h"
#include "graph/planner/plan/Logic.h"
#include "graph/planner/plan/Query.h"
#include "graph/validator/FetchVerticesValidator.h"
#include "graph/validator/test/ValidatorTestBase.h"

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
  auto src = ColumnExpression::make(pool_.get(), 0);
  {
    auto qctx = getQCtx("FETCH PROP ON person \"1\" YIELD vertex as node");
    auto *pool = qctx->objPool();
    auto *start = StartNode::make(qctx);

    auto tagIdResult = schemaMng_->toTagID(1, "person");
    ASSERT_TRUE(tagIdResult.ok());
    auto tagId = tagIdResult.value();
    storage::cpp2::VertexProp prop;
    prop.tag_ref() = tagId;
    auto props = std::make_unique<std::vector<storage::cpp2::VertexProp>>();
    props->emplace_back(std::move(prop));
    auto *gv = GetVertices::make(qctx, start, 1, src, std::move(props));
    gv->setColNames({nebula::kVid, "person.name", "person.age"});
    // project
    auto yieldColumns = std::make_unique<YieldColumns>();
    yieldColumns->addColumn(new YieldColumn(VertexExpression::make(pool), "node"));
    auto *project = Project::make(qctx, gv, yieldColumns.get());
    auto result = Eq(qctx->plan()->root(), project);
    ASSERT_TRUE(result.ok()) << result;
  }
  // multi-tags
  {
    auto qctx = getQCtx("FETCH PROP ON person, book \"1\" YIELD vertex as node");
    auto *pool = qctx->objPool();
    auto *start = StartNode::make(qctx);

    // person
    auto tagIdResult = schemaMng_->toTagID(1, "person");
    ASSERT_TRUE(tagIdResult.ok());
    auto tagId = tagIdResult.value();
    storage::cpp2::VertexProp personProp;
    personProp.tag_ref() = tagId;
    // book
    tagIdResult = schemaMng_->toTagID(1, "book");
    ASSERT_TRUE(tagIdResult.ok());
    tagId = tagIdResult.value();
    storage::cpp2::VertexProp bookProp;
    bookProp.tag_ref() = tagId;
    auto props = std::make_unique<std::vector<storage::cpp2::VertexProp>>();
    props->emplace_back(std::move(personProp));
    props->emplace_back(std::move(bookProp));
    auto *gv = GetVertices::make(qctx, start, 1, src, std::move(props));
    gv->setColNames({nebula::kVid, "person.name", "person.age", "book.name"});
    // project
    auto yieldColumns = std::make_unique<YieldColumns>();
    yieldColumns->addColumn(new YieldColumn(VertexExpression::make(pool), "node"));
    auto *project = Project::make(qctx, gv, yieldColumns.get());
    auto result = Eq(qctx->plan()->root(), project);
    ASSERT_TRUE(result.ok()) << result;
  }
  // With YIELD
  {
    auto qctx = getQCtx("FETCH PROP ON person \"1\" YIELD person.name, person.age");
    auto *pool = qctx->objPool();
    auto *start = StartNode::make(qctx);

    auto tagIdResult = schemaMng_->toTagID(1, "person");
    ASSERT_TRUE(tagIdResult.ok());
    auto tagId = tagIdResult.value();
    storage::cpp2::VertexProp prop;
    prop.tag_ref() = tagId;
    prop.props_ref() = std::vector<std::string>{"name", "age"};
    auto props = std::make_unique<std::vector<storage::cpp2::VertexProp>>();
    props->emplace_back(std::move(prop));
    storage::cpp2::Expr expr1;
    expr1.expr_ref() = TagPropertyExpression::make(pool, "person", "name")->encode();
    storage::cpp2::Expr expr2;
    expr2.expr_ref() = TagPropertyExpression::make(pool, "person", "age")->encode();
    auto exprs = std::make_unique<std::vector<storage::cpp2::Expr>>();
    exprs->emplace_back(std::move(expr1));
    exprs->emplace_back(std::move(expr2));
    auto *gv = GetVertices::make(qctx, start, 1, src, std::move(props), std::move(exprs));
    gv->setColNames({nebula::kVid, "person.name", "person.age"});

    // project
    auto yieldColumns = std::make_unique<YieldColumns>();
    yieldColumns->addColumn(new YieldColumn(TagPropertyExpression::make(pool, "person", "name")));
    yieldColumns->addColumn(new YieldColumn(TagPropertyExpression::make(pool, "person", "age")));
    auto *project = Project::make(qctx, gv, yieldColumns.get());
    project->setColNames({"person.name", "person.age"});

    auto result = Eq(qctx->plan()->root(), project);
    ASSERT_TRUE(result.ok()) << result;
  }
  // multi-tags With YIELD
  {
    auto qctx = getQCtx(
        "FETCH PROP ON person,book \"1\" YIELD person.name, person.age, "
        "book.name");
    auto *pool = qctx->objPool();
    auto *start = StartNode::make(qctx);

    // person
    auto tagIdResult = schemaMng_->toTagID(1, "person");
    ASSERT_TRUE(tagIdResult.ok());
    auto tagId = tagIdResult.value();
    storage::cpp2::VertexProp personProp;
    personProp.tag_ref() = tagId;
    personProp.props_ref() = std::vector<std::string>{"name", "age"};
    // book
    tagIdResult = schemaMng_->toTagID(1, "book");
    ASSERT_TRUE(tagIdResult.ok());
    tagId = tagIdResult.value();
    storage::cpp2::VertexProp bookProp;
    bookProp.tag_ref() = tagId;
    bookProp.props_ref() = std::vector<std::string>{"name"};
    auto props = std::make_unique<std::vector<storage::cpp2::VertexProp>>();
    props->emplace_back(std::move(personProp));
    props->emplace_back(std::move(bookProp));
    storage::cpp2::Expr expr1;
    expr1.expr_ref() = TagPropertyExpression::make(pool, "person", "name")->encode();
    storage::cpp2::Expr expr2;
    expr2.expr_ref() = TagPropertyExpression::make(pool, "person", "age")->encode();
    storage::cpp2::Expr expr3;
    expr3.expr_ref() = TagPropertyExpression::make(pool, "book", "name")->encode();
    auto exprs = std::make_unique<std::vector<storage::cpp2::Expr>>();
    exprs->emplace_back(std::move(expr1));
    exprs->emplace_back(std::move(expr2));
    exprs->emplace_back(std::move(expr3));
    auto *gv = GetVertices::make(qctx, start, 1, src, std::move(props), std::move(exprs));
    gv->setColNames({nebula::kVid, "person.name", "person.age", "book.name"});

    // project
    auto yieldColumns = std::make_unique<YieldColumns>();
    yieldColumns->addColumn(new YieldColumn(TagPropertyExpression::make(pool, "person", "name")));
    yieldColumns->addColumn(new YieldColumn(TagPropertyExpression::make(pool, "person", "age")));
    yieldColumns->addColumn(new YieldColumn(TagPropertyExpression::make(pool, "book", "name")));
    auto *project = Project::make(qctx, gv, yieldColumns.get());
    project->setColNames({"person.name", "person.age", "book.name"});

    auto result = Eq(qctx->plan()->root(), project);
    ASSERT_TRUE(result.ok()) << result;
  }
  // With YIELD const expression
  {
    auto qctx = getQCtx("FETCH PROP ON person \"1\" YIELD person.name, 1 > 1, person.age");
    auto *pool = qctx->objPool();
    auto *start = StartNode::make(qctx);

    // get vertices
    auto tagIdResult = schemaMng_->toTagID(1, "person");
    ASSERT_TRUE(tagIdResult.ok());
    auto tagId = tagIdResult.value();
    storage::cpp2::VertexProp prop;
    prop.tag_ref() = tagId;
    prop.props_ref() = std::vector<std::string>{"name", "age"};
    auto props = std::make_unique<std::vector<storage::cpp2::VertexProp>>();
    props->emplace_back(std::move(prop));
    storage::cpp2::Expr expr1;
    expr1.expr_ref() = TagPropertyExpression::make(pool, "person", "name")->encode();
    storage::cpp2::Expr expr2;
    expr2.expr_ref() = TagPropertyExpression::make(pool, "person", "age")->encode();
    auto exprs = std::make_unique<std::vector<storage::cpp2::Expr>>();
    exprs->emplace_back(std::move(expr1));
    exprs->emplace_back(std::move(expr2));
    auto *gv = GetVertices::make(qctx, start, 1, src, std::move(props), std::move(exprs));
    gv->setColNames({nebula::kVid, "person.name", "person.age"});

    // project
    auto yieldColumns = std::make_unique<YieldColumns>();
    yieldColumns->addColumn(new YieldColumn(TagPropertyExpression::make(pool, "person", "name")));
    yieldColumns->addColumn(new YieldColumn(RelationalExpression::makeGT(
        pool, ConstantExpression::make(pool, 1), ConstantExpression::make(pool, 1))));
    yieldColumns->addColumn(new YieldColumn(TagPropertyExpression::make(pool, "person", "age")));
    auto *project = Project::make(qctx, gv, yieldColumns.get());
    project->setColNames({"person.name", "(1>1)", "person.age"});

    auto result = Eq(qctx->plan()->root(), project);
    ASSERT_TRUE(result.ok()) << result;
  }
  // multi-tags With YIELD const expression
  {
    auto qctx = getQCtx(
        "FETCH PROP ON person, book \"1\" YIELD person.name, 1 > 1, book.name, "
        "person.age");
    auto *pool = qctx->objPool();
    auto *start = StartNode::make(qctx);

    // get vertices
    // person
    auto tagIdResult = schemaMng_->toTagID(1, "person");
    ASSERT_TRUE(tagIdResult.ok());
    auto tagId = tagIdResult.value();
    storage::cpp2::VertexProp personProp;
    personProp.tag_ref() = tagId;
    personProp.props_ref() = std::vector<std::string>{"name", "age"};
    // book
    tagIdResult = schemaMng_->toTagID(1, "book");
    ASSERT_TRUE(tagIdResult.ok());
    tagId = tagIdResult.value();
    storage::cpp2::VertexProp bookProp;
    bookProp.tag_ref() = tagId;
    bookProp.props_ref() = std::vector<std::string>{"name"};
    auto props = std::make_unique<std::vector<storage::cpp2::VertexProp>>();
    props->emplace_back(std::move(personProp));
    props->emplace_back(std::move(bookProp));

    storage::cpp2::Expr expr1;
    expr1.expr_ref() = TagPropertyExpression::make(pool, "person", "name")->encode();
    storage::cpp2::Expr expr2;
    expr2.expr_ref() = TagPropertyExpression::make(pool, "book", "name")->encode();
    storage::cpp2::Expr expr3;
    expr3.expr_ref() = TagPropertyExpression::make(pool, "person", "age")->encode();
    auto exprs = std::make_unique<std::vector<storage::cpp2::Expr>>();
    exprs->emplace_back(std::move(expr1));
    exprs->emplace_back(std::move(expr2));
    exprs->emplace_back(std::move(expr3));
    auto *gv = GetVertices::make(qctx, start, 1, src, std::move(props), std::move(exprs));
    gv->setColNames({nebula::kVid, "person.name", "book.name", "person.age"});

    // project
    auto yieldColumns = std::make_unique<YieldColumns>();
    yieldColumns->addColumn(new YieldColumn(TagPropertyExpression::make(pool, "person", "name")));
    yieldColumns->addColumn(new YieldColumn(RelationalExpression::makeGT(
        pool, ConstantExpression::make(pool, 1), ConstantExpression::make(pool, 1))));
    yieldColumns->addColumn(new YieldColumn(TagPropertyExpression::make(pool, "book", "name")));
    yieldColumns->addColumn(new YieldColumn(TagPropertyExpression::make(pool, "person", "age")));
    auto *project = Project::make(qctx, gv, yieldColumns.get());
    project->setColNames({"person.name", "(1>1)", "book.name", "person.age"});

    auto result = Eq(qctx->plan()->root(), project);
    ASSERT_TRUE(result.ok()) << result;
  }
  // With YIELD combine properties
  {
    auto qctx = getQCtx("FETCH PROP ON person \"1\" YIELD person.name + person.age");
    auto *pool = qctx->objPool();
    auto *start = StartNode::make(qctx);

    auto tagIdResult = schemaMng_->toTagID(1, "person");
    ASSERT_TRUE(tagIdResult.ok());
    auto tagId = tagIdResult.value();
    storage::cpp2::VertexProp prop;
    prop.tag_ref() = tagId;
    prop.props_ref() = std::vector<std::string>{"name", "age"};
    auto props = std::make_unique<std::vector<storage::cpp2::VertexProp>>();
    props->emplace_back(std::move(prop));
    storage::cpp2::Expr expr1;
    expr1.expr_ref() =
        ArithmeticExpression::makeAdd(pool,
                                      TagPropertyExpression::make(pool, "person", "name"),
                                      TagPropertyExpression::make(pool, "person", "age"))
            ->encode();
    auto exprs = std::make_unique<std::vector<storage::cpp2::Expr>>();
    exprs->emplace_back(std::move(expr1));

    auto *gv = GetVertices::make(qctx, start, 1, src, std::move(props), std::move(exprs));
    gv->setColNames({nebula::kVid, "person.name", "person.age"});

    // project, TODO(shylock) could push down to storage is it supported
    auto yieldColumns = std::make_unique<YieldColumns>();
    yieldColumns->addColumn(new YieldColumn(
        ArithmeticExpression::makeAdd(pool,
                                      TagPropertyExpression::make(pool, "person", "name"),
                                      TagPropertyExpression::make(pool, "person", "age"))));
    auto *project = Project::make(qctx, gv, yieldColumns.get());
    project->setColNames({"(person.name+person.age)"});

    auto result = Eq(qctx->plan()->root(), project);
    ASSERT_TRUE(result.ok()) << result;
  }
  // multi-tags With YIELD combine properties
  {
    auto qctx = getQCtx("FETCH PROP ON book,person \"1\" YIELD person.name + book.name");
    auto *pool = qctx->objPool();
    auto *start = StartNode::make(qctx);

    // person
    auto tagIdResult = schemaMng_->toTagID(1, "person");
    ASSERT_TRUE(tagIdResult.ok());
    auto tagId = tagIdResult.value();
    storage::cpp2::VertexProp personProp;
    personProp.tag_ref() = tagId;
    personProp.props_ref() = std::vector<std::string>{"name"};
    // book
    tagIdResult = schemaMng_->toTagID(1, "book");
    ASSERT_TRUE(tagIdResult.ok());
    tagId = tagIdResult.value();
    storage::cpp2::VertexProp bookProp;
    bookProp.tag_ref() = tagId;
    bookProp.props_ref() = std::vector<std::string>{"name"};
    auto props = std::make_unique<std::vector<storage::cpp2::VertexProp>>();
    props->emplace_back(std::move(personProp));
    props->emplace_back(std::move(bookProp));

    storage::cpp2::Expr expr1;
    expr1.expr_ref() =
        ArithmeticExpression::makeAdd(pool,
                                      TagPropertyExpression::make(pool, "person", "name"),
                                      TagPropertyExpression::make(pool, "book", "name"))
            ->encode();
    auto exprs = std::make_unique<std::vector<storage::cpp2::Expr>>();
    exprs->emplace_back(std::move(expr1));
    auto *gv = GetVertices::make(qctx, start, 1, src, std::move(props), std::move(exprs));
    gv->setColNames({nebula::kVid, "person.name", "book.name"});

    // project, TODO(shylock) could push down to storage is it supported
    auto yieldColumns = std::make_unique<YieldColumns>();
    yieldColumns->addColumn(new YieldColumn(
        ArithmeticExpression::makeAdd(pool,
                                      TagPropertyExpression::make(pool, "person", "name"),
                                      TagPropertyExpression::make(pool, "book", "name"))));
    auto *project = Project::make(qctx, gv, yieldColumns.get());
    project->setColNames({"(person.name+book.name)"});

    auto result = Eq(qctx->plan()->root(), project);
    ASSERT_TRUE(result.ok()) << result;
  }
  // With YIELD distinct
  {
    auto qctx = getQCtx("FETCH PROP ON person \"1\" YIELD distinct person.name, person.age");
    auto *pool = qctx->objPool();
    auto *start = StartNode::make(qctx);

    auto tagIdResult = schemaMng_->toTagID(1, "person");
    ASSERT_TRUE(tagIdResult.ok());
    auto tagId = tagIdResult.value();
    storage::cpp2::VertexProp prop;
    prop.tag_ref() = tagId;
    prop.props_ref() = std::vector<std::string>{"name", "age"};
    auto props = std::make_unique<std::vector<storage::cpp2::VertexProp>>();
    props->emplace_back(std::move(prop));
    storage::cpp2::Expr expr1;
    expr1.expr_ref() = TagPropertyExpression::make(pool, "person", "name")->encode();
    storage::cpp2::Expr expr2;
    expr2.expr_ref() = TagPropertyExpression::make(pool, "person", "age")->encode();
    auto exprs = std::make_unique<std::vector<storage::cpp2::Expr>>();
    exprs->emplace_back(std::move(expr1));
    exprs->emplace_back(std::move(expr2));
    auto *gv = GetVertices::make(qctx, start, 1, src, std::move(props), std::move(exprs));
    gv->setColNames({nebula::kVid, "person.name", "person.age"});

    auto yieldColumns = std::make_unique<YieldColumns>();
    yieldColumns->addColumn(new YieldColumn(TagPropertyExpression::make(pool, "person", "name")));
    yieldColumns->addColumn(new YieldColumn(TagPropertyExpression::make(pool, "person", "age")));
    auto *project = Project::make(qctx, gv, yieldColumns.get());
    project->setColNames({"person.name", "person.age"});

    auto *dedup = Dedup::make(qctx, project);

    auto result = Eq(qctx->plan()->root(), dedup);
    ASSERT_TRUE(result.ok()) << result;
  }
  // ON *
  {
    auto qctx = getQCtx("FETCH PROP ON * \"1\" YIELD vertex as node");
    auto *pool = qctx->objPool();
    auto *start = StartNode::make(qctx);

    auto *gv = GetVertices::make(qctx, start, 1, src);
    gv->setColNames({nebula::kVid, "person.name", "person.age"});
    // project
    auto yieldColumns = std::make_unique<YieldColumns>();
    yieldColumns->addColumn(new YieldColumn(VertexExpression::make(pool), "node"));
    auto *project = Project::make(qctx, gv, yieldColumns.get());
    auto result = Eq(qctx->plan()->root(), project);
    ASSERT_TRUE(result.ok()) << result;
  }
  {
    auto qctx = getQCtx("FETCH PROP ON * \"1\", \"2\" YIELD vertex as node");
    auto *pool = qctx->objPool();
    auto *start = StartNode::make(qctx);

    auto *gv = GetVertices::make(qctx, start, 1, src);
    gv->setColNames({nebula::kVid, "person.name", "person.age"});
    // project
    auto yieldColumns = std::make_unique<YieldColumns>();
    yieldColumns->addColumn(new YieldColumn(VertexExpression::make(pool), "node"));
    auto *project = Project::make(qctx, gv, yieldColumns.get());
    auto result = Eq(qctx->plan()->root(), project);
    ASSERT_TRUE(result.ok()) << result;
  }
  // ON * with yield
  {
    auto qctx = getQCtx("FETCH PROP ON * \"1\", \"2\" YIELD person.name");
    auto *pool = qctx->objPool();
    auto *start = StartNode::make(qctx);

    auto *gv = GetVertices::make(qctx, start, 1, src);
    gv->setColNames({nebula::kVid, "person.name"});

    auto yieldColumns = std::make_unique<YieldColumns>();
    yieldColumns->addColumn(new YieldColumn(TagPropertyExpression::make(pool, "person", "name")));
    auto *project = Project::make(qctx, gv, yieldColumns.get());

    auto result = Eq(qctx->plan()->root(), project);
    ASSERT_TRUE(result.ok()) << result;
  }
  {
    auto qctx = getQCtx("FETCH PROP ON * \"1\", \"2\" YIELD person.name, person.age");
    auto *pool = qctx->objPool();
    auto *start = StartNode::make(qctx);

    auto *gv = GetVertices::make(qctx, start, 1, src);
    gv->setColNames({nebula::kVid, "person.name", "person.age"});

    auto yieldColumns = std::make_unique<YieldColumns>();
    yieldColumns->addColumn(new YieldColumn(TagPropertyExpression::make(pool, "person", "name")));
    yieldColumns->addColumn(new YieldColumn(TagPropertyExpression::make(pool, "person", "age")));
    auto *project = Project::make(qctx, gv, yieldColumns.get());

    auto result = Eq(qctx->plan()->root(), project);
    ASSERT_TRUE(result.ok()) << result;
  }
  {
    auto qctx = getQCtx("FETCH PROP ON * \"1\", \"2\" YIELD 1+1, person.name, person.age");
    auto *pool = qctx->objPool();
    auto *start = StartNode::make(qctx);

    auto *gv = GetVertices::make(qctx, start, 1, src);
    gv->setColNames({nebula::kVid, "person.name", "person.age"});

    auto yieldColumns = std::make_unique<YieldColumns>();
    yieldColumns->addColumn(new YieldColumn(ArithmeticExpression::makeAdd(
        pool, ConstantExpression::make(pool, 1), ConstantExpression::make(pool, 1))));
    yieldColumns->addColumn(new YieldColumn(TagPropertyExpression::make(pool, "person", "name")));
    yieldColumns->addColumn(new YieldColumn(TagPropertyExpression::make(pool, "person", "age")));
    auto *project = Project::make(qctx, gv, yieldColumns.get());

    auto result = Eq(qctx->plan()->root(), project);
    ASSERT_TRUE(result.ok()) << result;
  }
}

TEST_F(FetchVerticesValidatorTest, FetchVerticesInputOutput) {
  // pipe
  {
    const std::string query =
        "FETCH PROP ON person \"1\" YIELD person.name AS name |"
        "FETCH PROP ON person $-.name YIELD vertex as node";
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
        "FETCH PROP ON person \"1\" YIELD person.name AS name |"
        "FETCH PROP ON person, book $-.name YIELD vertex as node";
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
    const std::string query =
        "$a = FETCH PROP ON person \"1\" YIELD person.name AS name;"
        "FETCH PROP ON person $a.name YIELD vertex as node";
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
        "$a = FETCH PROP ON person \"1\" YIELD person.name AS name;"
        "FETCH PROP ON book,person $a.name YIELD vertex as node";
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
        "FETCH PROP ON person \"1\" YIELD person.name + 1 AS name"
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
    const std::string query =
        "$a = FETCH PROP ON person \"1\" YIELD person.name + 1 AS name;"
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
    const std::string query =
        "FETCH PROP ON person \"1\", \"2\" YIELD person.name AS name"
        "| FETCH PROP ON * $-.name YIELD vertex as node";
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
        "FETCH PROP ON * $a.name YIELD vertex as node";
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
    const std::string query =
        "FETCH PROP ON * \"1\", \"2\" YIELD person.name AS name"
        "| FETCH PROP ON * $-.name YIELD vertex as node";
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
        "$a = FETCH PROP ON * \"1\", \"2\" YIELD person.name AS name;"
        "FETCH PROP ON * $a.name YIELD vertex as node";
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
  ASSERT_FALSE(
      validate("FETCH PROP ON person, not_exist_tag \"1\""
               " YIELD not_exist_tag.prop1, person.name"));
  ASSERT_FALSE(validate("FETCH PROP ON * \"1\" YIELD not_exist_tag.prop1"));

  // not exist property
  ASSERT_FALSE(validate("FETCH PROP ON person \"1\" YIELD person.not_exist_property"));
  ASSERT_FALSE(
      validate("FETCH PROP ON person, book \"1\""
               " YIELD person.not_exist_property, book.not_exist_property"));
  ASSERT_FALSE(validate("FETCH PROP ON * \"1\" YIELD person.not_exist_property"));

  // invalid yield expression
  ASSERT_FALSE(
      validate("$a = FETCH PROP ON person \"1\" YIELD person.name AS name;"
               " FETCH PROP ON person \"1\" YIELD $a.name + 1"));

  ASSERT_FALSE(validate("FETCH PROP ON person \"1\" YIELD $^.person.name"));
  ASSERT_FALSE(validate("FETCH PROP ON person \"1\" YIELD $$.person.name"));
  ASSERT_FALSE(
      validate("FETCH PROP ON person \"1\" YIELD person.name AS name | "
               " FETCH PROP ON person \"1\" YIELD $-.name + 1"));
  ASSERT_FALSE(validate("FETCH PROP ON person \"1\" YIELD person._src + 1"));
  ASSERT_FALSE(validate("FETCH PROP ON person \"1\" YIELD person._type"));
  ASSERT_FALSE(validate("FETCH PROP ON person \"1\" YIELD person._rank + 1"));
  ASSERT_FALSE(validate("FETCH PROP ON person \"1\" YIELD person._dst + 1"));

  // invalid yield expression on *
  ASSERT_FALSE(
      validate("$a = FETCH PROP ON * \"1\" YIELD person.name AS name;"
               " FETCH PROP ON * \"1\" YIELD $a.name + 1"));

  ASSERT_FALSE(validate("FETCH PROP ON * \"1\" YIELD $^.person.name"));
  ASSERT_FALSE(validate("FETCH PROP ON * \"1\" YIELD $$.person.name"));
  ASSERT_FALSE(
      validate("FETCH PROP ON * \"1\" YIELD person.name AS name | "
               " FETCH PROP ON * \"1\" YIELD $-.name + 1"));
  ASSERT_FALSE(validate("FETCH PROP ON * \"1\" YIELD person._src + 1"));
  ASSERT_FALSE(validate("FETCH PROP ON * \"1\" YIELD person._type"));
  ASSERT_FALSE(validate("FETCH PROP ON * \"1\" YIELD person._rank + 1"));
  ASSERT_FALSE(validate("FETCH PROP ON * \"1\" YIELD person._dst + 1"));
}

TEST_F(FetchVerticesValidatorTest, FetchVerticesInputFailed) {
  // mismatched variable
  ASSERT_FALSE(
      validate("$a = FETCH PROP ON person \"1\" YIELD person.name AS name;"
               "FETCH PROP ON person $b.name YIELD vertex as node"));
  ASSERT_FALSE(
      validate("$a = FETCH PROP ON * \"1\" YIELD person.name AS name;"
               "FETCH PROP * person $b.name YIELD vertex as node"));

  // mismatched variable property
  ASSERT_FALSE(
      validate("$a = FETCH PROP ON person \"1\" YIELD person.name AS name;"
               "FETCH PROP ON person $a.not_exist_property YIELD vertex as node"));
  ASSERT_FALSE(
      validate("$a = FETCH PROP * person \"1\" YIELD person.name AS name;"
               "FETCH PROP * person $a.not_exist_property YIELD vertex as node"));

  // mismatched input property
  ASSERT_FALSE(
      validate("FETCH PROP ON person \"1\" YIELD person.name AS name | "
               "FETCH PROP ON person $-.not_exist_property YIELD vertex as node"));
  ASSERT_FALSE(
      validate("FETCH PROP * person \"1\" YIELD person.name AS name | "
               "FETCH PROP * person $-.not_exist_property YIELD vertex as node"));
}

}  // namespace graph
}  // namespace nebula
