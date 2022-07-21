/* Copyright (c) 2018 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#include <gtest/gtest.h>

#include "common/base/Base.h"
#include "graph/util/AstUtils.h"
#include "parser/GQLParser.h"

namespace nebula {

using graph::QueryContext;
class ParserIso : public ::testing::Test {
 public:
  void SetUp() override {
    qctx_ = std::make_unique<graph::QueryContext>();
  }
  void TearDown() override {
    qctx_.reset();
  }

 protected:
  StatusOr<std::unique_ptr<Sentence>> parse(const std::string& query) {
    auto* qctx = qctx_.get();
    GQLParser parser(qctx);
    auto result = parser.parse(query);
    NG_RETURN_IF_ERROR(result);
    NG_RETURN_IF_ERROR(graph::AstUtils::reprAstCheck(*result.value(), qctx));
    return result;
  }

 protected:
  std::unique_ptr<QueryContext> qctx_;
};

TEST_F(ParserIso, TestSchemaCreation) {
  // All type
  {
    std::string query =
        "ISOMOR A.graph,b.graph";
    auto result = parse(query);
    //  ASSERT_TRUE(result.ok()) << result.status();
  }
}
/*
TEST_F(ParserIso, Set) {
  {
    std::string query =
        "GO FROM \"1\" OVER friend INTERSECT "
        "GO FROM \"2\" OVER friend";
    auto result = parse(query);
    ASSERT_TRUE(result.ok()) << result.status();
  }
  {
    std::string query =
        "GO FROM \"1\" OVER friend UNION "
        "GO FROM \"2\" OVER friend";
    auto result = parse(query);
    ASSERT_TRUE(result.ok()) << result.status();
  }
  {
    std::string query =
        "GO FROM \"1\" OVER friend MINUS "
        "GO FROM \"2\" OVER friend";
    auto result = parse(query);
    ASSERT_TRUE(result.ok()) << result.status();
  }
  {
    std::string query =
        "GO FROM \"1\" OVER friend MINUS "
        "GO FROM \"2\" OVER friend UNION "
        "GO FROM \"2\" OVER friend INTERSECT "
        "GO FROM \"3\" OVER friend";
    auto result = parse(query);
    ASSERT_TRUE(result.ok()) << result.status();
  }
  {
    std::string query =
        "(GO FROM \"1\" OVER friend | "
        "GO FROM \"2\" OVER friend) UNION "
        "GO FROM \"3\" OVER friend";
    auto result = parse(query);
    ASSERT_TRUE(result.ok()) << result.status();
  }
  {
    // pipe have priority over set
    std::string query =
        "GO FROM \"1\" OVER friend | "
        "GO FROM \"2\" OVER friend UNION "
        "GO FROM \"3\" OVER friend";
    auto result = parse(query);
    ASSERT_TRUE(result.ok()) << result.status();
  }
  {
    std::string query =
        "(GO FROM \"1\" OVER friend YIELD friend._dst AS id UNION "
        "GO FROM \"2\" OVER friend YIELD friend._dst AS id) | "
        "GO FROM $-.id OVER friend";
    auto result = parse(query);
    ASSERT_TRUE(result.ok()) << result.status();
  }
}  */
}  // namespace nebula
