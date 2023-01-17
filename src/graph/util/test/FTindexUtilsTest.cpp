// Copyright (c) 2022 vesoft inc. All rights reserved.
//
// This source code is licensed under Apache 2.0 License.

#include "common/plugin/fulltext/elasticsearch/ESAdapter.h"
#include "glog/logging.h"
#include "gmock/gmock.h"
#include "graph/util/ExpressionUtils.h"
#include "graph/util/FTIndexUtils.h"
#include "gtest/gtest.h"

DECLARE_uint32(ft_request_retry_times);

using ::testing::_;
using ::testing::Return;

namespace nebula::graph {

class MockESAdapter : public plugin::ESAdapter {
 public:
  MOCK_METHOD(StatusOr<plugin::ESQueryResult>,
              fuzzy,
              (const std::string&, const std::string&, const std::string&, int64_t, int64_t),
              (override));
  MOCK_METHOD(StatusOr<plugin::ESQueryResult>,
              prefix,
              (const std::string&, const std::string&, int64_t, int64_t),
              (override));
  MOCK_METHOD(StatusOr<plugin::ESQueryResult>,
              regexp,
              (const std::string&, const std::string&, int64_t, int64_t),
              (override));
  MOCK_METHOD(StatusOr<plugin::ESQueryResult>,
              wildcard,
              (const std::string&, const std::string&, int64_t, int64_t),
              (override));
};

class FTIndexUtilsTest : public ::testing::Test {
 public:
 protected:
  ObjectPool pool_;
  Expression* eq(Expression* left, Expression* right) {
    return RelationalExpression::makeEQ(&pool_, left, right);
  }
  Expression* or_(const std::vector<Expression*>& exprs) {
    return ExpressionUtils::pushOrs(&pool_, exprs);
  }
  Expression* tagProp(const std::string& tag, const std::string& prop) {
    return TagPropertyExpression::make(&pool_, tag, prop);
  }
  Expression* edgeProp(const std::string& edge, const std::string& prop) {
    return EdgePropertyExpression::make(&pool_, edge, prop);
  }
  template <typename T>
  Expression* constant(const T& value) {
    return ConstantExpression::make(&pool_, Value(value));
  }
};

TEST_F(FTIndexUtilsTest, rewriteTSFilter) {
  ObjectPool pool;
  std::string indexName = "test_index";
  std::string tagName = "tag1";
  std::string edgeName = "edge1";
  std::string propName = "prop1";
  using Item = plugin::ESQueryResult::Item;
  std::vector<Item> items = {Item("1", "abc"), Item("2", "abc1"), Item("3", "abc")};
  plugin::ESQueryResult esResult;
  esResult.items = items;
  {
    MockESAdapter mockESAdapter;
    EXPECT_CALL(mockESAdapter, prefix(indexName, "prefix_pattern", 10000, -1))
        .WillOnce(Return(esResult));
    auto argument = TextSearchArgument::make(&pool, tagName, propName, "prefix_pattern");
    auto expr = TextSearchExpression::makePrefix(&pool, argument);
    auto expect = or_({eq(tagProp(tagName, propName), constant("abc")),
                       eq(tagProp(tagName, propName), constant("abc1")),
                       eq(tagProp(tagName, propName), constant("abc"))});
    auto result = FTIndexUtils::rewriteTSFilter(&pool, false, expr, indexName, mockESAdapter);
    ASSERT_TRUE(result.ok()) << result.status().message();
    ASSERT_EQ(*result.value(), *expect) << result.value()->toString() << "\t" << expect->toString();
  }
  {
    plugin::ESQueryResult emptyEsResult;
    MockESAdapter mockESAdapter;
    EXPECT_CALL(mockESAdapter, prefix(indexName, "prefix_pattern", 10000, -1))
        .WillOnce(Return(emptyEsResult));
    auto argument = TextSearchArgument::make(&pool, tagName, propName, "prefix_pattern");
    auto expr = TextSearchExpression::makePrefix(&pool, argument);
    auto result = FTIndexUtils::rewriteTSFilter(&pool, false, expr, indexName, mockESAdapter);
    ASSERT_TRUE(result.ok()) << result.status().message();
    ASSERT_EQ(result.value(), nullptr);
  }
  {
    Status status = Status::Error("mock error");
    MockESAdapter mockESAdapter;
    EXPECT_CALL(mockESAdapter, prefix(indexName, "prefix_pattern", 10000, -1))
        .Times(FLAGS_ft_request_retry_times)
        .WillRepeatedly(Return(status));
    auto argument = TextSearchArgument::make(&pool, tagName, propName, "prefix_pattern");
    auto expr = TextSearchExpression::makePrefix(&pool, argument);
    auto result = FTIndexUtils::rewriteTSFilter(&pool, false, expr, indexName, mockESAdapter);
    ASSERT_FALSE(result.ok());
    ASSERT_EQ(result.status().message(), "mock error");
  }
  {
    MockESAdapter mockESAdapter;
    EXPECT_CALL(mockESAdapter, wildcard(indexName, "wildcard_pattern", 10000, -1))
        .WillOnce(Return(esResult));
    auto argument = TextSearchArgument::make(&pool, edgeName, propName, "wildcard_pattern");
    auto expr = TextSearchExpression::makeWildcard(&pool, argument);
    auto expect = or_({eq(edgeProp(edgeName, propName), constant("abc")),
                       eq(edgeProp(edgeName, propName), constant("abc1")),
                       eq(edgeProp(edgeName, propName), constant("abc"))});
    auto result = FTIndexUtils::rewriteTSFilter(&pool, true, expr, indexName, mockESAdapter);
    ASSERT_TRUE(result.ok()) << result.status().message();
    ASSERT_EQ(*result.value(), *expect) << result.value()->toString() << "\t" << expect->toString();
  }
  {
    plugin::ESQueryResult singleEsResult;
    singleEsResult.items = {Item("a", "b", 1, "edge text")};
    MockESAdapter mockESAdapter;
    EXPECT_CALL(mockESAdapter, wildcard(indexName, "wildcard_pattern", 10000, -1))
        .WillOnce(Return(singleEsResult));
    auto argument = TextSearchArgument::make(&pool, edgeName, propName, "wildcard_pattern");
    auto expr = TextSearchExpression::makeWildcard(&pool, argument);
    auto expect = eq(edgeProp(edgeName, propName), constant("edge text"));
    auto result = FTIndexUtils::rewriteTSFilter(&pool, true, expr, indexName, mockESAdapter);
    ASSERT_TRUE(result.ok()) << result.status().message();
    ASSERT_EQ(*result.value(), *expect) << result.value()->toString() << "\t" << expect->toString();
  }
  {
    MockESAdapter mockESAdapter;
    EXPECT_CALL(mockESAdapter, regexp(indexName, "regexp_pattern", 10000, -1))
        .WillOnce(Return(esResult));
    auto argument = TextSearchArgument::make(&pool, edgeName, propName, "regexp_pattern");
    auto expr = TextSearchExpression::makeRegexp(&pool, argument);
    auto expect = or_({eq(edgeProp(edgeName, propName), constant("abc")),
                       eq(edgeProp(edgeName, propName), constant("abc1")),
                       eq(edgeProp(edgeName, propName), constant("abc"))});
    auto result = FTIndexUtils::rewriteTSFilter(&pool, true, expr, indexName, mockESAdapter);
    ASSERT_TRUE(result.ok()) << result.status().message();
    ASSERT_EQ(*result.value(), *expect) << result.value()->toString() << "\t" << expect->toString();
  }
  {
    MockESAdapter mockESAdapter;
    EXPECT_CALL(mockESAdapter, fuzzy(indexName, "fuzzy_pattern", "1", 10000, -1))
        .WillOnce(Return(esResult));
    auto argument = TextSearchArgument::make(&pool, tagName, propName, "fuzzy_pattern");
    argument->setFuzziness(1);
    auto expr = TextSearchExpression::makeFuzzy(&pool, argument);
    auto expect = or_({eq(tagProp(tagName, propName), constant("abc")),
                       eq(tagProp(tagName, propName), constant("abc1")),
                       eq(tagProp(tagName, propName), constant("abc"))});
    auto result = FTIndexUtils::rewriteTSFilter(&pool, false, expr, indexName, mockESAdapter);
    ASSERT_TRUE(result.ok()) << result.status().message();
    ASSERT_EQ(*result.value(), *expect) << result.value()->toString() << "\t" << expect->toString();
  }
}

}  // namespace nebula::graph
