/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#include <gtest/gtest.h>

#include "graph/optimizer/rule/IndexScanRule.h"
#include "graph/util/OptimizerUtils.h"

using nebula::cpp2::PropertyType;
using nebula::graph::OptimizerUtils;
using nebula::storage::cpp2::IndexQueryContext;

using FilterItem = nebula::graph::OptimizerUtils::FilterItem;

namespace nebula {
namespace opt {

TEST(IndexScanRuleTest, IQCtxTest) {
  auto objPoolPtr = std::make_unique<ObjectPool>();
  auto* pool = objPoolPtr.get();
  {
    auto index = std::make_shared<meta::cpp2::IndexItem>();
    std::vector<FilterItem> items;
    std::vector<IndexQueryContext> iqctx;
    auto ret = OptimizerUtils::appendIQCtx(index, items, iqctx);
    ASSERT_TRUE(ret.ok());
  }
  {
    auto index = std::make_shared<meta::cpp2::IndexItem>();
    std::vector<FilterItem> items;
    std::vector<IndexQueryContext> iqctx;
    // setup index
    {
      std::vector<meta::cpp2::ColumnDef> cols;
      for (int8_t i = 0; i < 5; i++) {
        meta::cpp2::ColumnDef col;
        col.name_ref() = folly::stringPrintf("col%d", i);
        col.type.type_ref() = PropertyType::INT64;
        cols.emplace_back(std::move(col));
      }
      index->fields_ref() = std::move(cols);
      index->index_id_ref() = 1;
    }
    // setup FilterItems col0 < 1 and col0 <= 6
    {
      items.clear();
      items.emplace_back("col0", RelationalExpression::Kind::kRelLT, Value(1L));
      items.emplace_back("col0", RelationalExpression::Kind::kRelLE, Value(6L));

      auto ret = OptimizerUtils::appendIQCtx(index, items, iqctx);
      ASSERT_TRUE(ret.ok());

      ASSERT_EQ(1, iqctx.size());
      ASSERT_EQ(1, iqctx.begin()->get_column_hints().size());
      ASSERT_EQ(1, iqctx.begin()->get_index_id());
      ASSERT_EQ("", iqctx.begin()->get_filter());
      const auto& colHints = iqctx.begin()->get_column_hints();
      ASSERT_EQ("col0", colHints.begin()->get_column_name());
      ASSERT_EQ(storage::cpp2::ScanType::RANGE, colHints.begin()->get_scan_type());
      ASSERT_EQ(1, colHints.begin()->get_end_value());
      ASSERT_FALSE(colHints.begin()->get_include_end());
      ASSERT_FALSE(colHints.begin()->begin_value_ref().is_set());
    }

    // setup FilterItems col0 > 1 and col1 <= 2 and col1 > -1 and col2 > 3
    //                   and col3 < 4 and col4 == 4
    {
      items.clear();
      iqctx.clear();
      items.emplace_back("col0", RelationalExpression::Kind::kRelGT, Value(1L));
      items.emplace_back("col1", RelationalExpression::Kind::kRelLE, Value(2L));
      items.emplace_back("col1", RelationalExpression::Kind::kRelGT, Value(-1L));
      items.emplace_back("col2", RelationalExpression::Kind::kRelGT, Value(3L));
      items.emplace_back("col3", RelationalExpression::Kind::kRelLT, Value(4L));
      items.emplace_back("col4", RelationalExpression::Kind::kRelEQ, Value(4L));

      auto ret = OptimizerUtils::appendIQCtx(index, items, iqctx);
      ASSERT_TRUE(ret.ok());

      ASSERT_EQ(1, iqctx.size());
      ASSERT_EQ(1, iqctx.begin()->get_column_hints().size());
      ASSERT_EQ(1, iqctx.begin()->get_index_id());
      ASSERT_EQ("", iqctx.begin()->get_filter());
      const auto& colHints = iqctx.begin()->get_column_hints();
      {
        auto hint = colHints[0];
        ASSERT_EQ("col0", hint.get_column_name());
        ASSERT_EQ(storage::cpp2::ScanType::RANGE, hint.get_scan_type());
        ASSERT_EQ(1, hint.get_begin_value());
        ASSERT_FALSE(hint.get_include_begin());
        ASSERT_FALSE(hint.end_value_ref().is_set());
      }
    }

    // setup FilterItems col0 == 1 and col1 <= 2 and col1 > -1 and col2 != 3
    //                   and col3 < 4
    // only expect col0 and col1 exits in column hints.
    // col2 and col3 should be filter in storage layer.
    {
      items.clear();
      iqctx.clear();
      items.emplace_back("col0", RelationalExpression::Kind::kRelEQ, Value(1L));
      items.emplace_back("col1", RelationalExpression::Kind::kRelLE, Value(2L));
      items.emplace_back("col1", RelationalExpression::Kind::kRelGT, Value(-1L));
      items.emplace_back("col2", RelationalExpression::Kind::kRelNE, Value(3L));
      items.emplace_back("col3", RelationalExpression::Kind::kRelLT, Value(4L));

      auto ret = OptimizerUtils::appendIQCtx(index, items, iqctx);
      ASSERT_TRUE(ret.ok());

      ASSERT_EQ(1, iqctx.size());
      ASSERT_EQ(2, iqctx.begin()->get_column_hints().size());
      ASSERT_EQ(1, iqctx.begin()->get_index_id());
      ASSERT_EQ("", iqctx.begin()->get_filter());
      const auto& colHints = iqctx.begin()->get_column_hints();
      {
        auto hint = colHints[0];
        ASSERT_EQ("col0", hint.get_column_name());
        ASSERT_EQ(storage::cpp2::ScanType::PREFIX, hint.get_scan_type());
        ASSERT_EQ(Value(1L), hint.get_begin_value());
      }
      {
        auto hint = colHints[1];
        ASSERT_EQ("col1", hint.get_column_name());
        ASSERT_EQ(storage::cpp2::ScanType::RANGE, hint.get_scan_type());
        ASSERT_EQ(Value(-1L), hint.get_begin_value());
        ASSERT_FALSE(hint.get_include_begin());
        ASSERT_EQ(Value(2L), hint.get_end_value());
        ASSERT_TRUE(hint.get_include_end());
      }
    }
    // setup FilterItems col0 == 1 and col1 == 2 and col2 == -1 and col3 > 3
    //                   and col4 < 4
    // only expect col0, col1, col2 and col3 exits in column hints.
    // col4 should be filter in storage layer.
    {
      items.clear();
      iqctx.clear();
      items.emplace_back("col0", RelationalExpression::Kind::kRelEQ, Value(1L));
      items.emplace_back("col1", RelationalExpression::Kind::kRelEQ, Value(2L));
      items.emplace_back("col2", RelationalExpression::Kind::kRelEQ, Value(-1L));
      items.emplace_back("col3", RelationalExpression::Kind::kRelGT, Value(3L));
      items.emplace_back("col4", RelationalExpression::Kind::kRelLT, Value(4L));

      auto* expr = RelationalExpression::makeLT(
          pool, ConstantExpression::make(pool, "col4"), ConstantExpression::make(pool, 4));
      auto ret = OptimizerUtils::appendIQCtx(index, items, iqctx, expr);
      ASSERT_TRUE(ret.ok());

      ASSERT_EQ(1, iqctx.size());
      ASSERT_EQ(4, iqctx.begin()->get_column_hints().size());
      ASSERT_EQ(1, iqctx.begin()->get_index_id());
      ASSERT_EQ("(\"col4\"<4)", Expression::decode(pool, iqctx.begin()->get_filter())->toString());
      const auto& colHints = iqctx.begin()->get_column_hints();
      {
        auto hint = colHints[0];
        ASSERT_EQ("col0", hint.get_column_name());
        ASSERT_EQ(storage::cpp2::ScanType::PREFIX, hint.get_scan_type());
        ASSERT_EQ(Value(1L), hint.get_begin_value());
      }
      {
        auto hint = colHints[1];
        ASSERT_EQ("col1", hint.get_column_name());
        ASSERT_EQ(storage::cpp2::ScanType::PREFIX, hint.get_scan_type());
        ASSERT_EQ(Value(2L), hint.get_begin_value());
      }
      {
        auto hint = colHints[2];
        ASSERT_EQ("col2", hint.get_column_name());
        ASSERT_EQ(storage::cpp2::ScanType::PREFIX, hint.get_scan_type());
        ASSERT_EQ(Value(-1L), hint.get_begin_value());
      }
      {
        auto hint = colHints[3];
        ASSERT_EQ("col3", hint.get_column_name());
        ASSERT_EQ(storage::cpp2::ScanType::RANGE, hint.get_scan_type());
        ASSERT_EQ(Value(3L), hint.get_begin_value());
        ASSERT_FALSE(hint.get_include_begin());
      }
    }
  }
}

TEST(IndexScanRuleTest, BoundValueRangeTest) {
  {
    meta::cpp2::ColumnDef col;
    col.name_ref() = "col_int";
    col.type.type_ref() = PropertyType::INT64;
    {
      std::vector<storage::cpp2::IndexColumnHint> hints;
      // col_int < 2
      std::vector<FilterItem> items;
      items.emplace_back("col_int", RelationalExpression::Kind::kRelLT, Value(2L));
      auto status = OptimizerUtils::appendColHint(hints, items, col);
      EXPECT_TRUE(status.ok());
      EXPECT_EQ(1, hints.size());
      const auto& hint = hints[0];
      EXPECT_EQ("col_int", *hint.column_name_ref());
      EXPECT_EQ(storage::cpp2::ScanType::RANGE, *hint.scan_type_ref());
      EXPECT_FALSE(hint.begin_value_ref().is_set());
      EXPECT_FALSE(hint.get_include_end());
      EXPECT_EQ(2, *hint.end_value_ref());
    }
    {
      std::vector<storage::cpp2::IndexColumnHint> hints;
      // col_int <= 2
      std::vector<FilterItem> items;
      items.emplace_back("col_int", RelationalExpression::Kind::kRelLE, Value(2L));
      auto status = OptimizerUtils::appendColHint(hints, items, col);
      EXPECT_TRUE(status.ok());
      EXPECT_EQ(1, hints.size());
      const auto& hint = hints[0];
      EXPECT_EQ("col_int", *hint.column_name_ref());
      EXPECT_EQ(storage::cpp2::ScanType::RANGE, *hint.scan_type_ref());
      EXPECT_FALSE(hint.begin_value_ref().is_set());
      EXPECT_TRUE(hint.get_include_end());
      EXPECT_EQ(2, *hint.end_value_ref());
    }
    {
      std::vector<storage::cpp2::IndexColumnHint> hints;
      // col_int > 2
      std::vector<FilterItem> items;
      items.emplace_back("col_int", RelationalExpression::Kind::kRelGT, Value(2L));
      auto status = OptimizerUtils::appendColHint(hints, items, col);
      EXPECT_TRUE(status.ok());
      EXPECT_EQ(1, hints.size());
      const auto& hint = hints[0];
      EXPECT_EQ("col_int", *hint.column_name_ref());
      EXPECT_EQ(storage::cpp2::ScanType::RANGE, *hint.scan_type_ref());
      EXPECT_FALSE(hint.end_value_ref().is_set());
      EXPECT_FALSE(hint.get_include_begin());
      EXPECT_EQ(2, *hint.begin_value_ref());
    }
    {
      std::vector<storage::cpp2::IndexColumnHint> hints;
      // col_int >= 2
      std::vector<FilterItem> items;
      items.emplace_back("col_int", RelationalExpression::Kind::kRelGE, Value(2L));
      auto status = OptimizerUtils::appendColHint(hints, items, col);
      EXPECT_TRUE(status.ok());
      EXPECT_EQ(1, hints.size());
      const auto& hint = hints[0];
      EXPECT_EQ("col_int", *hint.column_name_ref());
      EXPECT_EQ(storage::cpp2::ScanType::RANGE, *hint.scan_type_ref());
      EXPECT_FALSE(hint.end_value_ref().is_set());
      EXPECT_TRUE(hint.get_include_begin());
      EXPECT_EQ(2, *hint.begin_value_ref());
    }
    {
      std::vector<storage::cpp2::IndexColumnHint> hints;
      // col_int > 2 and col_int < 5
      std::vector<FilterItem> items;
      items.emplace_back("col_int", RelationalExpression::Kind::kRelGT, Value(2L));
      items.emplace_back("col_int", RelationalExpression::Kind::kRelLT, Value(5L));
      auto status = OptimizerUtils::appendColHint(hints, items, col);
      EXPECT_TRUE(status.ok());
      EXPECT_EQ(1, hints.size());
      const auto& hint = hints[0];
      EXPECT_EQ("col_int", *hint.column_name_ref());
      EXPECT_EQ(storage::cpp2::ScanType::RANGE, *hint.scan_type_ref());
      EXPECT_EQ(2, hint.get_begin_value());
      EXPECT_FALSE(hint.get_include_begin());
      EXPECT_EQ(5, hint.get_end_value());
      EXPECT_FALSE(hint.get_include_end());
    }
    {
      std::vector<storage::cpp2::IndexColumnHint> hints;
      // col_int >= 2 and col_int <= 5
      std::vector<FilterItem> items;
      items.emplace_back("col_int", RelationalExpression::Kind::kRelGE, Value(2L));
      items.emplace_back("col_int", RelationalExpression::Kind::kRelLE, Value(5L));
      auto status = OptimizerUtils::appendColHint(hints, items, col);
      EXPECT_TRUE(status.ok());
      EXPECT_EQ(1, hints.size());
      const auto& hint = hints[0];
      EXPECT_EQ("col_int", *hint.column_name_ref());
      EXPECT_EQ(storage::cpp2::ScanType::RANGE, *hint.scan_type_ref());
      EXPECT_EQ(2, hint.get_begin_value());
      EXPECT_TRUE(hint.get_include_begin());
      EXPECT_EQ(5, hint.get_end_value());
      EXPECT_TRUE(hint.get_include_end());
    }
  }
  {
    meta::cpp2::ColumnDef col;
    col.name_ref() = "col_bool";
    col.type.type_ref() = PropertyType::BOOL;
    {
      std::vector<storage::cpp2::IndexColumnHint> hints;
      // col_bool < true
      std::vector<FilterItem> items;
      items.emplace_back("col_bool", RelationalExpression::Kind::kRelLT, Value(true));
      auto status = OptimizerUtils::appendColHint(hints, items, col);
      EXPECT_FALSE(status.ok());
    }
    {
      std::vector<storage::cpp2::IndexColumnHint> hints;
      // col_bool >= false
      std::vector<FilterItem> items;
      items.emplace_back("col_bool", RelationalExpression::Kind::kRelGE, Value(false));
      auto status = OptimizerUtils::appendColHint(hints, items, col);
      EXPECT_FALSE(status.ok());
    }
  }
  {
    meta::cpp2::ColumnDef col;
    col.name_ref() = "col_double";
    col.type.type_ref() = PropertyType::DOUBLE;
    {
      std::vector<storage::cpp2::IndexColumnHint> hints;
      // col_double < 1.0
      std::vector<FilterItem> items;
      items.emplace_back("col_double", RelationalExpression::Kind::kRelLT, Value(1.0));
      auto status = OptimizerUtils::appendColHint(hints, items, col);
      EXPECT_TRUE(status.ok());
      EXPECT_EQ(1, hints.size());
      const auto& hint = hints[0];
      EXPECT_EQ("col_double", *hint.column_name_ref());
      EXPECT_EQ(storage::cpp2::ScanType::RANGE, *hint.scan_type_ref());
      EXPECT_EQ(1.0, hint.get_end_value());
      EXPECT_FALSE(hint.get_include_end());
      EXPECT_FALSE(hint.begin_value_ref().is_set());
    }
    {
      std::vector<storage::cpp2::IndexColumnHint> hints;
      // col_double <= 3.0
      std::vector<FilterItem> items;
      items.emplace_back("col_double", RelationalExpression::Kind::kRelLE, Value(3.0));
      auto status = OptimizerUtils::appendColHint(hints, items, col);
      EXPECT_TRUE(status.ok());
      EXPECT_EQ(1, hints.size());
      const auto& hint = hints[0];
      EXPECT_EQ("col_double", *hint.column_name_ref());
      EXPECT_EQ(storage::cpp2::ScanType::RANGE, *hint.scan_type_ref());
      EXPECT_EQ(3.0, hint.get_end_value());
      EXPECT_TRUE(hint.get_include_end());
      EXPECT_FALSE(hint.begin_value_ref().is_set());
    }
    {
      std::vector<storage::cpp2::IndexColumnHint> hints;
      // col_double >= 1.0
      std::vector<FilterItem> items;
      items.emplace_back("col_double", RelationalExpression::Kind::kRelGE, Value(1.0));
      auto status = OptimizerUtils::appendColHint(hints, items, col);
      EXPECT_TRUE(status.ok());
      EXPECT_EQ(1, hints.size());
      const auto& hint = hints[0];
      EXPECT_EQ("col_double", *hint.column_name_ref());
      EXPECT_EQ(storage::cpp2::ScanType::RANGE, *hint.scan_type_ref());
      EXPECT_EQ(1.0, hint.get_begin_value());
      EXPECT_TRUE(hint.get_include_begin());
      EXPECT_FALSE(hint.end_value_ref().is_set());
    }
    {
      std::vector<storage::cpp2::IndexColumnHint> hints;
      // 1.0 < col_double <= 5.0
      std::vector<FilterItem> items;
      items.emplace_back("col_double", RelationalExpression::Kind::kRelGT, Value(1.0));
      items.emplace_back("col_double", RelationalExpression::Kind::kRelLE, Value(5.0));
      auto status = OptimizerUtils::appendColHint(hints, items, col);
      EXPECT_TRUE(status.ok());
      EXPECT_EQ(1, hints.size());
      const auto& hint = hints[0];
      EXPECT_EQ("col_double", *hint.column_name_ref());
      EXPECT_EQ(storage::cpp2::ScanType::RANGE, *hint.scan_type_ref());
      EXPECT_FALSE(hint.get_include_begin());
      EXPECT_EQ(1, hint.get_begin_value());
      EXPECT_TRUE(hint.get_include_end());
      EXPECT_EQ(5, hint.get_end_value());
    }
  }
  {
    meta::cpp2::ColumnDef col;
    size_t len = 10;
    col.name_ref() = "col_str";
    col.type.type_ref() = PropertyType::STRING;
    col.type.type_length_ref() = len;
    {
      std::vector<storage::cpp2::IndexColumnHint> hints;
      // col_str < "ccc"
      std::vector<FilterItem> items;
      items.emplace_back("col_str", RelationalExpression::Kind::kRelLT, Value("ccc"));
      auto status = OptimizerUtils::appendColHint(hints, items, col);
      EXPECT_TRUE(status.ok());
      EXPECT_EQ(1, hints.size());
      const auto& hint = hints[0];
      EXPECT_EQ("col_str", *hint.column_name_ref());
      EXPECT_EQ(storage::cpp2::ScanType::RANGE, *hint.scan_type_ref());
      EXPECT_FALSE(hint.begin_value_ref().is_set());
      EXPECT_FALSE(hint.get_include_end());
      EXPECT_EQ("ccc", *hint.end_value_ref());
    }
    {
      std::vector<storage::cpp2::IndexColumnHint> hints;
      // col_str > "ccc"
      std::vector<FilterItem> items;
      items.emplace_back("col_str", RelationalExpression::Kind::kRelGT, Value("ccc"));
      auto status = OptimizerUtils::appendColHint(hints, items, col);
      EXPECT_TRUE(status.ok());
      EXPECT_EQ(1, hints.size());
      const auto& hint = hints[0];
      EXPECT_EQ("col_str", *hint.column_name_ref());
      EXPECT_EQ(storage::cpp2::ScanType::RANGE, *hint.scan_type_ref());
      EXPECT_FALSE(hint.end_value_ref().is_set());
      EXPECT_FALSE(hint.get_include_begin());
      EXPECT_EQ("ccc", hint.get_begin_value());
    }
    {
      std::vector<storage::cpp2::IndexColumnHint> hints;
      // "aaa" < col_str < "ccc"
      std::vector<FilterItem> items;
      items.emplace_back("col_str", RelationalExpression::Kind::kRelGT, Value("aaa"));
      items.emplace_back("col_str", RelationalExpression::Kind::kRelLT, Value("ccc"));
      auto status = OptimizerUtils::appendColHint(hints, items, col);
      EXPECT_TRUE(status.ok());
      EXPECT_EQ(1, hints.size());
      const auto& hint = hints[0];
      EXPECT_EQ("col_str", *hint.column_name_ref());
      EXPECT_EQ(storage::cpp2::ScanType::RANGE, *hint.scan_type_ref());
      EXPECT_FALSE(hint.get_include_begin());
      EXPECT_EQ("aaa", hint.get_begin_value());
      EXPECT_FALSE(hint.get_include_end());
      EXPECT_EQ("ccc", hint.get_end_value());
    }
    {
      std::vector<storage::cpp2::IndexColumnHint> hints;
      // "aaa" <= col_str <= "ccc"
      std::vector<FilterItem> items;
      items.emplace_back("col_str", RelationalExpression::Kind::kRelGE, Value("aaa"));
      items.emplace_back("col_str", RelationalExpression::Kind::kRelLE, Value("ccc"));
      auto status = OptimizerUtils::appendColHint(hints, items, col);
      EXPECT_TRUE(status.ok());
      EXPECT_EQ(1, hints.size());
      const auto& hint = hints[0];
      EXPECT_EQ("col_str", *hint.column_name_ref());
      EXPECT_EQ(storage::cpp2::ScanType::RANGE, *hint.scan_type_ref());
      EXPECT_TRUE(hint.get_include_begin());
      EXPECT_EQ("aaa", hint.get_begin_value());
      EXPECT_TRUE(hint.get_include_end());
      EXPECT_EQ("ccc", hint.get_end_value());
    }
    {
      std::vector<storage::cpp2::IndexColumnHint> hints;
      // "aaa" <= col_str < "ccc"
      std::vector<FilterItem> items;
      items.emplace_back("col_str", RelationalExpression::Kind::kRelGE, Value("aaa"));
      items.emplace_back("col_str", RelationalExpression::Kind::kRelLT, Value("ccc"));
      auto status = OptimizerUtils::appendColHint(hints, items, col);
      EXPECT_TRUE(status.ok());
      EXPECT_EQ(1, hints.size());
      const auto& hint = hints[0];
      EXPECT_EQ("col_str", *hint.column_name_ref());
      EXPECT_EQ(storage::cpp2::ScanType::RANGE, *hint.scan_type_ref());
      EXPECT_TRUE(hint.get_include_begin());
      EXPECT_EQ("aaa", hint.get_begin_value());
      EXPECT_FALSE(hint.get_include_end());
      EXPECT_EQ("ccc", hint.get_end_value());
    }
  }
}

}  // namespace opt
}  // namespace nebula

int main(int argc, char** argv) {
  testing::InitGoogleTest(&argc, argv);
  folly::init(&argc, &argv, true);
  google::SetStderrLogging(google::INFO);
  return RUN_ALL_TESTS();
}
