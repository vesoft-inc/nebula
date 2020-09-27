/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include <gtest/gtest.h>
#include "optimizer/OptimizerUtils.h"
#include "optimizer/rule/IndexScanRule.h"

namespace nebula {
namespace opt {

TEST(IndexScanRuleTest, BoundValueTest) {
    meta::cpp2::ColumnDef col;
    auto* inst = std::move(IndexScanRule::kInstance).get();
    auto* instance = static_cast<IndexScanRule*>(inst);
    IndexScanRule::FilterItems items;
    {
        Value begin, end;
        col.set_name("col1");
        col.type.set_type(meta::cpp2::PropertyType::INT64);
        // col > 1 and col < 5
        items.addItem("col1", RelationalExpression::Kind::kRelGT, Value(1L));
        items.addItem("col1", RelationalExpression::Kind::kRelLT, Value(5L));
        for (const auto& item : items.items) {
            auto ret = instance->boundValue(item, col, begin, end);
            ASSERT_TRUE(ret.ok());
        }
        // Expect begin = 2 , end = 5;
        EXPECT_EQ((Value(2L)), begin);
        EXPECT_EQ((Value(5L)), end);
    }
    {
        Value begin, end;
        items.items.clear();
        col.set_name("col1");
        col.type.set_type(meta::cpp2::PropertyType::INT64);
        // col > 1 and col > 6
        items.addItem("col1", RelationalExpression::Kind::kRelGT, Value(1L));
        items.addItem("col1", RelationalExpression::Kind::kRelGT, Value(6L));
        for (const auto& item : items.items) {
            auto ret = instance->boundValue(item, col, begin, end);
            ASSERT_TRUE(ret.ok());
        }
        // Expect begin = 7
        EXPECT_EQ(Value(7L), begin);
        EXPECT_EQ(Value(), end);
    }
    {
        Value begin, end;
        items.items.clear();
        col.set_name("col1");
        col.type.set_type(meta::cpp2::PropertyType::INT64);
        // col > 1 and col >= 6
        items.addItem("col1", RelationalExpression::Kind::kRelGT, Value(1L));
        items.addItem("col1", RelationalExpression::Kind::kRelGE, Value(6L));
        for (const auto& item : items.items) {
            auto ret = instance->boundValue(item, col, begin, end);
            ASSERT_TRUE(ret.ok());
        }
        // Expect begin = 6
        EXPECT_EQ(Value(6L), begin);
        EXPECT_EQ(Value(), end);
    }
    {
        Value begin, end;
        items.items.clear();
        col.set_name("col1");
        col.type.set_type(meta::cpp2::PropertyType::INT64);
        // col < 1 and col <= 6
        items.addItem("col1", RelationalExpression::Kind::kRelLT, Value(1L));
        items.addItem("col1", RelationalExpression::Kind::kRelLE, Value(6L));
        for (const auto& item : items.items) {
            auto ret = instance->boundValue(item, col, begin, end);
            ASSERT_TRUE(ret.ok());
        }
        // Expect end = 1
        EXPECT_EQ(Value(1L), end);
        EXPECT_EQ(Value(), begin);
    }
}

TEST(IndexScanRuleTest, IQCtxTest) {
    auto* inst = std::move(IndexScanRule::kInstance).get();
    auto* instance = static_cast<IndexScanRule*>(inst);
    {
        IndexItem index = std::make_unique<meta::cpp2::IndexItem>();
        IndexScanRule::FilterItems items;
        IndexQueryCtx iqctx = std::make_unique<std::vector<IndexQueryContext>>();
        auto ret = instance->appendIQCtx(index, items, iqctx);
        ASSERT_TRUE(ret.ok());
    }
    {
        IndexItem index = std::make_unique<meta::cpp2::IndexItem>();
        IndexScanRule::FilterItems items;
        IndexQueryCtx iqctx = std::make_unique<std::vector<IndexQueryContext>>();
        // setup index
        {
            std::vector<meta::cpp2::ColumnDef> cols;
            for (int8_t i = 0; i < 5; i++) {
                meta::cpp2::ColumnDef col;
                col.set_name(folly::stringPrintf("col%d", i));
                col.type.set_type(meta::cpp2::PropertyType::INT64);
                cols.emplace_back(std::move(col));
            }
            index->set_fields(std::move(cols));
            index->set_index_id(1);
        }
        // setup FilterItems col0 < 1 and col0 <= 6
        {
            items.items.clear();
            items.addItem("col0", RelationalExpression::Kind::kRelLT, Value(1L));
            items.addItem("col0", RelationalExpression::Kind::kRelLE, Value(6L));

            auto ret = instance->appendIQCtx(index, items, iqctx);
            ASSERT_TRUE(ret.ok());

            ASSERT_EQ(1, iqctx->size());
            ASSERT_EQ(1, (iqctx.get()->begin())->get_column_hints().size());
            ASSERT_EQ(1, (iqctx.get()->begin())->get_index_id());
            ASSERT_EQ("", (iqctx.get()->begin())->get_filter());
            const auto& colHints = (iqctx.get()->begin())->get_column_hints();
            ASSERT_EQ("col0", colHints.begin()->get_column_name());
            ASSERT_EQ(storage::cpp2::ScanType::RANGE, colHints.begin()->get_scan_type());
            ASSERT_EQ(Value(std::numeric_limits<int64_t>::min()),
                      colHints.begin()->get_begin_value());
            ASSERT_EQ(Value(1L), colHints.begin()->get_end_value());
        }

        // setup FilterItems col0 > 1 and col1 <= 2 and col1 > -1 and col2 > 3
        //                   and col3 < 4 and col4 == 4
        {
            items.items.clear();
            iqctx.get()->clear();
            items.addItem("col0", RelationalExpression::Kind::kRelGT, Value(1L));
            items.addItem("col1", RelationalExpression::Kind::kRelLE, Value(2L));
            items.addItem("col1", RelationalExpression::Kind::kRelGT, Value(-1L));
            items.addItem("col2", RelationalExpression::Kind::kRelGT, Value(3L));
            items.addItem("col3", RelationalExpression::Kind::kRelLT, Value(4L));
            items.addItem("col4", RelationalExpression::Kind::kRelEQ, Value(4L));

            auto ret = instance->appendIQCtx(index, items, iqctx);
            ASSERT_TRUE(ret.ok());

            ASSERT_EQ(1, iqctx->size());
            ASSERT_EQ(5, (iqctx.get()->begin())->get_column_hints().size());
            ASSERT_EQ(1, (iqctx.get()->begin())->get_index_id());
            ASSERT_EQ("", (iqctx.get()->begin())->get_filter());
            const auto& colHints = (iqctx.get()->begin())->get_column_hints();
            {
                auto hint = colHints[0];
                ASSERT_EQ("col0", hint.get_column_name());
                ASSERT_EQ(storage::cpp2::ScanType::RANGE, hint.get_scan_type());
                ASSERT_EQ(Value(2L), hint.get_begin_value());
                ASSERT_EQ(Value(std::numeric_limits<int64_t>::max()), hint.get_end_value());
            }
            {
                auto hint = colHints[1];
                ASSERT_EQ("col1", hint.get_column_name());
                ASSERT_EQ(storage::cpp2::ScanType::RANGE, hint.get_scan_type());
                ASSERT_EQ(Value(0L), hint.get_begin_value());
                ASSERT_EQ(Value(3L), hint.get_end_value());
            }
            {
                auto hint = colHints[2];
                ASSERT_EQ("col2", hint.get_column_name());
                ASSERT_EQ(storage::cpp2::ScanType::RANGE, hint.get_scan_type());
                ASSERT_EQ(Value(4L), hint.get_begin_value());
                ASSERT_EQ(Value(std::numeric_limits<int64_t>::max()), hint.get_end_value());
            }
            {
                auto hint = colHints[3];
                ASSERT_EQ("col3", hint.get_column_name());
                ASSERT_EQ(storage::cpp2::ScanType::RANGE, hint.get_scan_type());
                ASSERT_EQ(Value(std::numeric_limits<int64_t>::min()), hint.get_begin_value());
                ASSERT_EQ(Value(4L), hint.get_end_value());
            }
            {
                auto hint = colHints[4];
                ASSERT_EQ("col4", hint.get_column_name());
                ASSERT_EQ(storage::cpp2::ScanType::PREFIX, hint.get_scan_type());
                ASSERT_EQ(Value(4L), hint.get_begin_value());
            }
        }

        // setup FilterItems col0 > 1 and col1 <= 2 and col1 > -1 and col2 != 3
        //                   and col3 < 4
        // only expect col0 and col1 exits in column hints.
        // col2 and col3 should be filter in storage layer.
        {
            items.items.clear();
            iqctx.get()->clear();
            items.addItem("col0", RelationalExpression::Kind::kRelGT, Value(1L));
            items.addItem("col1", RelationalExpression::Kind::kRelLE, Value(2L));
            items.addItem("col1", RelationalExpression::Kind::kRelGT, Value(-1L));
            items.addItem("col2", RelationalExpression::Kind::kRelNE, Value(3L));
            items.addItem("col3", RelationalExpression::Kind::kRelLT, Value(4L));

            auto ret = instance->appendIQCtx(index, items, iqctx);
            ASSERT_TRUE(ret.ok());

            ASSERT_EQ(1, iqctx->size());
            ASSERT_EQ(2, (iqctx.get()->begin())->get_column_hints().size());
            ASSERT_EQ(1, (iqctx.get()->begin())->get_index_id());
            ASSERT_EQ("", (iqctx.get()->begin())->get_filter());
            const auto& colHints = (iqctx.get()->begin())->get_column_hints();
            {
                auto hint = colHints[0];
                ASSERT_EQ("col0", hint.get_column_name());
                ASSERT_EQ(storage::cpp2::ScanType::RANGE, hint.get_scan_type());
                ASSERT_EQ(Value(2L), hint.get_begin_value());
                ASSERT_EQ(Value(std::numeric_limits<int64_t>::max()), hint.get_end_value());
            }
            {
                auto hint = colHints[1];
                ASSERT_EQ("col1", hint.get_column_name());
                ASSERT_EQ(storage::cpp2::ScanType::RANGE, hint.get_scan_type());
                ASSERT_EQ(Value(0L), hint.get_begin_value());
                ASSERT_EQ(Value(3L), hint.get_end_value());
            }
        }
    }
}

}   // namespace opt
}   // namespace nebula
