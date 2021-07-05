/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include <gtest/gtest.h>
#include "optimizer/OptimizerUtils.h"
#include "optimizer/rule/IndexScanRule.h"

using nebula::graph::OptimizerUtils;

namespace nebula {
namespace opt {

TEST(IndexScanRuleTest, BoundValueTest) {
    meta::cpp2::ColumnDef col;
    IndexScanRule::FilterItems items;
    {
        Value begin, end;
        col.set_name("col1");
        col.type.set_type(meta::cpp2::PropertyType::INT64);
        // col > 1 and col < 5
        items.addItem("col1", RelationalExpression::Kind::kRelGT, Value(1L));
        items.addItem("col1", RelationalExpression::Kind::kRelLT, Value(5L));
        for (const auto& item : items.items) {
            auto ret = OptimizerUtils::boundValue(item.relOP_, item.value_, col, begin, end);
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
            auto ret = OptimizerUtils::boundValue(item.relOP_, item.value_, col, begin, end);
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
            auto ret = OptimizerUtils::boundValue(item.relOP_, item.value_, col, begin, end);
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
            auto ret = OptimizerUtils::boundValue(item.relOP_, item.value_, col, begin, end);
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
        std::vector<IndexQueryContext> iqctx;
        auto ret = instance->appendIQCtx(index, items, iqctx);
        ASSERT_TRUE(ret.ok());
    }
    {
        IndexItem index = std::make_unique<meta::cpp2::IndexItem>();
        IndexScanRule::FilterItems items;
        std::vector<IndexQueryContext> iqctx;
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

            ASSERT_EQ(1, iqctx.size());
            ASSERT_EQ(1, iqctx.begin()->get_column_hints().size());
            ASSERT_EQ(1, iqctx.begin()->get_index_id());
            ASSERT_EQ("", iqctx.begin()->get_filter());
            const auto& colHints = iqctx.begin()->get_column_hints();
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
            iqctx.clear();
            items.addItem("col0", RelationalExpression::Kind::kRelGT, Value(1L));
            items.addItem("col1", RelationalExpression::Kind::kRelLE, Value(2L));
            items.addItem("col1", RelationalExpression::Kind::kRelGT, Value(-1L));
            items.addItem("col2", RelationalExpression::Kind::kRelGT, Value(3L));
            items.addItem("col3", RelationalExpression::Kind::kRelLT, Value(4L));
            items.addItem("col4", RelationalExpression::Kind::kRelEQ, Value(4L));

            auto ret = instance->appendIQCtx(index, items, iqctx);
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
                ASSERT_EQ(Value(2L), hint.get_begin_value());
                ASSERT_EQ(Value(std::numeric_limits<int64_t>::max()), hint.get_end_value());
            }
        }

        // setup FilterItems col0 == 1 and col1 <= 2 and col1 > -1 and col2 != 3
        //                   and col3 < 4
        // only expect col0 and col1 exits in column hints.
        // col2 and col3 should be filter in storage layer.
        {
            items.items.clear();
            iqctx.clear();
            items.addItem("col0", RelationalExpression::Kind::kRelEQ, Value(1L));
            items.addItem("col1", RelationalExpression::Kind::kRelLE, Value(2L));
            items.addItem("col1", RelationalExpression::Kind::kRelGT, Value(-1L));
            items.addItem("col2", RelationalExpression::Kind::kRelNE, Value(3L));
            items.addItem("col3", RelationalExpression::Kind::kRelLT, Value(4L));

            auto ret = instance->appendIQCtx(index, items, iqctx);
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
                ASSERT_EQ(Value(0L), hint.get_begin_value());
                ASSERT_EQ(Value(3L), hint.get_end_value());
            }
        }
        // setup FilterItems col0 == 1 and col1 == 2 and col2 == -1 and col3 > 3
        //                   and col4 < 4
        // only expect col0, col1, col2 and col3 exits in column hints.
        // col4 should be filter in storage layer.
        {
            items.items.clear();
            iqctx.clear();
            items.addItem("col0", RelationalExpression::Kind::kRelEQ, Value(1L));
            items.addItem("col1", RelationalExpression::Kind::kRelEQ, Value(2L));
            items.addItem("col2", RelationalExpression::Kind::kRelEQ, Value(-1L));
            items.addItem("col3", RelationalExpression::Kind::kRelGT, Value(3L));
            items.addItem("col4", RelationalExpression::Kind::kRelLT, Value(4L));

            auto ret = instance->appendIQCtx(index, items, iqctx, "col4 < 4");
            ASSERT_TRUE(ret.ok());

            ASSERT_EQ(1, iqctx.size());
            ASSERT_EQ(4, iqctx.begin()->get_column_hints().size());
            ASSERT_EQ(1, iqctx.begin()->get_index_id());
            ASSERT_EQ("col4 < 4", iqctx.begin()->get_filter());
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
                ASSERT_EQ(Value(4L), hint.get_begin_value());
                ASSERT_EQ(Value(std::numeric_limits<int64_t>::max()), hint.get_end_value());
            }
        }
    }
}

TEST(IndexScanRuleTest, BoundValueRangeTest) {
    auto* inst = std::move(IndexScanRule::kInstance).get();
    auto* instance = static_cast<IndexScanRule*>(inst);

    {
        meta::cpp2::ColumnDef col;
        col.set_name("col_int");
        col.type.set_type(meta::cpp2::PropertyType::INT64);
        {
            std::vector<storage::cpp2::IndexColumnHint> hints;
            // col_int < 2
            IndexScanRule::FilterItems items;
            items.addItem("col_int", RelationalExpression::Kind::kRelLT, Value(2L));
            auto status = instance->appendColHint(hints, items, col);
            EXPECT_TRUE(status.ok());
            EXPECT_EQ(1, hints.size());
            const auto& hint = hints[0];
            EXPECT_EQ("col_int", *hint.column_name_ref());
            EXPECT_EQ(storage::cpp2::ScanType::RANGE, *hint.scan_type_ref());
            EXPECT_EQ(std::numeric_limits<int64_t>::min(), *hint.begin_value_ref());
            EXPECT_EQ(2, *hint.end_value_ref());
        }
        {
            std::vector<storage::cpp2::IndexColumnHint> hints;
            // col_int <= 2
            IndexScanRule::FilterItems items;
            items.addItem("col_int", RelationalExpression::Kind::kRelLE, Value(2L));
            auto status = instance->appendColHint(hints, items, col);
            EXPECT_TRUE(status.ok());
            EXPECT_EQ(1, hints.size());
            const auto& hint = hints[0];
            EXPECT_EQ("col_int", *hint.column_name_ref());
            EXPECT_EQ(storage::cpp2::ScanType::RANGE, *hint.scan_type_ref());
            EXPECT_EQ(std::numeric_limits<int64_t>::min(), *hint.begin_value_ref());
            EXPECT_EQ(3, *hint.end_value_ref());
        }
        {
            std::vector<storage::cpp2::IndexColumnHint> hints;
            // col_int > 2
            IndexScanRule::FilterItems items;
            items.addItem("col_int", RelationalExpression::Kind::kRelGT, Value(2L));
            auto status = instance->appendColHint(hints, items, col);
            EXPECT_TRUE(status.ok());
            EXPECT_EQ(1, hints.size());
            const auto& hint = hints[0];
            EXPECT_EQ("col_int", *hint.column_name_ref());
            EXPECT_EQ(storage::cpp2::ScanType::RANGE, *hint.scan_type_ref());
            EXPECT_EQ(3, *hint.begin_value_ref());
            EXPECT_EQ(std::numeric_limits<int64_t>::max(), *hint.end_value_ref());
        }
        {
            std::vector<storage::cpp2::IndexColumnHint> hints;
            // col_int >= 2
            IndexScanRule::FilterItems items;
            items.addItem("col_int", RelationalExpression::Kind::kRelGE, Value(2L));
            auto status = instance->appendColHint(hints, items, col);
            EXPECT_TRUE(status.ok());
            EXPECT_EQ(1, hints.size());
            const auto& hint = hints[0];
            EXPECT_EQ("col_int", *hint.column_name_ref());
            EXPECT_EQ(storage::cpp2::ScanType::RANGE, *hint.scan_type_ref());
            EXPECT_EQ(2, *hint.begin_value_ref());
            EXPECT_EQ(std::numeric_limits<int64_t>::max(), *hint.end_value_ref());
        }
        {
            std::vector<storage::cpp2::IndexColumnHint> hints;
            // col_int > 2 and col_int < 5
            IndexScanRule::FilterItems items;
            items.addItem("col_int", RelationalExpression::Kind::kRelGT, Value(2L));
            items.addItem("col_int", RelationalExpression::Kind::kRelLT, Value(5L));
            auto status = instance->appendColHint(hints, items, col);
            EXPECT_TRUE(status.ok());
            EXPECT_EQ(1, hints.size());
            const auto& hint = hints[0];
            EXPECT_EQ("col_int", *hint.column_name_ref());
            EXPECT_EQ(storage::cpp2::ScanType::RANGE, *hint.scan_type_ref());
            EXPECT_EQ(3, *hint.begin_value_ref());
            EXPECT_EQ(5, *hint.end_value_ref());
        }
        {
            std::vector<storage::cpp2::IndexColumnHint> hints;
            // col_int >= 2 and col_int <= 5
            IndexScanRule::FilterItems items;
            items.addItem("col_int", RelationalExpression::Kind::kRelGE, Value(2L));
            items.addItem("col_int", RelationalExpression::Kind::kRelLE, Value(5L));
            auto status = instance->appendColHint(hints, items, col);
            EXPECT_TRUE(status.ok());
            EXPECT_EQ(1, hints.size());
            const auto& hint = hints[0];
            EXPECT_EQ("col_int", *hint.column_name_ref());
            EXPECT_EQ(storage::cpp2::ScanType::RANGE, *hint.scan_type_ref());
            EXPECT_EQ(2, *hint.begin_value_ref());
            EXPECT_EQ(6, *hint.end_value_ref());
        }
    }
    {
        meta::cpp2::ColumnDef col;
        col.set_name("col_bool");
        col.type.set_type(meta::cpp2::PropertyType::BOOL);
        {
            std::vector<storage::cpp2::IndexColumnHint> hints;
            // col_bool < true
            IndexScanRule::FilterItems items;
            items.addItem("col_bool", RelationalExpression::Kind::kRelLT, Value(true));
            auto status = instance->appendColHint(hints, items, col);
            EXPECT_FALSE(status.ok());
        }
        {
            std::vector<storage::cpp2::IndexColumnHint> hints;
            // col_bool >= false
            IndexScanRule::FilterItems items;
            items.addItem("col_bool", RelationalExpression::Kind::kRelGE, Value(false));
            auto status = instance->appendColHint(hints, items, col);
            EXPECT_FALSE(status.ok());
        }
    }
    {
        meta::cpp2::ColumnDef col;
        col.set_name("col_double");
        col.type.set_type(meta::cpp2::PropertyType::DOUBLE);
        {
            std::vector<storage::cpp2::IndexColumnHint> hints;
            // col_double < 1.0
            IndexScanRule::FilterItems items;
            items.addItem("col_double", RelationalExpression::Kind::kRelLT, Value(1.0));
            auto status = instance->appendColHint(hints, items, col);
            EXPECT_TRUE(status.ok());
            EXPECT_EQ(1, hints.size());
            const auto& hint = hints[0];
            EXPECT_EQ("col_double", *hint.column_name_ref());
            EXPECT_EQ(storage::cpp2::ScanType::RANGE, *hint.scan_type_ref());
            EXPECT_EQ(-std::numeric_limits<double>::max(), *hint.begin_value_ref());
            EXPECT_EQ(1, *hint.end_value_ref());
        }
        {
            std::vector<storage::cpp2::IndexColumnHint> hints;
            // col_double <= 3.0
            IndexScanRule::FilterItems items;
            items.addItem("col_double", RelationalExpression::Kind::kRelLE, Value(3.0));
            auto status = instance->appendColHint(hints, items, col);
            EXPECT_TRUE(status.ok());
            EXPECT_EQ(1, hints.size());
            const auto& hint = hints[0];
            EXPECT_EQ("col_double", *hint.column_name_ref());
            EXPECT_EQ(storage::cpp2::ScanType::RANGE, *hint.scan_type_ref());
            EXPECT_EQ(-std::numeric_limits<double>::max(), *hint.begin_value_ref());
            EXPECT_EQ(3 + kEpsilon, *hint.end_value_ref());
        }
        {
            std::vector<storage::cpp2::IndexColumnHint> hints;
            // col_double >= 1.0
            IndexScanRule::FilterItems items;
            items.addItem("col_double", RelationalExpression::Kind::kRelGE, Value(1.0));
            auto status = instance->appendColHint(hints, items, col);
            EXPECT_TRUE(status.ok());
            EXPECT_EQ(1, hints.size());
            const auto& hint = hints[0];
            EXPECT_EQ("col_double", *hint.column_name_ref());
            EXPECT_EQ(storage::cpp2::ScanType::RANGE, *hint.scan_type_ref());
            EXPECT_EQ(1, *hint.begin_value_ref());
            EXPECT_EQ(std::numeric_limits<double>::max(), *hint.end_value_ref());
        }
        {
            std::vector<storage::cpp2::IndexColumnHint> hints;
            // 1.0 < col_double <= 5.0
            IndexScanRule::FilterItems items;
            items.addItem("col_double", RelationalExpression::Kind::kRelGT, Value(1.0));
            items.addItem("col_double", RelationalExpression::Kind::kRelLE, Value(5.0));
            auto status = instance->appendColHint(hints, items, col);
            EXPECT_TRUE(status.ok());
            EXPECT_EQ(1, hints.size());
            const auto& hint = hints[0];
            EXPECT_EQ("col_double", *hint.column_name_ref());
            EXPECT_EQ(storage::cpp2::ScanType::RANGE, *hint.scan_type_ref());
            EXPECT_EQ(1 + kEpsilon, *hint.begin_value_ref());
            EXPECT_EQ(5 + kEpsilon, *hint.end_value_ref());
        }
    }
    {
        meta::cpp2::ColumnDef col;
        size_t len = 10;
        col.set_name("col_str");
        col.type.set_type(meta::cpp2::PropertyType::STRING);
        col.type.set_type_length(len);
        {
            std::vector<storage::cpp2::IndexColumnHint> hints;
            // col_str < "ccc"
            IndexScanRule::FilterItems items;
            items.addItem("col_str", RelationalExpression::Kind::kRelLT, Value("ccc"));
            auto status = instance->appendColHint(hints, items, col);
            EXPECT_TRUE(status.ok());
            EXPECT_EQ(1, hints.size());
            const auto& hint = hints[0];
            EXPECT_EQ("col_str", *hint.column_name_ref());
            EXPECT_EQ(storage::cpp2::ScanType::RANGE, *hint.scan_type_ref());
            EXPECT_EQ(std::string(len, '\0'), *hint.begin_value_ref());
            EXPECT_EQ("ccc", *hint.end_value_ref());
        }
        {
            std::vector<storage::cpp2::IndexColumnHint> hints;
            // col_str > "ccc"
            IndexScanRule::FilterItems items;
            items.addItem("col_str", RelationalExpression::Kind::kRelGT, Value("ccc"));
            auto status = instance->appendColHint(hints, items, col);
            EXPECT_TRUE(status.ok());
            EXPECT_EQ(1, hints.size());
            const auto& hint = hints[0];
            EXPECT_EQ("col_str", *hint.column_name_ref());
            EXPECT_EQ(storage::cpp2::ScanType::RANGE, *hint.scan_type_ref());
            std::string begin = std::string(3, 'c').append(6, 0x00).append(1, 0x01);
            std::string end = std::string(len, static_cast<char>(0xFF));
            EXPECT_EQ(begin, *hint.begin_value_ref());
            EXPECT_EQ(end, *hint.end_value_ref());
        }
        {
            std::vector<storage::cpp2::IndexColumnHint> hints;
            // "aaa" < col_str < "ccc"
            IndexScanRule::FilterItems items;
            items.addItem("col_str", RelationalExpression::Kind::kRelGT, Value("aaa"));
            items.addItem("col_str", RelationalExpression::Kind::kRelLT, Value("ccc"));
            auto status = instance->appendColHint(hints, items, col);
            EXPECT_TRUE(status.ok());
            EXPECT_EQ(1, hints.size());
            const auto& hint = hints[0];
            EXPECT_EQ("col_str", *hint.column_name_ref());
            EXPECT_EQ(storage::cpp2::ScanType::RANGE, *hint.scan_type_ref());
            std::string begin = std::string(3, 'a').append(6, 0x00).append(1, 0x01);
            std::string end = "ccc";
            EXPECT_EQ(begin, *hint.begin_value_ref());
            EXPECT_EQ(end, *hint.end_value_ref());
        }
        {
            std::vector<storage::cpp2::IndexColumnHint> hints;
            // "aaa" <= col_str <= "ccc"
            IndexScanRule::FilterItems items;
            items.addItem("col_str", RelationalExpression::Kind::kRelGE, Value("aaa"));
            items.addItem("col_str", RelationalExpression::Kind::kRelLE, Value("ccc"));
            auto status = instance->appendColHint(hints, items, col);
            EXPECT_TRUE(status.ok());
            EXPECT_EQ(1, hints.size());
            const auto& hint = hints[0];
            EXPECT_EQ("col_str", *hint.column_name_ref());
            EXPECT_EQ(storage::cpp2::ScanType::RANGE, *hint.scan_type_ref());
            std::string begin = "aaa";
            std::string end = std::string(3, 'c').append(6, 0x00).append(1, 0x01);
            EXPECT_EQ(begin, *hint.begin_value_ref());
            EXPECT_EQ(end, *hint.end_value_ref());
        }
        {
            std::vector<storage::cpp2::IndexColumnHint> hints;
            // "aaa" <= col_str < "ccc"
            IndexScanRule::FilterItems items;
            items.addItem("col_str", RelationalExpression::Kind::kRelGE, Value("aaa"));
            items.addItem("col_str", RelationalExpression::Kind::kRelLT, Value("ccc"));
            auto status = instance->appendColHint(hints, items, col);
            EXPECT_TRUE(status.ok());
            EXPECT_EQ(1, hints.size());
            const auto& hint = hints[0];
            EXPECT_EQ("col_str", *hint.column_name_ref());
            EXPECT_EQ(storage::cpp2::ScanType::RANGE, *hint.scan_type_ref());
            EXPECT_EQ("aaa", *hint.begin_value_ref());
            EXPECT_EQ("ccc", *hint.end_value_ref());
        }
    }
}

}   // namespace opt
}   // namespace nebula

int main(int argc, char** argv) {
    testing::InitGoogleTest(&argc, argv);
    folly::init(&argc, &argv, true);
    google::SetStderrLogging(google::INFO);
    return RUN_ALL_TESTS();
}
